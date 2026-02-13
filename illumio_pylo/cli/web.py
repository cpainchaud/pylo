"""Minimal web UI server for Pylo CLI

This module provides a tiny FastAPI server that exposes:
- GET /api/commands -> list of available commands (name, description)
- GET /api/commands/{name} -> metadata (arguments) for a command
- GET /api/credentials -> list available credential profiles
- POST /api/run -> run a command (synchronously) and return stdout/stderr at completion
- Static files under / (served from web_static)

The implementation intentionally keeps things simple: commands are executed synchronously in a background thread while we capture stdout/stderr and return them when finished. For streaming logs or handling long-running tasks a task queue should be integrated later.
"""

try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
    from fastapi.staticfiles import StaticFiles
    FASTAPI_AVAILABLE = True
except Exception:
    FASTAPI_AVAILABLE = False

import contextlib
import io
import os
import sys
import threading
import webbrowser
from typing import Dict, Any

import illumio_pylo as pylo
from illumio_pylo.API.CredentialsManager import get_all_credentials
from illumio_pylo.cli import commands

if FASTAPI_AVAILABLE:
    app = FastAPI()

    # mount static folder
    static_dir = os.path.join(os.path.dirname(__file__), 'web_static')
    if os.path.isdir(static_dir):
        # Mount static files under /static so API endpoints are not shadowed.
        app.mount('/static', StaticFiles(directory=static_dir, html=True), name='static')

    # Serve index.html at root for SPA
    @app.get('/')
    def root_index():
        index_path = os.path.join(static_dir, 'index.html')
        if os.path.exists(index_path):
            return FileResponse(index_path, media_type='text/html')
        return HTMLResponse('<html><body><h1>Index not found</h1></body></html>', status_code=404)


    def _describe_argparse_parser(parser) -> Dict[str, Any]:
        # Extract basic metadata from argparse parser actions
        args = []
        for action in parser._actions:
            # skip help
            if action.dest == 'help':
                continue
            arg = {
                'dest': action.dest,
                'option_strings': action.option_strings,
                'help': getattr(action, 'help', ''),
                'default': getattr(action, 'default', None),
                'required': getattr(action, 'required', False),
            }

            # Check for parser-level defaults (e.g., report_format_default)
            # If the action default is None but there's a parser-level default, use that
            if arg['default'] is None and hasattr(parser, '_defaults'):
                # Look for a parser-level default with the pattern {dest}_default
                parser_level_default = parser._defaults.get(action.dest + '_default')
                if parser_level_default is not None:
                    arg['default'] = parser_level_default
                # Special case: for report_format, if no explicit default was set, use 'csv' as the implicit default
                elif action.dest == 'report_format' and 'report_format_default' in parser._defaults:
                    # The key exists but value is None, which means no explicit default was provided
                    # In this case, the code will use 'csv' as fallback at runtime
                    arg['default'] = 'csv'

            # type is not always available; use str
            try:
                arg['type'] = action.type.__name__ if action.type is not None else 'str'
            except Exception:
                arg['type'] = 'str'
            if hasattr(action, 'choices') and action.choices is not None:
                arg['choices'] = list(action.choices)
            args.append(arg)
        return {'arguments': args}



    @app.get('/api/commands')
    def api_commands_list():
        # Return commands sorted alphabetically by name
        result = [{'name': cmd.name} for cmd in sorted(commands.available_commands.values(), key=lambda c: c.name)]
        return JSONResponse(result)


    @app.get('/api/commands/{name}')
    def api_command_metadata(name: str):
        cmd = commands.available_commands.get(name)
        if cmd is None:
            raise HTTPException(status_code=404, detail='Command not found')
        # build a temporary argparse parser to extract metadata
        import argparse
        tmp = argparse.ArgumentParser()
        cmd.fill_parser(tmp)
        metadata = _describe_argparse_parser(tmp)
        return JSONResponse(metadata)


    @app.get('/api/credentials')
    def api_credentials_list():
        creds = get_all_credentials()
        result = []
        for c in creds:
            result.append({'name': c.name, 'fqdn': c.fqdn, 'originating_file': c.originating_file})
        return JSONResponse(result)


    @app.post('/api/run')
    async def api_run(request: Request):
        data = await request.json()
        # expected fields: command (name), args (dict), pce (optional)
        command_name = data.get('command')
        if not command_name:
            raise HTTPException(status_code=400, detail='Missing command')
        cmd = commands.available_commands.get(command_name)
        if cmd is None:
            raise HTTPException(status_code=404, detail='Command not found')

        provided_args = data.get('args', {})
        pce = data.get('pce')

        # Prepare org and connector similar to CLI run() but simplified
        connector = None
        config_data = None
        org = None

        # For commands that need credentials, ensure we have a pce
        if not cmd.credentials_manager_mode:
            if pce is None:
                raise HTTPException(status_code=400, detail='Missing pce profile name')
            connector = pylo.APIConnector.create_from_credentials_in_file(pce, request_if_missing=False)
            if connector is None:
                raise HTTPException(status_code=400, detail='Cannot find credentials for pce {}'.format(pce))
            # download objects
            config_data = connector.get_pce_objects(list_of_objects_to_load=cmd.load_specific_objects_only, force_async_mode=False, include_deleted_workloads=False)
            org = pylo.Organization(1)
            org.connector = connector
            if not cmd.skip_pce_config_loading:
                org.pce_version = connector.get_software_version()
                org.load_from_json(config_data, list_of_objects_to_load=cmd.load_specific_objects_only)

        # Build a full args dict using the parser defaults so missing keys are present (mimic argparse behavior)
        import argparse as _argparse
        tmp_parser = _argparse.ArgumentParser()
        try:
            cmd.fill_parser(tmp_parser)
        except Exception:
            # some commands may implement fill_parser expecting subparsers or special behavior; ignore failures to avoid blocking
            pass

        full_args = {}
        for action in tmp_parser._actions:
            if getattr(action, 'dest', None) == 'help':
                continue
            dest = action.dest
            default = getattr(action, 'default', None)
            # provided_args uses dest names as keys
            if dest in provided_args:
                full_args[dest] = provided_args[dest]
            else:
                full_args[dest] = default

        # Fallback: ensure any args referenced directly in the command's source (args['...']) are present
        # This helps when cmd.fill_parser raises or when parser doesn't expose every key the command expects
        try:
            import inspect, re
            src = ''
            try:
                src = inspect.getsource(cmd.main)
            except Exception:
                # might be a builtin or otherwise unavailable; ignore silently
                src = ''

            if src:
                for m in re.finditer(r"args\[['\"]([a-zA-Z0-9_\-]+)['\"]]\)", src):
                     key = m.group(1)
                     # argparse dests normally replace '-' with '_' but code may use either; normalize both
                     norm_key = key.replace('-', '_')
                     if key not in full_args and norm_key not in full_args:
                         full_args[norm_key] = None
        except Exception:
            # be tolerant: do not block command execution on fallback discovery issues
            pass

        # Execute command in background thread but block until completion while capturing output
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        def target():
            try:
                with contextlib.redirect_stdout(stdout_capture), contextlib.redirect_stderr(stderr_capture):
                    # CLI commands expect args as dict; provide the full_args dict (with argparse-like defaults)
                    cmd.main(full_args, org=org, config_data=config_data, connector=connector)
            except Exception as e:
                print('ERROR: {}'.format(e), file=sys.stderr)

        thread = threading.Thread(target=target)
        thread.start()
        thread.join()

        return JSONResponse({'stdout': stdout_capture.getvalue(), 'stderr': stderr_capture.getvalue()})


    def start_server(host: str = '127.0.0.1', port: int = 8000, webview_mode: str = 'auto'):
        # import uvicorn lazily
        try:
            import uvicorn
        except Exception as e:
            raise RuntimeError('Missing uvicorn dependency: {}'.format(e))
        url = f'http://{host}:{port}/'

        # Determine desired mode: 'auto' (try native then browser), 'native' (require pywebview), 'browser' (open system browser), 'none' (do not open any UI)
        mode = (webview_mode or 'auto').lower()

        # Try to import webview only if mode allows it
        has_webview = False
        webview_module = None
        if mode in ('auto', 'native'):
            try:
                import webview as webview_module
                has_webview = True
            except Exception:
                has_webview = False

        if mode == 'none':
            # Explicitly do not open any UI; just run the server in current process
            print(f"* Starting Pylo Web UI server (no UI) at {url}")
            uvicorn.run(app, host=host, port=port)
            return

        if has_webview and mode in ('auto', 'native'):
            # Run uvicorn in a background daemon thread so we can open a native window on the main thread.
            def _run_uvicorn():
                try:
                    uvicorn.run(app, host=host, port=port)
                except Exception:
                    # If uvicorn fails, ensure we don't crash silently
                    import traceback
                    traceback.print_exc()

            uvicorn_thread = threading.Thread(target=_run_uvicorn, daemon=True)
            uvicorn_thread.start()

            # Wait briefly for the server to bind (best-effort). Use socket connect loop for robustness.
            try:
                import time, socket
                for _ in range(30):
                    try:
                        s = socket.create_connection((host, port), timeout=0.5)
                        s.close()
                        break
                    except Exception:
                        time.sleep(0.1)
            except Exception:
                # ignore timing/connect issues; webview will try to load the url anyway
                pass

            print(f"* Starting Pylo Web UI in native window at {url}")
            try:
                # create_window is non-blocking; start() will block until window closed
                # Keep a reference to the window so we can attach event handlers
                try:
                    window = webview_module.create_window('Pylo', url)
                except Exception:
                    # Some pywebview variants may require positional args only
                    window = webview_module.create_window('Pylo', url)

                # When the window is closed we want to terminate the whole process
                def _on_window_closed(*args, **kwargs):
                    try:
                        os._exit(0)
                    except Exception:
                        # If os._exit is not available for some reason, attempt sys.exit
                        try:
                            sys.exit(0)
                        except Exception:
                            pass

                # Attach to common event names across pywebview versions; be tolerant if they don't exist
                try:
                    window.events.closed += _on_window_closed
                except Exception:
                    try:
                        window.events.closing += _on_window_closed
                    except Exception:
                        try:
                            window.events.destroyed += _on_window_closed
                        except Exception:
                            # If none of the events exist, rely on start() returning and the final os._exit(0) below.
                            pass

                webview_module.start()
            except Exception:
                # If webview fails at runtime, fallback to opening external browser
                try:
                    webbrowser.open_new_tab(url)
                except Exception:
                    pass
            finally:
                try:
                    # Exit the process when the webview window is closed to ensure uvicorn thread stops
                    pylo.log.info('Web UI window closed; shutting down server...')
                    os._exit(0)
                except Exception:
                    try:
                        pylo.log.info()
                        sys.exit(0)
                    except Exception:
                        pass

        else:
            # No pywebview available or mode explicitly set to 'browser': open the system browser (best-effort) and run uvicorn in current process.
            if mode == 'native' and not has_webview:
                print("* Requested native webview but pywebview is not available; falling back to system browser")

            def _open_browser():
                try:
                    webbrowser.open_new_tab(url)
                except Exception:
                    # best-effort only; do not fail startup if browser can't be opened
                    pass
            try:
                # schedule browser open in background
                threading.Timer(1.0, _open_browser).start()
            except Exception:
                pass

            print(f"* Starting Pylo Web UI server and opening {url}")
            uvicorn.run(app, host=host, port=port)

else:
    def start_server(host: str = '127.0.0.1', port: int = 8000):
        raise RuntimeError('FastAPI (fastapi, uvicorn) is not installed. Please install required dependencies to use web mode.')
