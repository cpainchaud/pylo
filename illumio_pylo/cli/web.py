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
                for m in re.finditer(r"args\[['\"]([a-zA-Z0-9_\-]+)['\"]]", src):
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


    def start_server(host: str = '127.0.0.1', port: int = 8000):
        # import uvicorn lazily
        try:
            import uvicorn
        except Exception as e:
            raise RuntimeError('Missing uvicorn dependency: {}'.format(e))
        uvicorn.run(app, host=host, port=port)

else:
    def start_server(host: str = '127.0.0.1', port: int = 8000):
        raise RuntimeError('FastAPI (fastapi, uvicorn) is not installed. Please install required dependencies to use web mode.')
