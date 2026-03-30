import importlib

import pytest


def test_web_module_importable():
    w = importlib.import_module('illumio_pylo.cli.web')
    assert hasattr(w, 'start_server')


def test_start_server_raises_when_fastapi_missing():
    w = importlib.import_module('illumio_pylo.cli.web')
    # If FASTAPI is not available, start_server should raise RuntimeError
    if not getattr(w, 'FASTAPI_AVAILABLE', False):
        with pytest.raises(RuntimeError):
            w.start_server()
    else:
        pytest.skip('FastAPI is available in this environment; skipping missing-dependency expectation')

