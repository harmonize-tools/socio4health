import http.server
import os
import threading
import zipfile
from unittest.mock import patch

import pytest
import requests

from socio4health import Extractor
from socio4health.utils import extractor_utils
from socio4health.utils.extractor_utils import download_request

PAYLOAD = b"x" * (3 * 1024 * 1024 + 123)  # a few chunks


@pytest.fixture
def server():
    """Local HTTP server: /file.zip returns PAYLOAD, anything else 404. Counts GET requests."""
    hits = {"count": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits["count"] += 1
            if self.path != "/file.zip":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers()
            self.wfile.write(PAYLOAD)

        def log_message(self, *args):
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_port}", hits
    httpd.shutdown()


def test_downloads_file_and_reports_progress(server, tmp_path):
    base, _ = server
    seen = []
    path = download_request(f"{base}/file.zip", "file.zip", str(tmp_path),
                            on_progress=lambda done, total: seen.append((done, total)))

    assert path == os.path.join(str(tmp_path), "file.zip")
    assert open(path, "rb").read() == PAYLOAD
    assert seen[-1] == (len(PAYLOAD), len(PAYLOAD))
    assert len(seen) > 1
    assert not os.path.exists(path + ".part")


def test_skips_file_already_downloaded(server, tmp_path):
    base, _ = server
    (tmp_path / "file.zip").write_bytes(PAYLOAD)
    with patch("builtins.open", side_effect=AssertionError("file was downloaded again")):
        assert download_request(f"{base}/file.zip", "file.zip", str(tmp_path)) is not None


def test_redownloads_incomplete_file(server, tmp_path):
    base, _ = server
    (tmp_path / "file.zip").write_bytes(PAYLOAD[:10])
    path = download_request(f"{base}/file.zip", "file.zip", str(tmp_path))
    assert open(path, "rb").read() == PAYLOAD


def test_failure_returns_none_after_retries_and_leaves_no_partial_file(server, tmp_path):
    base, hits = server
    with patch.object(extractor_utils.time, "sleep"):
        assert download_request(f"{base}/missing.zip", "missing.zip", str(tmp_path), retries=3) is None
    assert hits["count"] == 3
    assert os.listdir(tmp_path) == []


def test_timeout_is_retried(tmp_path):
    calls = {"n": 0}

    def flaky_get(*args, **kwargs):
        calls["n"] += 1
        assert kwargs.get("timeout"), "requests.get must always use a timeout"
        raise requests.exceptions.ReadTimeout("server stopped sending data")

    with patch.object(extractor_utils.requests, "get", side_effect=flaky_get), \
            patch.object(extractor_utils.time, "sleep"):
        assert download_request("http://example.invalid/a.zip", "a.zip", str(tmp_path), retries=2) is None
    assert calls["n"] == 2


def test_failed_downloads_do_not_crash_processing(tmp_path):
    archive = tmp_path / "AC.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("AC_Domicilios.txt", "1201\n")

    ext = Extractor(input_path="http://example.invalid/", down_ext=[".zip", ".txt"], output_path=str(tmp_path),
                    is_fwf=True, colnames=["UF", "N"], colspecs=[(0, 2), (2, 4)])
    ext._process_downloaded_files([None, str(archive)])  # None = a download that failed

    assert len(ext.dataframes) == 1
