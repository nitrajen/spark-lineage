"""
Local HTTP server for spark-lineage reports.

Usage:
    import spark_lineage as spl

    spl.save_report(orders, path="./sales_lineage", name="Sales")
    spl.serve_report("./sales_lineage")          # opens browser, blocks until Ctrl-C
    spl.serve_report("./sales_lineage", port=9000, open_browser=False)
"""
from __future__ import annotations

import http.server
import os
import socketserver
import threading
import webbrowser


def serve_report(path: str, port: int = 8765, open_browser: bool = True) -> None:
    """
    Serve <path>.html on a local HTTP server at http://localhost:{port}.

    path         — same prefix passed to save_report() (no extension needed).
    port         — TCP port, default 8765.
    open_browser — open the system browser automatically (default True).

    Blocking — press Ctrl-C to stop.
    """
    html_path = path + ".html" if not path.endswith(".html") else path
    if not os.path.exists(html_path):
        raise FileNotFoundError(
            f"Report not found: {html_path}\n"
            f"Run spl.save_report(..., path='{path}') first."
        )

    directory = os.path.dirname(os.path.abspath(html_path))
    basename  = os.path.basename(html_path)

    class _Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

        def do_GET(self):
            if self.path == "/":
                self.path = "/" + basename
            super().do_GET()

        def log_message(self, fmt, *args):
            pass  # suppress access log noise

    with socketserver.TCPServer(("", port), _Handler) as httpd:
        httpd.allow_reuse_address = True
        url = f"http://localhost:{port}"
        print(f"[spark_lineage] Serving report at {url}  (Ctrl-C to stop)")
        if open_browser:
            threading.Timer(0.4, webbrowser.open, args=[url]).start()
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[spark_lineage] Server stopped.")
