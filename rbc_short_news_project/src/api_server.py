import json
import os
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from rbc_short_news_parser import RBC_BASE_URL, RBC_RSSHUB_FEEDS, run, setup_logging


DEFAULT_OPTIONS: dict[str, Any] = {
    "hours": 24,
    "rbc_max_pages": 300,
    "rbc_max_items": 3000,
    "rbc_fulltext_limit": 200,
    "rbc_rss_urls": RBC_RSSHUB_FEEDS,
    "rbc_cookie": os.getenv("RBC_COOKIE", ""),
    "rbc_referer": os.getenv("RBC_REFERER", RBC_BASE_URL),
    "dzen_max_pages": 8,
    "ria_max_items": 300,
    "ria_fulltext_limit": 150,
    "dzen_max_items": 200,
    "dzen_fulltext_limit": 80,
    "lenta_max_items": 200,
    "lenta_fulltext_limit": 80,
    "tproger_max_items": 200,
    "tproger_fulltext_limit": 80,
    "ren_max_items": 200,
    "ren_fulltext_limit": 80,
    "mk_max_items": 200,
    "mk_fulltext_limit": 80,
    "m24_max_items": 200,
    "m24_fulltext_limit": 80,
    "gazeta_max_items": 200,
    "gazeta_fulltext_limit": 80,
    "max_search_seconds": 300,
    "output_path": "",
}


def _coerce_int(payload: dict[str, Any], key: str, fallback: int) -> int:
    value = payload.get(key, fallback)
    try:
        return int(value)
    except Exception:
        return int(fallback)


def _build_options(payload: dict[str, Any]) -> dict[str, Any]:
    opts = dict(DEFAULT_OPTIONS)
    int_keys = [
        "hours",
        "rbc_max_pages",
        "rbc_max_items",
        "rbc_fulltext_limit",
        "dzen_max_pages",
        "ria_max_items",
        "ria_fulltext_limit",
        "dzen_max_items",
        "dzen_fulltext_limit",
        "lenta_max_items",
        "lenta_fulltext_limit",
        "tproger_max_items",
        "tproger_fulltext_limit",
        "ren_max_items",
        "ren_fulltext_limit",
        "mk_max_items",
        "mk_fulltext_limit",
        "m24_max_items",
        "m24_fulltext_limit",
        "gazeta_max_items",
        "gazeta_fulltext_limit",
        "max_search_seconds",
    ]
    for key in int_keys:
        opts[key] = _coerce_int(payload, key, int(opts[key]))

    rss_urls = payload.get("rbc_rss_urls", opts["rbc_rss_urls"])
    if isinstance(rss_urls, str):
        opts["rbc_rss_urls"] = [u.strip() for u in rss_urls.split(",") if u.strip()]
    elif isinstance(rss_urls, list):
        opts["rbc_rss_urls"] = [str(u).strip() for u in rss_urls if str(u).strip()]

    if "rbc_cookie" in payload:
        opts["rbc_cookie"] = str(payload.get("rbc_cookie") or "").strip()
    if "rbc_referer" in payload:
        opts["rbc_referer"] = str(payload.get("rbc_referer") or RBC_BASE_URL).strip() or RBC_BASE_URL
    if "output_path" in payload:
        opts["output_path"] = str(payload.get("output_path") or "").strip()
    return opts


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "RbcNewsApi/1.0"

    def _send_json(self, code: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"ok": True, "service": "rbc-short-news-api"})
            return
        self._send_json(404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/run":
            self._send_json(404, {"ok": False, "error": "Not found"})
            return

        try:
            content_len = int(self.headers.get("Content-Length", "0"))
            body_raw = self.rfile.read(content_len) if content_len > 0 else b"{}"
            payload = json.loads(body_raw.decode("utf-8") or "{}")
            if not isinstance(payload, dict):
                payload = {}

            log_level = str(payload.get("log_level", "INFO")).upper()
            log_file = setup_logging(log_level)
            options = _build_options(payload)

            out_file, stats = run(**options)
            json_payload = json.loads(Path(out_file).read_text(encoding="utf-8"))
            include_items = bool(payload.get("include_items", True))
            if not include_items:
                json_payload = {k: v for k, v in json_payload.items() if k != "items"}

            self._send_json(
                200,
                {
                    "ok": True,
                    "output_file": str(out_file),
                    "log_file": str(log_file),
                    "count": json_payload.get("count", 0),
                    "stats": stats,
                    "result": json_payload,
                },
            )
        except Exception as exc:
            self._send_json(
                500,
                {
                    "ok": False,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )


def main() -> None:
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8080"))
    server = ThreadingHTTPServer((host, port), ApiHandler)
    log_file = setup_logging("INFO")
    print(f"API started on http://{host}:{port} | log={log_file}")
    server.serve_forever()


if __name__ == "__main__":
    main()
