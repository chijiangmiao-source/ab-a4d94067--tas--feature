"""HTTP 服务：静态页面 + 审计裁决接口（仅用标准库）。"""

from __future__ import annotations

import argparse
import json
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from .models import ValidationError, parse_request
from .scheduler import build_wait_chain
from .store import ConflictError, DecisionStore

STATIC_DIR = Path(__file__).resolve().parent / "static"

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
}


def make_handler(store: DecisionStore) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "TASAudit/1.0"

        def log_message(self, fmt: str, *args: object) -> None:  # noqa: A003
            sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

        # ---- 工具 ----

        def _json(self, payload: object, status: int = 200, headers: dict | None = None) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _error(self, status: int, code: str, message: str, extra: dict | None = None) -> None:
            payload: dict = {"ok": False, "error": code, "message": message}
            if extra:
                payload.update(extra)
            self._json(payload, status)

        # ---- 路由 ----

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path in ("/", "/index.html"):
                self._serve_static("index.html")
            elif path.startswith("/static/"):
                name = path[len("/static/"):]
                self._serve_static(unquote(name))
            elif path == "/api/health":
                self._json({"ok": True, "frozen_ids": store.list_ids()})
            elif path.startswith("/api/decisions/"):
                rest = unquote(path[len("/api/decisions/"):])
                parts = rest.split("/")
                if len(parts) == 4 and parts[1] == "waits":
                    # /api/decisions/<audit_id>/waits/<flow_id>/<seq>
                    self._serve_wait_chain(parts[0], parts[2], parts[3])
                    return
                audit_id = rest
                decision = store.get(audit_id)
                if decision is None:
                    self._error(404, "NOT_FOUND", f"审计标识 {audit_id} 尚无冻结裁决")
                else:
                    self._json({"ok": True, **decision.to_json()})
            else:
                self._error(404, "NOT_FOUND", "资源不存在")

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path != "/api/submit":
                self._error(404, "NOT_FOUND", "资源不存在")
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._error(400, "BAD_REQUEST", "Content-Length 无效")
                return
            raw = self.rfile.read(length) if length > 0 else b""
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._error(400, "BAD_REQUEST", "请求体必须是合法 UTF-8 JSON")
                return
            try:
                req = parse_request(data)
            except ValidationError as exc:
                self._error(400, "VALIDATION_FAILED", str(exc))
                return
            try:
                decision, created = store.submit(req)
            except ConflictError as exc:
                existing = store.get(exc.audit_id)
                self._error(
                    409,
                    "AUDIT_CONFLICT",
                    str(exc),
                    {
                        "audit_id": exc.audit_id,
                        "stored_hash": exc.stored_hash,
                        "incoming_hash": exc.incoming_hash,
                        # 原冻结裁决保持不变并随冲突响应回显。
                        "existing_decision": existing.to_json() if existing else None,
                    },
                )
                return
            self._json(
                {"ok": True, "created": created, **decision.to_json()},
                201 if created else 200,
            )

        # ---- 静态资源 ----

        def _serve_wait_chain(self, audit_id: str, flow_id: str, seq_raw: str) -> None:
            """按实例查询已冻结裁决的逐段等待链（只读，绝不改写冻结结果）。"""
            decision = store.get(audit_id)
            if decision is None:
                self._error(404, "NOT_FOUND", f"审计标识 {audit_id} 尚无冻结裁决")
                return
            try:
                seq = int(seq_raw)
                if seq < 0:
                    raise ValueError
            except ValueError:
                self._error(400, "INVALID_INSTANCE", "实例序号必须是非负整数")
                return
            payload, err = build_wait_chain(decision.waits, flow_id, seq)
            if err is not None:
                status, code, message = err
                self._error(status, code, message)
                return
            self._json(
                {
                    "ok": True,
                    "audit_id": audit_id,
                    "content_hash": decision.content_hash,
                    **payload,
                }
            )

        # ---- 静态资源 ----

        def _serve_static(self, name: str) -> None:
            # 防止路径穿越。
            target = (STATIC_DIR / name).resolve()
            if not str(target).startswith(str(STATIC_DIR.resolve()) + "/") or not target.is_file():
                self._error(404, "NOT_FOUND", f"静态资源 {name} 不存在")
                return
            ctype = _CONTENT_TYPES.get(target.suffix, "application/octet-stream")
            body = target.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return Handler


def serve(host: str = "0.0.0.0", port: int = 8080) -> None:
    store = DecisionStore()
    httpd = ThreadingHTTPServer((host, port), make_handler(store))
    print(f"TAS 审计裁决服务监听 http://{host}:{port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="机载以太网 TAS 门控调度审计服务")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
