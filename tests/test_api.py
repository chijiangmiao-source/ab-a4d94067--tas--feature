"""HTTP 接口层测试：在随机端口启动真实服务进行端到端验证。"""

import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from app.server import make_handler
from app.store import DecisionStore


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class HttpServerFixture:
    def __init__(self) -> None:
        self.store = DecisionStore()
        self.httpd = ThreadingHTTPServer(
            ("127.0.0.1", free_port()), make_handler(self.store)
        )
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> "HttpServerFixture":
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def call(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())


BASE_PAYLOAD = {
    "audit_id": "API-001",
    "gate_period": 1000,
    "flows": [
        {"flow_id": "A", "priority": 0, "period": 1000,
         "transmit_time": 100, "deadline": 1000}
    ],
    "gate_entries": [{"start": 0, "end": 500, "priorities": [0]}],
}


class ApiTests(unittest.TestCase):
    def test_health_and_page(self):
        with HttpServerFixture() as srv:
            status, body = srv.call("GET", "/api/health")
            self.assertEqual(status, 200)
            self.assertTrue(body["ok"])
            req = urllib.request.Request(srv.base + "/")
            with urllib.request.urlopen(req, timeout=5) as resp:
                self.assertEqual(resp.status, 200)
                self.assertIn("冻结", resp.read().decode())

    def test_submit_freeze_read_idempotent_conflict(self):
        with HttpServerFixture() as srv:
            status, body = srv.call("POST", "/api/submit", BASE_PAYLOAD)
            self.assertEqual(status, 201)
            self.assertTrue(body["created"])
            h1 = body["content_hash"]
            self.assertEqual(body["decision"]["verdict"], "SCHEDULABLE")

            # 完全相同的提交 -> 同一冻结结论。
            status, body = srv.call("POST", "/api/submit", BASE_PAYLOAD)
            self.assertEqual(status, 200)
            self.assertFalse(body["created"])
            self.assertEqual(body["content_hash"], h1)

            # 读取接口返回同一冻结结论。
            status, body = srv.call("GET", "/api/decisions/API-001")
            self.assertEqual(status, 200)
            self.assertEqual(body["content_hash"], h1)
            self.assertTrue(body["frozen"])

            # 同一标识不同内容 -> 409，原裁决不变。
            changed = json.loads(json.dumps(BASE_PAYLOAD))
            changed["gate_entries"][0]["end"] = 400
            status, body = srv.call("POST", "/api/submit", changed)
            self.assertEqual(status, 409)
            self.assertEqual(body["error"], "AUDIT_CONFLICT")
            self.assertNotEqual(body["stored_hash"], body["incoming_hash"])
            self.assertEqual(body["existing_decision"]["content_hash"], h1)

            status, body = srv.call("GET", "/api/decisions/API-001")
            self.assertEqual(body["content_hash"], h1)
            self.assertEqual(
                body["request"]["gate_entries"][0]["end"], 500
            )

    def test_validation_error_and_missing(self):
        with HttpServerFixture() as srv:
            bad = json.loads(json.dumps(BASE_PAYLOAD))
            bad["audit_id"] = "API-BAD"
            bad["flows"] = []
            status, body = srv.call("POST", "/api/submit", bad)
            self.assertEqual(status, 400)
            self.assertEqual(body["error"], "VALIDATION_FAILED")

            status, body = srv.call("GET", "/api/decisions/UNKNOWN")
            self.assertEqual(status, 404)

            # 非 JSON 请求体 -> 400。
            req = urllib.request.Request(
                srv.base + "/api/submit",
                data=b"not-json",
                method="POST",
            )
            req.add_header("Content-Type", "application/json")
            try:
                urllib.request.urlopen(req, timeout=5)
                self.fail("应返回 400")
            except urllib.error.HTTPError as exc:
                self.assertEqual(exc.code, 400)

    def test_non_convergent_payload_via_api(self):
        with HttpServerFixture() as srv:
            payload = {
                "audit_id": "API-GROW",
                "gate_period": 1000,
                "flows": [
                    {"flow_id": "X", "priority": 0, "period": 1000,
                     "transmit_time": 900, "deadline": 1000}
                ],
                "gate_entries": [{"start": 0, "end": 100, "priorities": [0]}],
            }
            status, body = srv.call("POST", "/api/submit", payload)
            self.assertEqual(status, 201)
            self.assertEqual(body["decision"]["verdict"], "NON_CONVERGENT")
            chain = body["decision"]["growth_chain"]
            self.assertGreaterEqual(len(chain), 2)


if __name__ == "__main__":
    unittest.main()
