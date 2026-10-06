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

    def test_wait_chain_endpoint_and_errors(self):
        with HttpServerFixture() as srv:
            status, body = srv.call("POST", "/api/submit", BASE_PAYLOAD)
            self.assertEqual(status, 201)
            h = body["content_hash"]
            # 裁决体内含实例目录。
            self.assertIn("instances", body["decision"])
            self.assertTrue(
                any(i["frame"] == "A#0"
                    for i in body["decision"]["instances"]["instances"])
            )

            # A#0 释放即发送，等待为空的连续链。
            status, body = srv.call("GET", "/api/decisions/API-001/wait-chain?frame=A%230")
            self.assertEqual(status, 200)
            ch = body["wait_chain"]
            self.assertEqual(ch["coverage"], [0, 0])
            self.assertEqual(ch["total_wait_us"], 0)
            self.assertEqual(body["content_hash"], h)

            # 未带 frame -> 400。
            status, body = srv.call("GET", "/api/decisions/API-001/wait-chain")
            self.assertEqual(status, 400)
            self.assertEqual(body["error"], "BAD_REQUEST")

            # 从未出现的实例 -> 404 INSTANCE_NOT_FOUND。
            status, body = srv.call(
                "GET", "/api/decisions/API-001/wait-chain?frame=ZZ%239"
            )
            self.assertEqual(status, 404)
            self.assertEqual(body["error"], "INSTANCE_NOT_FOUND")

            # 未知裁决 -> 404 NOT_FOUND。
            status, body = srv.call(
                "GET", "/api/decisions/NOPE/wait-chain?frame=A%230"
            )
            self.assertEqual(status, 404)
            self.assertEqual(body["error"], "NOT_FOUND")

            # 错误查询不改写冻结裁决。
            status, body = srv.call("GET", "/api/decisions/API-001")
            self.assertEqual(status, 200)
            self.assertEqual(body["content_hash"], h)

    def test_wait_chain_contiguous_segments_via_api(self):
        with HttpServerFixture() as srv:
            payload = {
                "audit_id": "API-WC",
                "gate_period": 1000,
                "flows": [
                    {"flow_id": "HI", "priority": 0, "period": 2000,
                     "transmit_time": 400, "deadline": 2000},
                    {"flow_id": "LO", "priority": 1, "period": 2000,
                     "transmit_time": 400, "deadline": 2000},
                ],
                "gate_entries": [{"start": 0, "end": 400, "priorities": [0, 1]}],
            }
            srv.call("POST", "/api/submit", payload)
            status, body = srv.call(
                "GET", "/api/decisions/API-WC/wait-chain?frame=LO%230"
            )
            self.assertEqual(status, 200)
            ivs = body["wait_chain"]["intervals"]
            self.assertGreater(len(ivs), 1)
            for a, b in zip(ivs, ivs[1:]):
                self.assertEqual(a["to"], b["from"])   # 连续无重叠
            self.assertEqual(ivs[0]["from"], 0)
            self.assertEqual(ivs[-1]["to"], 1000)
            kinds = {i["blocking_category"] for i in ivs}
            self.assertIn("higher_priority_tx", kinds)
            self.assertIn("gate_closed", kinds)
            # 占用段必须关联来源帧与释放时刻。
            hp = next(i for i in ivs if i["blocking_category"] == "higher_priority_tx")
            self.assertEqual(hp["source_frame"], "HI#0")
            self.assertEqual(hp["source_release"], 0)


if __name__ == "__main__":
    unittest.main()
