"""HTTP 端到端契约：冻结、离线灌包、处置、覆盖、视图与回连合并。"""

import copy
import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from service import Handler

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def post(base, path, payload):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(base + path, data=data, method="POST",
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def get(base, path):
    with urlopen(base + path, timeout=3) as response:
        return response.status, json.load(response)


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 每个测试类使用全新的应用状态。
        from app import EdgeApp
        from protocol import FrozenProtocol
        cls.protocol = FrozenProtocol.load()
        cls.server = cls._start_server(EdgeApp(cls.protocol))
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        with open(os.path.join(FIXTURES, "incidents.json"), encoding="utf-8") as handle:
            incidents = {i["incident_id"]: i for i in json.load(handle)["incidents"]}
        cls.heat = incidents["INC-HEAT-01"]

    @staticmethod
    def _start_server(app):
        handler_cls = type("BoundHandler", (Handler,), {"app": app})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        server._worker_thread = thread
        return server

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server._worker_thread.join(timeout=2)

    def _freeze_and_feed(self, mission_id):
        status, body = post(self.base, "/missions", {
            "mission_id": mission_id,
            "subject_ids": [self.heat["subject_id"]],
            "started_at": self.heat["base_time"],
            "profiles": {self.heat["subject_id"]: "高温"},
        })
        self.assertEqual(status, 201)
        self.assertEqual(body["protocol"]["version"], self.protocol.version)
        self.assertEqual(len(body["protocol"]["protocol_hash"]), 64)
        for index, slice_ref in enumerate(self.heat["slices"]):
            status, body = post(self.base, f"/missions/{mission_id}/ingest", {
                "subject_id": self.heat["subject_id"],
                "slice": slice_ref,
                "received_at": self.heat["base_time"],
            })
            self.assertEqual(status, 200, body)
            self.assertIn(body["outcome"], ("accepted", "duplicate"))
        # 全部切片同一时刻送达，由心跳统一推动定稿。
        status, body = post(self.base, f"/missions/{mission_id}/heartbeat", {
            "now": "2026-09-10T08:06:00Z",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["finalized_count"], 4)

    def test_end_to_end_alert_workflow_and_commander_view(self):
        self._freeze_and_feed("M-HTTP-1")
        sid = self.heat["subject_id"]
        status, body = get(self.base, f"/missions/M-HTTP-1/status?subject_id={sid}")
        self.assertEqual(status, 200)
        self.assertEqual(body["state"], "干预中")
        self.assertEqual(body["effective_level"], "intervene")

        status, commander = get(
            self.base,
            "/missions/M-HTTP-1/view?" + urlencode({"role": "指挥人员"}),
        )
        self.assertEqual(status, 200)
        serialized = json.dumps(commander, ensure_ascii=False)
        self.assertIn("转运", str(commander["roster"][0]["pending_actions"]))
        for forbidden in ("core_temp", "rule_hits", "40.4"):
            self.assertNotIn(forbidden, serialized)

    def test_action_permissions_and_override_reason(self):
        self._freeze_and_feed("M-HTTP-2")
        sid = self.heat["subject_id"]
        # 卫生员下令转运必须被拒。
        status, body = post(self.base, "/missions/M-HTTP-2/actions", {
            "subject_id": sid, "action": "转运", "actor": "MEDIC-A1",
            "at": "2026-09-10T08:04:30Z", "role": "现场卫生员",
        })
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "mission_error")

        # 军医覆盖不填理由必须被拒。
        status, body = post(self.base, "/missions/M-HTTP-2/override", {
            "subject_id": sid, "medic_id": "DOC-7",
            "at": "2026-09-10T08:05:00Z", "forced_level": "review", "reason": "",
        })
        self.assertEqual(status, 400)

        # 军医覆盖有效，且指挥视图可见 overridden 标记。
        status, _ = post(self.base, "/missions/M-HTTP-2/override", {
            "subject_id": sid, "medic_id": "DOC-7",
            "at": "2026-09-10T08:05:00Z", "forced_level": "review",
            "reason": "现场复核后决定边观察边后撤",
        })
        self.assertEqual(status, 200)
        _, commander = get(
            self.base,
            "/missions/M-HTTP-2/view?" + urlencode({"role": "指挥人员"}),
        )
        self.assertTrue(commander["roster"][0]["overridden"])

    def test_journal_intact_and_sync_merge_roundtrip(self):
        self._freeze_and_feed("M-HTTP-3")
        status, body = get(self.base, "/missions/M-HTTP-3/journal")
        self.assertEqual(status, 200)
        self.assertTrue(body["intact"])
        eval_entries = [e for e in body["journal"]["entries"]
                        if e["type"] == "ASSESSMENT"]
        self.assertEqual(len(eval_entries), 4)

        # 导出该边缘设备的离线日志包。
        status, bundle = post(self.base, "/missions/M-HTTP-3/sync/export", {
            "device_id": "EDGE-9",
        })
        self.assertEqual(status, 200)
        self.assertEqual(bundle["protocol_hash"], self.protocol.protocol_hash)

        # 第二个边缘节点（同任务编号、同冻结协议）接收日志包。
        from app import EdgeApp
        host_server = self._start_server(EdgeApp(self.protocol))
        host_base = f"http://127.0.0.1:{host_server.server_port}"
        post(host_base, "/missions", {
            "mission_id": "M-HTTP-3",
            "subject_ids": [self.heat["subject_id"]],
            "started_at": self.heat["base_time"],
            "profiles": {self.heat["subject_id"]: "高温"},
        })
        status, report = post(host_base, "/missions/M-HTTP-3/sync/merge", {
            "bundles": [bundle],
        })
        self.assertEqual(status, 200)
        self.assertTrue(report["merged"])
        status, report2 = post(host_base, "/missions/M-HTTP-3/sync/merge", {
            "bundles": [bundle],
        })
        self.assertTrue(report2["duplicates"])
        self.assertFalse(report2["merged"])
        # 汇入后该节点风险时间线与源节点一致。
        status, host_status = get(
            host_base,
            f"/missions/M-HTTP-3/status?subject_id={self.heat['subject_id']}",
        )
        self.assertEqual(host_status["effective_level"], "intervene")
        host_server.shutdown()
        host_server.server_close()

    def test_missing_field_returns_structured_error(self):
        status, body = post(self.base, "/missions", {"mission_id": "X"})
        self.assertEqual(status, 400)
        self.assertIn("subject_ids", body["message"])

    def test_slice_conflict_rejected_and_visible_without_level_change(self):
        self._freeze_and_feed("M-HTTP-CF")
        sid = self.heat["subject_id"]
        _, before = get(self.base, f"/missions/M-HTTP-CF/status?subject_id={sid}")
        self.assertEqual(before["effective_level"], "intervene")

        # 逐字节语义一致的重投：幂等重复。
        original = self.heat["slices"][0]
        status, body = post(self.base, "/missions/M-HTTP-CF/ingest", {
            "subject_id": sid, "slice": original,
            "received_at": "2026-09-10T08:07:00Z",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["outcome"], "duplicate")
        self.assertIsNone(body["rejection"])

        # 同一切片编号携带不同读数（疑似固件回滚）：登记冲突并拒绝。
        clash = copy.deepcopy(original)
        clash["samples"][0]["v"] = 45.0
        status, body = post(self.base, "/missions/M-HTTP-CF/ingest", {
            "subject_id": sid, "slice": clash,
            "received_at": "2026-09-10T08:07:30Z",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["outcome"], "rejected")
        self.assertEqual(body["rejection"]["code"], "SLICE_CONFLICT")
        self.assertEqual(body["rejection"]["diff"], ["samples"])
        # 同一冲突内容再次投递，判定保持一致。
        status, body2 = post(self.base, "/missions/M-HTTP-CF/ingest", {
            "subject_id": sid, "slice": clash,
            "received_at": "2026-09-10T08:07:40Z",
        })
        self.assertEqual(body2["rejection"]["code"], "SLICE_CONFLICT")

        # 风险等级不被冲突切片推进。
        _, after = get(self.base, f"/missions/M-HTTP-CF/status?subject_id={sid}")
        self.assertEqual(after["effective_level"], "intervene")
        self.assertEqual(len(after["slice_conflicts"]), 2)

        # 冲突进入不可篡改日志，哈希链完整。
        _, journal = get(self.base, "/missions/M-HTTP-CF/journal")
        self.assertTrue(journal["intact"])
        conflicts = [
            e for e in journal["journal"]["entries"]
            if e["type"] == "SLICE_REJECTED"
            and e["payload"]["rejection"]["code"] == "SLICE_CONFLICT"
        ]
        self.assertEqual(len(conflicts), 2)

        # 军医视图可见冲突细节；指挥视图只有计数且无读数泄漏。
        _, medic = get(self.base, "/missions/M-HTTP-CF/view?"
                       + urlencode({"role": "值班军医", "subject_id": sid}))
        self.assertEqual(medic["detail"]["slice_conflicts"][0]["diff"], ["samples"])
        _, commander = get(self.base, "/missions/M-HTTP-CF/view?"
                           + urlencode({"role": "指挥人员"}))
        self.assertEqual(commander["roster"][0]["slice_conflict_count"], 2)
        serialized = json.dumps(commander, ensure_ascii=False)
        self.assertNotIn("45.0", serialized)
        self.assertNotIn('"established"', serialized)

    def test_unregistered_device_rejected_over_http(self):
        self._freeze_and_feed("M-HTTP-DEV")
        sid = self.heat["subject_id"]
        rogue = copy.deepcopy(self.heat["slices"][0])
        rogue["slice_id"] = "ROGUE-1"
        rogue["device_id"] = "EVIL-9"
        status, body = post(self.base, "/missions/M-HTTP-DEV/ingest", {
            "subject_id": sid, "slice": rogue,
            "received_at": "2026-09-10T08:07:00Z",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["outcome"], "rejected")
        self.assertEqual(body["rejection"]["code"], "UNKNOWN_DEVICE")


if __name__ == "__main__":
    unittest.main()
