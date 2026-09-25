"""HTTP 端到端契约：冻结、离线灌包、处置、覆盖、视图与回连合并。"""

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


class SliceConflictHttpTest(unittest.TestCase):
    """HTTP 场景：同编号异内容切片被拒、留痕并按角色呈现，且不升级风险。"""

    @classmethod
    def setUpClass(cls):
        import copy as _copy
        from app import EdgeApp
        from protocol import FrozenProtocol
        cls.protocol = FrozenProtocol.load()
        cls.server = HttpApiTest._start_server(EdgeApp(cls.protocol))
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        with open(os.path.join(FIXTURES, "incidents.json"), encoding="utf-8") as handle:
            incidents = {i["incident_id"]: i for i in json.load(handle)["incidents"]}
        cls.heat = incidents["INC-HEAT-01"]
        cls.sid = cls.heat["subject_id"]
        # 干净对照任务与冲突任务：两者最终风险时间线必须一致。
        cls._freeze_and_feed("M-HTTP-CLEAN")
        cls._freeze_and_feed("M-HTTP-CONF")
        # 固件回滚场景：窗口定稿后，同 slice_id 携带 42.0℃ 重传。
        import copy
        cls.rolled = copy.deepcopy(cls.heat["slices"][0])  # H-TH-1
        cls.rolled["samples"][0]["v"] = 42.0
        status, body = post(cls.base, "/missions/M-HTTP-CONF/ingest", {
            "subject_id": cls.sid,
            "slice": cls.rolled,
            "received_at": "2026-09-10T08:10:00Z",
        })
        assert status == 200 and body["outcome"] == "conflict", body

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server._worker_thread.join(timeout=2)

    @classmethod
    def _freeze_and_feed(cls, mission_id):
        post(cls.base, "/missions", {
            "mission_id": mission_id,
            "subject_ids": [cls.sid],
            "started_at": cls.heat["base_time"],
            "profiles": {cls.sid: "高温"},
        })
        for slice_ref in cls.heat["slices"]:
            status, body = post(cls.base, f"/missions/{mission_id}/ingest", {
                "subject_id": cls.sid,
                "slice": slice_ref,
                "received_at": cls.heat["base_time"],
            })
            assert status == 200 and body["outcome"] in ("accepted", "duplicate")
        post(cls.base, f"/missions/{mission_id}/heartbeat",
             {"now": "2026-09-10T08:06:00Z"})

    def test_rollback_slice_is_conflict_via_http(self):
        import copy
        rolled = copy.deepcopy(self.heat["slices"][0])  # H-TH-1
        rolled["samples"][0]["v"] = 42.0
        status, body = post(self.base, "/missions/M-HTTP-CONF/ingest", {
            "subject_id": self.sid,
            "slice": rolled,
            "received_at": "2026-09-10T08:10:00Z",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["outcome"], "conflict")
        self.assertEqual(body["rejection_code"], "SLICE_CONTENT_CONFLICT")
        self.assertEqual(body["changed_fields"], ["samples"])
        self.assertEqual(body["finalized_count"], 0)

        # 字节一致的正常重发仍然幂等。
        status, body = post(self.base, "/missions/M-HTTP-CONF/ingest", {
            "subject_id": self.sid,
            "slice": self.heat["slices"][0],
            "received_at": "2026-09-10T08:10:01Z",
        })
        self.assertEqual(body["outcome"], "duplicate")

    def test_conflict_does_not_change_risk_level(self):
        _, clean = get(self.base,
                       f"/missions/M-HTTP-CLEAN/status?subject_id={self.sid}")
        _, conf = get(self.base,
                      f"/missions/M-HTTP-CONF/status?subject_id={self.sid}")
        self.assertEqual(conf["effective_level"], clean["effective_level"])
        self.assertEqual(conf["effective_level"], "intervene")

    def test_conflict_views_are_role_scoped(self):
        # 指挥人员：只见计数与编号。
        _, commander = get(
            self.base,
            "/missions/M-HTTP-CONF/conflicts?" + urlencode({"role": "指挥人员"}),
        )
        self.assertEqual(commander["slice_conflict_count"], 1)
        self.assertEqual(
            commander["slice_conflicts"][0]["slice_id"], "H-TH-1"
        )
        serialized = json.dumps(commander, ensure_ascii=False)
        for forbidden in ("fingerprint", "42.0", "changed_fields"):
            self.assertNotIn(forbidden, serialized)

        # 卫生员：见差异字段类别（中文），不见读数。
        _, corpsman = get(
            self.base,
            "/missions/M-HTTP-CONF/conflicts?"
            + urlencode({"role": "现场卫生员", "subject_id": self.sid}),
        )
        self.assertEqual(corpsman["slice_conflicts"][0]["changed_fields"],
                         ["校准前读数"])
        self.assertNotIn("42.0", json.dumps(corpsman, ensure_ascii=False))

        # 军医：见双侧指纹与内容。
        _, medic = get(
            self.base,
            "/missions/M-HTTP-CONF/conflicts?"
            + urlencode({"role": "值班军医", "subject_id": self.sid}),
        )
        item = medic["slice_conflicts"][0]
        self.assertEqual(len(item["stored_fingerprint"]), 64)
        self.assertNotEqual(item["stored_fingerprint"],
                            item["incoming_fingerprint"])

        # 任务人员无权查看冲突。
        with self.assertRaises(HTTPError) as error:
            get(
                self.base,
                "/missions/M-HTTP-CONF/conflicts?"
                + urlencode({"role": "任务人员", "subject_id": self.sid}),
            )
        self.assertEqual(error.exception.code, 400)
        error.exception.close()

    def test_conflict_is_in_journal_and_chain_intact(self):
        status, body = get(self.base, "/missions/M-HTTP-CONF/journal")
        self.assertEqual(status, 200)
        self.assertTrue(body["intact"])
        codes = [
            e["payload"]["rejection"]["code"]
            for e in body["journal"]["entries"]
            if e["type"] == "SLICE_REJECTED"
        ]
        self.assertIn("SLICE_CONTENT_CONFLICT", codes)

    def test_restart_restore_keeps_conflict_judgement(self):
        # 导出冲突任务的日志，在全新应用（模拟进程重启）上恢复。
        status, exported = get(self.base, "/missions/M-HTTP-CONF/journal")
        self.assertTrue(exported["intact"])
        from app import EdgeApp
        restarted = HttpApiTest._start_server(EdgeApp(self.protocol))
        restarted_base = f"http://127.0.0.1:{restarted.server_port}"
        try:
            status, body = post(restarted_base, "/missions/restore", {
                "journal": exported["journal"],
            })
            self.assertEqual(status, 201)
            self.assertEqual(body["mission_id"], "M-HTTP-CONF")

            # 恢复后同一回滚副本仍判冲突，正常重发仍幂等。
            status, body = post(
                restarted_base, "/missions/M-HTTP-CONF/ingest", {
                    "subject_id": self.sid,
                    "slice": self.rolled,
                    "received_at": "2026-09-10T08:11:00Z",
                })
            self.assertEqual(body["outcome"], "conflict")
            self.assertEqual(body["rejection_code"], "SLICE_CONTENT_CONFLICT")
            status, body = post(
                restarted_base, "/missions/M-HTTP-CONF/ingest", {
                    "subject_id": self.sid,
                    "slice": self.heat["slices"][0],
                    "received_at": "2026-09-10T08:11:01Z",
                })
            self.assertEqual(body["outcome"], "duplicate")

            # 风险等级与冲突历史在重启后原样保留。
            _, status_body = get(
                restarted_base,
                f"/missions/M-HTTP-CONF/status?subject_id={self.sid}",
            )
            self.assertEqual(status_body["effective_level"], "intervene")
            _, conflicts = get(
                restarted_base,
                "/missions/M-HTTP-CONF/conflicts?"
                + urlencode({"role": "指挥人员"}),
            )
            # 停机前 1 条冲突 + 恢复后这次回滚重传 1 条，两次都留痕。
            self.assertEqual(conflicts["slice_conflict_count"], 2)
        finally:
            restarted.shutdown()
            restarted.server_close()
            restarted._worker_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
