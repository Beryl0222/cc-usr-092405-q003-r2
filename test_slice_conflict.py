"""切片内容冲突链路测试：字节幂等、同编号异内容拒绝、留痕不升级、合并/重启一致。

固件回滚后设备可能用同一 slice_id 重传携带不同读数的切片。本组测试
证明：
- 字节语义一致的重复投递继续幂等（JSON 键序不同也算一致）；
- 设备、序号、采样时间或任一校准前读数变化一律登记
  SLICE_CONTENT_CONFLICT 并拒绝入窗；
- 冲突进入哈希链日志与角色化视图，但绝不推进风险等级；
- 离线合并、处理时间水位、窗口定稿与任务重启后判断完全一致；
- 既有迟到、乱序、校准规则不回归（由其余测试文件保证）。
"""

import copy
import json
import os
import unittest

from mission import Mission, MissionError
from protocol import FrozenProtocol
from sync import export_bundle, merge_bundles
from timeutil import parse_ts
from views import render

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def protocol():
    return FrozenProtocol.load(os.path.join(FIXTURES, "protocol.json"))


def incidents():
    with open(os.path.join(FIXTURES, "incidents.json"), encoding="utf-8") as handle:
        return {i["incident_id"]: i for i in json.load(handle)["incidents"]}


def heat_mission(mission_id="M-CONF", proto=None):
    proto = proto or protocol()
    data = incidents()["INC-HEAT-01"]
    mission = Mission(
        mission_id, proto, [data["subject_id"]], data["base_time"],
        profiles={data["subject_id"]: "高温"},
    )
    return mission, data


def feed(mission, data, heartbeat=True):
    base = parse_ts(data["base_time"])
    for index, slice_ref in enumerate(data["slices"]):
        mission.ingest(data["subject_id"], slice_ref, base - 1 + index * 0.001)
    if heartbeat:
        max_sampled = max(parse_ts(s["sampled_at"]) for s in data["slices"])
        mission.heartbeat(
            max_sampled + mission.protocol.window_seconds
            + mission.protocol.allowed_lateness_seconds + 1
        )


def find_slice(data, slice_id):
    return copy.deepcopy(next(s for s in data["slices"] if s["slice_id"] == slice_id))


class IdempotencyTest(unittest.TestCase):
    def test_byte_identical_redelivery_stays_idempotent_even_reordered_keys(self):
        mission, data = heat_mission()
        sid = data["subject_id"]
        first = mission.ingest(sid, data["slices"][0], data["base_time"])
        self.assertEqual(first["outcome"], "accepted")
        # JSON 对象键插入顺序不同，但规范化序列化后字节一致 → 仍幂等。
        reordered = {
            "samples": copy.deepcopy(data["slices"][0]["samples"]),
            "observed_at": data["slices"][0]["observed_at"],
            "sampled_at": data["slices"][0]["sampled_at"],
            "seq": data["slices"][0]["seq"],
            "device_id": data["slices"][0]["device_id"],
            "slice_id": data["slices"][0]["slice_id"],
        }
        second = mission.ingest(sid, reordered, "2026-09-10T08:00:05Z")
        self.assertEqual(second["outcome"], "duplicate")
        self.assertEqual(second["fingerprint"], first["fingerprint"])

    def test_observed_at_change_is_still_same_slice(self):
        # observed_at 是时钟信封字段，不是读数内容：漂移走质量事实，
        # 不应把同一份切片误判成内容冲突。
        mission, data = heat_mission()
        sid = data["subject_id"]
        mission.ingest(sid, data["slices"][0], data["base_time"])
        again = copy.deepcopy(data["slices"][0])
        again["observed_at"] = "2026-09-10T08:00:02Z"
        result = mission.ingest(sid, again, "2026-09-10T08:00:05Z")
        self.assertEqual(result["outcome"], "duplicate")


class ContentConflictTest(unittest.TestCase):
    def _conflict_after_first(self, mutate):
        mission, data = heat_mission()
        sid = data["subject_id"]
        original = copy.deepcopy(data["slices"][0])  # H-TH-1, 37.4℃
        mission.ingest(sid, original, data["base_time"])
        rolled_back = copy.deepcopy(original)
        mutate(rolled_back)
        result = mission.ingest(sid, rolled_back, "2026-09-10T08:00:09Z")
        return mission, data, original, rolled_back, result

    def test_change_of_each_identity_field_is_conflict(self):
        cases = [
            ("读数", lambda s: s["samples"][0].__setitem__("v", 42.0), "samples"),
            ("采样时间", lambda s: s.__setitem__(
                "sampled_at", "2026-09-10T08:00:05Z"), "sampled_at"),
            ("序号", lambda s: s.__setitem__("seq", 101), "seq"),
            ("设备", lambda s: s.__setitem__("device_id", "OX-1"), "device_id"),
        ]
        for label, mutate, field in cases:
            mission, data, original, rolled, result = self._conflict_after_first(mutate)
            self.assertEqual(result["outcome"], "conflict", label)
            self.assertEqual(
                result["rejection"]["code"], "SLICE_CONTENT_CONFLICT", label
            )
            self.assertEqual(result["rejection"]["changed_fields"], [field], label)
            self.assertNotEqual(
                result["rejection"]["stored_fingerprint"],
                result["rejection"]["incoming_fingerprint"],
                label,
            )

    def test_conflicting_reading_never_enters_window_or_advances_level(self):
        mission, data, original, rolled, result = self._conflict_after_first(
            lambda s: s["samples"][0].__setitem__("v", 42.0)
        )
        sid = data["subject_id"]
        # 定稿第一窗口（其余切片照喂）。
        feed(mission, data)
        levels = [a["level"] for a in mission.subjects[sid].timeline.levels()]
        # 与干净链路逐字节一致：42.0 的危险读数没有把任何窗口顶成高危。
        clean, _ = heat_mission("M-CLEAN")
        feed(clean, data)
        self.assertEqual(
            json.dumps(mission.subjects[sid].timeline.levels(), sort_keys=True),
            json.dumps(clean.subjects[sid].timeline.levels(), sort_keys=True),
        )
        # 首窗口仍只含原始 37.4℃（校准后 37.3），冲突读数不在样本里。
        first = mission.subjects[sid].timeline.levels()[0]
        temp_values = [
            hit["value"] for hit in first["rule_hits"]
            if hit["signal"] == "core_temp"
        ]
        self.assertTrue(temp_values)
        self.assertLess(max(temp_values), 38.0)
        self.assertEqual(levels[0], "normal")

    def test_conflict_is_hash_chained_and_does_not_create_assessment(self):
        mission, data, original, rolled, result = self._conflict_after_first(
            lambda s: s["samples"][0].__setitem__("v", 42.0)
        )
        sid = data["subject_id"]
        types = [e["type"] for e in mission.journal.entries]
        self.assertIn("SLICE_REJECTED", types)
        self.assertNotIn("ASSESSMENT", types)  # 尚未定稿，冲突不产生评估
        ok, broken = mission.journal.verify_chain()
        self.assertTrue(ok)
        # 冲突条目本身被钉进哈希链：篡改即断裂。
        conflict_entry = next(
            e for e in mission.journal.entries if e["type"] == "SLICE_REJECTED"
        )
        conflict_entry["payload"]["rejection"]["code"] = "LATE_AFTER_FINALIZED"
        ok, broken = mission.journal.verify_chain()
        self.assertFalse(ok)
        self.assertIsNotNone(broken)

    def test_conflict_after_window_finalized_still_refused(self):
        mission, data = heat_mission()
        sid = data["subject_id"]
        feed(mission, data)
        frozen = json.dumps(mission.subjects[sid].timeline.levels(), sort_keys=True)
        # 窗口全部定稿后，回滚重传一个高危读数：仍然是冲突而非迟到/接受。
        rolled = find_slice(data, "H-TH-1")
        rolled["samples"][0]["v"] = 42.0
        result = mission.ingest(sid, rolled, "2026-09-10T08:10:00Z")
        self.assertEqual(result["outcome"], "conflict")
        self.assertEqual(
            result["rejection"]["code"], "SLICE_CONTENT_CONFLICT"
        )
        self.assertEqual(
            json.dumps(mission.subjects[sid].timeline.levels(), sort_keys=True),
            frozen,
        )

    def test_conflict_consistent_around_watermark_advance(self):
        # 心跳推进水位前后，对同一回滚副本的判断必须一致。
        mission, data = heat_mission()
        sid = data["subject_id"]
        original = find_slice(data, "H-OX-1")
        mission.ingest(sid, original, data["base_time"])
        mission.heartbeat("2026-09-10T08:01:00Z")
        rolled = copy.deepcopy(original)
        rolled["samples"][0]["v"] = 70
        first = mission.ingest(sid, rolled, "2026-09-10T08:01:30Z")
        second = mission.ingest(sid, copy.deepcopy(rolled), "2026-09-10T08:02:00Z")
        self.assertEqual(first["outcome"], "conflict")
        # 回滚副本本身不会被“接受”为新身份：它的再次出现仍是冲突。
        self.assertEqual(second["outcome"], "conflict")


class RoleViewConflictTest(unittest.TestCase):
    def setUp(self):
        self.mission, self.data = heat_mission()
        self.sid = self.data["subject_id"]
        feed(self.mission, self.data)
        rolled = find_slice(self.data, "H-TH-1")
        rolled["samples"][0]["v"] = 42.0
        self.mission.ingest(self.sid, rolled, "2026-09-10T08:10:00Z")

    def test_commander_sees_only_count_and_id(self):
        view = render(self.mission, "指挥人员")
        row = view["roster"][0]
        self.assertEqual(row["slice_conflict_count"], 1)
        self.assertEqual(row["slice_conflicts"][0]["slice_id"], "H-TH-1")
        allowed = {"slice_id", "journal_seq", "at"}
        self.assertEqual(set(row["slice_conflicts"][0]), allowed)
        serialized = json.dumps(view, ensure_ascii=False)
        # 不得出现任何读数数值、指纹或差异内容。
        for forbidden in ("fingerprint", "42.0", "37.4", "changed_fields",
                          "stored_identity"):
            self.assertNotIn(forbidden, serialized)

    def test_corpsman_sees_field_category_but_no_values(self):
        view = render(self.mission, "现场卫生员", self.sid)
        conflict = view["slice_conflicts"][0]
        self.assertEqual(conflict["changed_fields"], ["校准前读数"])
        serialized = json.dumps(view, ensure_ascii=False)
        for forbidden in ("fingerprint", "42.0", "37.4", "stored_identity",
                          "incoming_identity"):
            self.assertNotIn(forbidden, serialized)

    def test_medic_sees_both_fingerprints_and_identities(self):
        view = render(self.mission, "值班军医", self.sid)
        conflict = view["detail"]["slice_conflicts"][0]
        self.assertEqual(conflict["changed_fields"], ["samples"])
        self.assertEqual(len(conflict["stored_fingerprint"]), 64)
        self.assertEqual(len(conflict["incoming_fingerprint"]), 64)
        self.assertNotEqual(
            conflict["stored_fingerprint"], conflict["incoming_fingerprint"]
        )
        self.assertEqual(
            conflict["stored_identity"]["samples"][0]["v"], 37.4
        )
        self.assertEqual(
            conflict["incoming_identity"]["samples"][0]["v"], 42.0
        )

    def test_personnel_view_carries_no_conflict_and_level_unchanged(self):
        view = render(self.mission, "任务人员", self.sid)
        self.assertEqual(
            set(view), {"view", "subject_id", "state", "instructions"}
        )
        # 冲突不推进/改变既有风险等级（干净链路同样停在 intervene）。
        self.assertEqual(
            self.mission.status(self.sid)["effective_level"], "intervene"
        )


class MergeConflictTest(unittest.TestCase):
    def test_cross_node_fingerprint_divergence_aborts_whole_merge(self):
        proto = protocol()
        data = incidents()["INC-HEAT-01"]
        sid = data["subject_id"]

        def edge(mission_id, slice_overrides=None):
            mission = Mission(
                mission_id, proto, [sid], data["base_time"],
                profiles={sid: "高温"},
            )
            slice_ref = copy.deepcopy(data["slices"][0])
            if slice_overrides:
                slice_overrides(slice_ref)
            mission.ingest(sid, slice_ref, data["base_time"])
            return mission

        edge_a = edge("M-SYNC")
        bundle_a = export_bundle(edge_a, "EDGE-A")
        edge_b = edge("M-SYNC", lambda s: s["samples"][0].__setitem__("v", 42.0))
        bundle_b = export_bundle(edge_b, "EDGE-B")

        host = edge("M-SYNC")
        before = len(host.journal.entries)
        report = merge_bundles(host, [bundle_a, bundle_b])
        # 事务式：整次汇入被拒，日志没有新增任何条目。
        self.assertTrue(report["rejected"])
        self.assertIn("指纹", report["rejected"][0]["reason"])
        self.assertFalse(report["merged"])
        self.assertEqual(len(host.journal.entries), before)
        # 主节点身份表保持原样：回滚副本随后直连仍被判冲突。
        rolled = find_slice(data, "H-TH-1")
        rolled["samples"][0]["v"] = 42.0
        result = host.ingest(sid, rolled, "2026-09-10T08:05:00Z")
        self.assertEqual(result["outcome"], "conflict")

    def test_identical_fingerprint_merges_once_then_dedups(self):
        # 同内容跨节点重放：接受事实与评估都应幂等，不产生冲突。
        proto = protocol()
        data = incidents()["INC-HEAT-01"]
        sid = data["subject_id"]
        edge_a = Mission("M-SYNC2", proto, [sid], data["base_time"],
                         profiles={sid: "高温"})
        feed(edge_a, data)
        bundle = export_bundle(edge_a, "EDGE-A")
        host = Mission("M-SYNC2", proto, [sid], data["base_time"],
                       profiles={sid: "高温"})
        first = merge_bundles(host, [bundle])
        self.assertFalse(host.conflicts())
        self.assertTrue(
            any(item["type"] == "SLICE_ACCEPTED" for item in first["merged"])
        )
        second = merge_bundles(host, [bundle])
        self.assertFalse(second["merged"])
        self.assertTrue(second["duplicates"])
        self.assertEqual(
            [a["level"] for a in host.subjects[sid].timeline.levels()],
            ["normal", "review", "alert", "intervene"],
        )

    def test_pending_unfinalized_slices_merge_and_finalize_on_host(self):
        # 边缘离线期间只缓存了切片、从未心跳（没有 ASSESSMENT）。
        # 合并后主节点必须靠这些切片自行定稿，样本一个都不能丢。
        proto = protocol()
        data = incidents()["INC-HEAT-01"]
        sid = data["subject_id"]
        edge = Mission("M-PEND", proto, [sid], data["base_time"],
                       profiles={sid: "高温"})
        base = parse_ts(data["base_time"])
        for index, slice_ref in enumerate(data["slices"]):
            edge.ingest(sid, slice_ref, base - 1 + index * 0.001)
        # 无心跳：一个评估都没有，只有切片接受事实。
        self.assertFalse(
            [e for e in edge.journal.entries if e["type"] == "ASSESSMENT"]
        )
        bundle = export_bundle(edge, "EDGE-OFFLINE")
        host = Mission("M-PEND", proto, [sid], data["base_time"],
                       profiles={sid: "高温"})
        report = merge_bundles(host, [bundle])
        self.assertFalse(report["rejected"])
        self.assertTrue(
            any(item["type"] == "SLICE_ACCEPTED" for item in report["merged"])
        )
        # 主节点心跳定稿：结果与正常链路一致。
        max_sampled = max(parse_ts(s["sampled_at"]) for s in data["slices"])
        host.heartbeat(max_sampled + proto.window_seconds
                       + proto.allowed_lateness_seconds + 1)
        clean = Mission("M-PEND-CLEAN", proto, [sid], data["base_time"],
                        profiles={sid: "高温"})
        feed(clean, data)
        self.assertEqual(
            json.dumps(host.subjects[sid].timeline.levels(), sort_keys=True),
            json.dumps(clean.subjects[sid].timeline.levels(), sort_keys=True),
        )
        # 合并后同样的包再到必须幂等。
        again = merge_bundles(host, [bundle])
        self.assertFalse(again["merged"])
        self.assertTrue(again["duplicates"])


class RestartConflictTest(unittest.TestCase):
    def test_restored_mission_judges_rollback_identically_and_keeps_finalizing(self):
        proto = protocol()
        data = incidents()["INC-HEAT-01"]
        sid = data["subject_id"]

        # 停机前：只喂前两个窗口（不定稿），模拟任务进行中重启。
        live = Mission("M-RESTART", proto, [sid], data["base_time"],
                       profiles={sid: "高温"})
        base = parse_ts(data["base_time"])
        early = [s for s in data["slices"]
                 if parse_ts(s["sampled_at"]) < base + 120]
        for index, slice_ref in enumerate(early):
            live.ingest(sid, slice_ref, base - 1 + index * 0.001)
        exported = live.journal.export()

        restored = Mission.restore(proto, exported)
        # 重启后回滚副本到达：身份表已从日志重建，仍判冲突。
        rolled = find_slice(data, "H-TH-1")
        rolled["samples"][0]["v"] = 42.0
        result = restored.ingest(sid, rolled, "2026-09-10T08:03:30Z")
        self.assertEqual(result["outcome"], "conflict")

        # 续传剩余切片并用心跳定稿：结果与从未重启的链路逐字节一致。
        late = [s for s in data["slices"]
                if parse_ts(s["sampled_at"]) >= base + 120]
        for index, slice_ref in enumerate(late):
            restored.ingest(sid, slice_ref, base + 200 + index * 0.001)
        max_sampled = max(parse_ts(s["sampled_at"]) for s in data["slices"])
        restored.heartbeat(
            max_sampled + proto.window_seconds
            + proto.allowed_lateness_seconds + 1
        )
        clean = Mission("M-CLEAN2", proto, [sid], data["base_time"],
                        profiles={sid: "高温"})
        feed(clean, data)
        self.assertEqual(
            json.dumps(restored.subjects[sid].timeline.levels(), sort_keys=True),
            json.dumps(clean.subjects[sid].timeline.levels(), sort_keys=True),
        )
        # 冲突记录与哈希链在恢复后同样完整。
        self.assertEqual(len(restored.conflicts()), 1)
        ok, _ = restored.journal.verify_chain()
        self.assertTrue(ok)

    def test_restore_after_full_run_keeps_conflict_history(self):
        proto = protocol()
        mission, data = heat_mission("M-RESTART2", proto)
        sid = data["subject_id"]
        feed(mission, data)
        rolled = find_slice(data, "H-TH-1")
        rolled["samples"][0]["v"] = 42.0
        mission.ingest(sid, rolled, "2026-09-10T08:10:00Z")

        restored = Mission.restore(proto, mission.journal.export())
        self.assertEqual(len(restored.conflicts()), 1)
        result = restored.ingest(sid, copy.deepcopy(rolled), "2026-09-10T08:11:00Z")
        self.assertEqual(result["outcome"], "conflict")
        self.assertEqual(
            [a["level"] for a in restored.subjects[sid].timeline.levels()],
            ["normal", "review", "alert", "intervene"],
        )

    def test_restore_rejects_tampered_and_cross_protocol_journal(self):
        mission, data = heat_mission("M-RESTART3")
        feed(mission, data)
        exported = mission.journal.export()
        exported["entries"][-1]["payload"] = {"tampered": True}
        with self.assertRaises(MissionError):
            Mission.restore(protocol(), exported)

        # 换一份不同哈希的冻结协议来恢复，必须被守门拒绝。
        with open(os.path.join(FIXTURES, "protocol.json"), encoding="utf-8") as h:
            proto_data = json.load(h)
        proto_data["version"] = "2026.10-restart-guard"
        foreign = FrozenProtocol(proto_data)
        mission2, _ = heat_mission("M-RESTART3", protocol())
        feed(mission2, data)
        with self.assertRaises(MissionError):
            Mission.restore(foreign, mission2.journal.export())


if __name__ == "__main__":
    unittest.main()
