"""任务处置、军医覆盖、角色视图、不可篡改日志与回连合并测试。"""

import copy
import json
import os
import random
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


def build_heat_mission(mission_id="M-1", proto=None):
    proto = proto or protocol()
    data = incidents()["INC-HEAT-01"]
    mission = Mission(
        mission_id, proto, [data["subject_id"]], data["base_time"],
        profiles={data["subject_id"]: "高温"},
    )
    return mission, data


def feed_to_end(mission, data, received_base=None):
    base = received_base or parse_ts(data["base_time"])
    max_sampled = max(parse_ts(s["sampled_at"]) for s in data["slices"])
    for index, slice_ref in enumerate(data["slices"]):
        mission.ingest(data["subject_id"], slice_ref, base - 1 + index * 0.001)
    mission.heartbeat(max_sampled + mission.protocol.window_seconds
                      + mission.protocol.allowed_lateness_seconds + 1)


class MissionWorkflowTest(unittest.TestCase):
    def test_actions_escalate_and_each_carries_basis(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        status = mission.status(data["subject_id"])
        self.assertEqual(status["state"], "干预中")
        names = [a["action"] for a in status["actions"]]
        self.assertEqual(names, ["现场复核", "降温补水", "转运"])
        for action in status["actions"]:
            self.assertEqual(
                action["basis"]["protocol_version"], mission.protocol.version
            )
            self.assertEqual(
                action["basis"]["protocol_hash"], mission.protocol.protocol_hash
            )
            self.assertIsNotNone(action["basis"]["window_end"])
            self.assertTrue(action["basis"]["rule_ids"])

    def test_explanation_cites_calibration_rule_and_window(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        explanation = mission.explanation(data["subject_id"])
        latest = explanation["latest_assessment"]
        temp_hit = next(
            h for h in latest["rule_hits"] if h["rule_id"] == "R-TEMP-HIGH"
        )
        self.assertEqual(temp_hit["band"], "intervene")
        self.assertEqual(temp_hit["cal_ids"], ["cal-TH1-2026-09"])
        self.assertGreaterEqual(temp_hit["value"], 40.0)
        self.assertEqual(latest["input_window"]["window_seconds"], 60)

    def test_medic_can_override_but_reason_is_mandatory(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        with self.assertRaises(MissionError):
            mission.override(data["subject_id"], "DOC-7", data["base_time"],
                             "normal", "   ")
        entry = mission.override(
            data["subject_id"], "DOC-7", "2026-09-10T08:05:00Z",
            "normal", "现场复测体温 37.2℃，判定装备误报，解除转运",
        )
        status = mission.status(data["subject_id"])
        self.assertEqual(status["effective_level"], "normal")
        self.assertEqual(status["level_source"], "override")
        self.assertTrue(entry["payload"]["reason"].startswith("现场复测"))
        # 覆盖在后续窗口/再次喂数后仍然有效，直到军医解除。
        mission.heartbeat("2026-09-10T08:10:00Z")
        self.assertEqual(
            mission.status(data["subject_id"])["level_source"], "override"
        )

    def test_transport_requires_medic(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        with self.assertRaises(MissionError):
            mission.record_action(
                data["subject_id"], "转运", "MEDIC-A1",
                "2026-09-10T08:04:00Z", "现场卫生员",
            )
        entry = mission.record_action(
            data["subject_id"], "转运", "DOC-7",
            "2026-09-10T08:04:00Z", "值班军医", "立即后送",
        )
        self.assertEqual(entry["type"], "ACTION")

    def test_corpsman_field_check_is_allowed(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        entry = mission.record_action(
            data["subject_id"], "现场复核", "MEDIC-A1",
            "2026-09-10T08:03:30Z", "现场卫生员", "已到身边复测",
        )
        self.assertEqual(entry["payload"]["status"], "acknowledged")

    def test_resolve_clears_actions(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        mission.resolve(data["subject_id"], "DOC-7",
                        "2026-09-10T08:20:00Z", "体温恢复，留观")
        status = mission.status(data["subject_id"])
        self.assertEqual(status["state"], "已解除")
        self.assertTrue(all(a["status"] == "cleared"
                            for a in status["actions"]))

    def test_closed_mission_rejects_writes(self):
        mission, data = build_heat_mission()
        mission.close("2026-09-10T09:00:00Z")
        with self.assertRaises(MissionError):
            mission.ingest(data["subject_id"], data["slices"][0],
                           "2026-09-10T09:01:00Z")


class ImmutabilityTest(unittest.TestCase):
    def test_threshold_update_never_rewrites_old_judgement(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        before = json.dumps(
            [e for e in mission.journal.entries if e["type"] == "ASSESSMENT"],
            sort_keys=True, ensure_ascii=False,
        )
        # 任务后“下发新版阈值”：只允许在新任务上使用，旧任务的评估原样保留。
        with open(os.path.join(FIXTURES, "protocol.json"), encoding="utf-8") as h:
            new_data = json.load(h)
        new_data["version"] = "2026.10-revised-v1"
        for rule in new_data["rules"]:
            if rule["id"] == "R-TEMP-HIGH":
                rule["bands"] = {"review": 39.0, "alert": 40.0, "intervene": 41.5}
        new_proto = FrozenProtocol(new_data)
        mission2, _ = build_heat_mission("M-2", new_proto)
        feed_to_end(mission2, data)
        after = json.dumps(
            [e for e in mission.journal.entries if e["type"] == "ASSESSMENT"],
            sort_keys=True, ensure_ascii=False,
        )
        self.assertEqual(before, after)
        self.assertNotEqual(mission.protocol_hash if hasattr(mission, "protocol_hash")
                            else mission.protocol.protocol_hash,
                            mission2.protocol.protocol_hash)
        # 旧任务干预档仍然成立，新任务因阈值更高停在更低等级。
        self.assertEqual(
            mission.status(data["subject_id"])["effective_level"], "intervene"
        )
        self.assertIn(
            mission2.status(data["subject_id"])["effective_level"],
            ("review", "alert"),
        )

    def test_journal_is_hash_chained_and_tamper_detected(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        ok, broken = mission.journal.verify_chain()
        self.assertTrue(ok)
        # 篡改一条干预档评估的等级，哈希链立刻断裂。
        target = next(
            e for e in mission.journal.entries
            if e["type"] == "ASSESSMENT"
            and e["payload"]["assessment"]["level"] == "intervene"
        )
        target["payload"]["assessment"]["level"] = "normal"
        ok, broken = mission.journal.verify_chain()
        self.assertFalse(ok)
        self.assertIsNotNone(broken)


class RoleViewTest(unittest.TestCase):
    def test_commander_sees_only_operational_state(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        view = render(mission, "指挥人员")
        row = view["roster"][0]
        self.assertEqual(row["state"], "干预中")
        self.assertIn("转运", row["pending_actions"])
        # 不包含任何生理读数、规则细节或完整档案字段。
        serialized = json.dumps(view, ensure_ascii=False)
        for forbidden in ("core_temp", "rule_hits", "cal_id", "40.4", "quality_flags"):
            self.assertNotIn(forbidden, serialized)

    def test_medic_sees_full_detail_personnel_sees_minimal(self):
        mission, data = build_heat_mission()
        feed_to_end(mission, data)
        medic = render(mission, "值班军医", data["subject_id"])
        self.assertIn("rule_hits", json.dumps(medic, ensure_ascii=False))
        personnel = render(mission, "任务人员", data["subject_id"])
        self.assertEqual(set(personnel.keys()),
                         {"view", "subject_id", "state", "instructions"})


class SyncMergeTest(unittest.TestCase):
    def test_offline_bundles_merge_by_device_seq_and_dedup(self):
        # 两台边缘节点各自离线负责一名人员，回连后各自按设备序列安全汇入。
        proto = protocol()
        heat = incidents()["INC-HEAT-01"]
        alt = incidents()["INC-ALT-01"]
        profiles = {heat["subject_id"]: "高温", alt["subject_id"]: "高原"}

        def fresh(mission_id):
            return Mission(
                mission_id, proto, [heat["subject_id"], alt["subject_id"]],
                heat["base_time"], profiles=profiles,
            )

        edge_a = fresh("M-JOINT")
        feed_to_end(edge_a, heat)
        bundle_a = export_bundle(edge_a, "EDGE-A")
        edge_b = fresh("M-JOINT")
        feed_to_end(edge_b, alt)
        bundle_b = export_bundle(edge_b, "EDGE-B")

        def merge_in_order(order):
            host = fresh("M-JOINT")
            shuffled = [bundle_a, bundle_b]
            random.Random(order).shuffle(shuffled)
            merge_bundles(host, shuffled)
            # 再投一次，必须幂等。
            report2 = merge_bundles(host, list(reversed(shuffled)))
            return host, report2

        hosts = [merge_in_order(seed)[0] for seed in (1, 2, 3)]
        timelines = [
            json.dumps(
                [
                    [sid, [e["assessment"]["level"]
                           for e in m.subjects[sid].timeline.finalized]]
                    for sid in sorted(m.subjects)
                ],
                ensure_ascii=False, sort_keys=True,
            )
            for m in hosts
        ]
        self.assertEqual(len(set(timelines)), 1)
        # 两名人员的时间线都已汇入。
        host = hosts[0]
        self.assertEqual(
            len(host.subjects[heat["subject_id"]].timeline.finalized), 4
        )
        self.assertEqual(
            len(host.subjects[alt["subject_id"]].timeline.finalized), 4
        )
        _host, second_report = merge_in_order(1)
        self.assertFalse(second_report["merged"])
        self.assertTrue(second_report["duplicates"])

    def test_bundle_with_other_protocol_hash_is_rejected(self):
        proto = protocol()
        heat = incidents()["INC-HEAT-01"]
        edge, data = build_heat_mission("M-SAFE", proto)
        feed_to_end(edge, data)
        bundle = export_bundle(edge, "EDGE-A")

        tampered = copy.deepcopy(bundle)
        tampered["protocol_hash"] = "x" * 64
        host, _ = build_heat_mission("M-SAFE", proto)
        report = merge_bundles(host, [tampered])
        self.assertTrue(report["rejected"])
        self.assertFalse(report["merged"])
        self.assertIn("协议哈希", report["rejected"][0]["reason"])

    def test_merged_override_and_action_are_replayed_with_reason(self):
        proto = protocol()
        heat = incidents()["INC-HEAT-01"]
        edge = Mission(
            "M-MED", proto, [heat["subject_id"]], heat["base_time"],
            profiles={heat["subject_id"]: "高温"},
        )
        feed_to_end(edge, heat)
        edge.override(heat["subject_id"], "DOC-9",
                      "2026-09-10T08:05:00Z", "review", "指挥要求边观察边后撤")
        bundle = export_bundle(edge, "EDGE-A")
        host = Mission(
            "M-MED", proto, [heat["subject_id"]], heat["base_time"],
            profiles={heat["subject_id"]: "高温"},
        )
        merge_bundles(host, [bundle])
        status = host.status(heat["subject_id"])
        self.assertEqual(status["effective_level"], "review")
        self.assertEqual(status["override"]["medic_id"], "DOC-9")
        self.assertEqual(status["override"]["reason"], "指挥要求边观察边后撤")


class SliceConflictMissionTest(unittest.TestCase):
    """切片冲突进入不可篡改日志与角色化视图，但不推进风险等级。"""

    def _mission_with_conflict(self, mission_id="M-CF"):
        mission, data = build_heat_mission(mission_id)
        base = parse_ts(data["base_time"])
        first = data["slices"][0]
        mission.ingest(data["subject_id"], first, base - 1)
        clash = copy.deepcopy(first)
        clash["samples"][0]["v"] = 45.0  # 同编号不同读数，疑似固件回滚
        result = mission.ingest(data["subject_id"], clash, base)
        return mission, data, result

    def test_conflict_is_rejected_and_journaled_with_intact_chain(self):
        mission, data, result = self._mission_with_conflict()
        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(result["rejection"]["code"], "SLICE_CONFLICT")
        entries = [e for e in mission.journal.entries
                   if e["type"] == "SLICE_REJECTED"]
        self.assertEqual(len(entries), 1)
        payload = entries[0]["payload"]["rejection"]
        self.assertEqual(payload["code"], "SLICE_CONFLICT")
        self.assertEqual(payload["diff"], ["samples"])
        self.assertEqual(payload["established"]["samples"][0]["v"], 37.4)
        self.assertEqual(payload["received"]["samples"][0]["v"], 45.0)
        self.assertNotEqual(payload["expected_hash"], payload["got_hash"])
        ok, broken = mission.journal.verify_chain()
        self.assertTrue(ok)
        self.assertIsNone(broken)

    def test_conflict_does_not_change_effective_level_or_actions(self):
        mission, data = build_heat_mission("M-CF-LV")
        feed_to_end(mission, data)
        before = mission.status(data["subject_id"])
        self.assertEqual(before["effective_level"], "intervene")
        clash = copy.deepcopy(data["slices"][-1])
        clash["samples"][0]["v"] = 99.0
        result = mission.ingest(data["subject_id"], clash,
                                parse_ts(data["base_time"]) + 1000)
        self.assertEqual(result["outcome"], "rejected")
        after = mission.status(data["subject_id"])
        self.assertEqual(after["effective_level"], "intervene")
        self.assertEqual(after["state"], before["state"])
        self.assertEqual(
            json.dumps(after["actions"], sort_keys=True, ensure_ascii=False),
            json.dumps(before["actions"], sort_keys=True, ensure_ascii=False),
        )
        self.assertEqual(len(after["slice_conflicts"]), 1)
        # 冲突不产生任何新评估。
        self.assertEqual(
            len([e for e in mission.journal.entries if e["type"] == "ASSESSMENT"]),
            4,
        )

    def test_role_views_expose_conflict_by_need_to_know(self):
        mission, data, _ = self._mission_with_conflict("M-CF-VIEW")
        sid = data["subject_id"]
        # 值班军医：完整冲突细节，含双向哈希与差异字段。
        medic = render(mission, "值班军医", sid)
        conflicts = medic["detail"]["slice_conflicts"]
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["code"], "SLICE_CONFLICT")
        self.assertEqual(conflicts[0]["diff"], ["samples"])
        # 指挥人员：只有计数，序列化视图不得泄漏读数与冲突内容。
        commander = render(mission, "指挥人员")
        self.assertEqual(commander["roster"][0]["slice_conflict_count"], 1)
        serialized = json.dumps(commander, ensure_ascii=False)
        for forbidden in ("45.0", "37.4", '"established"', '"received"',
                          "expected_hash", "core_temp"):
            self.assertNotIn(forbidden, serialized)
        # 现场卫生员：可见设备/序号/差异字段用于现场复核，不见读数。
        corpsman = render(mission, "现场卫生员", sid)
        self.assertEqual(len(corpsman["slice_conflicts"]), 1)
        self.assertEqual(corpsman["slice_conflicts"][0]["device_id"], "TH-1")
        self.assertEqual(corpsman["slice_conflicts"][0]["diff"], ["samples"])
        c_serialized = json.dumps(corpsman, ensure_ascii=False)
        for forbidden in ("45.0", "37.4", '"established"', '"received"'):
            self.assertNotIn(forbidden, c_serialized)
        # 任务人员视图保持最小知情，不含冲突信息。
        personnel = render(mission, "任务人员", sid)
        self.assertEqual(set(personnel.keys()),
                         {"view", "subject_id", "state", "instructions"})


class SliceConflictMergeTest(unittest.TestCase):
    def test_conflict_replays_through_merge_and_stays_consistent(self):
        proto = protocol()
        heat = incidents()["INC-HEAT-01"]
        sid = heat["subject_id"]
        base = parse_ts(heat["base_time"])

        edge = Mission("M-CF-SYNC", proto, [sid], heat["base_time"],
                       profiles={sid: "高温"})
        first = heat["slices"][0]
        edge.ingest(sid, first, base - 1)
        clash = copy.deepcopy(first)
        clash["samples"][0]["v"] = 45.0
        edge.ingest(sid, clash, base)
        bundle = export_bundle(edge, "EDGE-A")
        self.assertTrue(any(e["type"] == "SLICE_REJECTED"
                            for e in bundle["entries"]))

        # 回连汇入全新节点（等价于任务重建/重启后的恢复）。
        host = Mission("M-CF-SYNC", proto, [sid], heat["base_time"],
                       profiles={sid: "高温"})
        report = merge_bundles(host, [bundle])
        self.assertEqual(len(report["merged"]), 1)
        self.assertEqual(report["merged"][0]["type"], "SLICE_REJECTED")
        # 主机日志可见冲突，哈希链完整。
        self.assertEqual(len(host.slice_conflicts(sid)), 1)
        ok, _ = host.journal.verify_chain()
        self.assertTrue(ok)
        # 主机对同一冲突切片作出一致判断：仍判冲突而非接收。
        again = host.ingest(sid, copy.deepcopy(clash), base + 10)
        self.assertEqual(again["outcome"], "rejected")
        self.assertEqual(again["rejection"]["code"], "SLICE_CONFLICT")
        # 原始切片判为幂等重复（其内容已通过定稿评估体现），不被顶掉。
        dup = host.ingest(sid, copy.deepcopy(first), base + 20)
        self.assertEqual(dup["outcome"], "duplicate")
        # 合并一条 + 本机再判一条，冲突登记全程可追溯。
        self.assertEqual(len(host.slice_conflicts(sid)), 2)
        ok, _ = host.journal.verify_chain()
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
