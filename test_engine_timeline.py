"""评分引擎与离线时间线的确定性测试。"""

import copy
import json
import os
import random
import unittest

from engine import evaluate_window
from protocol import FrozenProtocol
from timeutil import parse_ts
from timeline import Timeline

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def load_protocol():
    return FrozenProtocol.load(os.path.join(FIXTURES, "protocol.json"))


def load_incidents():
    with open(os.path.join(FIXTURES, "incidents.json"), encoding="utf-8") as handle:
        return json.load(handle)["incidents"]


def replay(protocol, incident, required_signals, shuffle_seed):
    """把事件切片按随机乱序（含重复）送入，返回定稿评估序列。

    模拟离线缓冲：所有切片先在设备本地缓存，回连后在一个紧凑送达窗口内
    乱序、重复投递，定稿统一由随后的处理时间心跳驱动。
    """
    timeline = Timeline(incident["subject_id"], protocol, required_signals)
    slices = incident["slices"]
    stream = list(slices) + list(slices)  # 全量重复一遍
    random.Random(shuffle_seed).shuffle(stream)
    base = parse_ts(incident["base_time"])
    max_sampled = max(parse_ts(s["sampled_at"]) for s in slices)
    # 送达时刻全部早于第一窗口可能定稿的时刻，避免投递顺序影响内容。
    for index, slice_ref in enumerate(stream):
        timeline.ingest(slice_ref, base - 1 + index * 0.001)
    # 心跳推进水位线，定稿全部窗口。
    timeline.advance(max_sampled + protocol.allowed_lateness_seconds
                     + protocol.window_seconds + 1)
    return timeline


class EngineTest(unittest.TestCase):
    def setUp(self):
        self.protocol = load_protocol()

    def test_calibration_is_applied_and_cited(self):
        value, cal_id = self.protocol.calibrate("TH-1", "core_temp", 39.0)
        self.assertAlmostEqual(value, 38.9)  # 冻结偏移 -0.1
        self.assertEqual(cal_id, "cal-TH1-2026-09")

    def test_unknown_device_and_signal_are_rejected_not_guessed(self):
        with self.assertRaises(ValueError):
            self.protocol.calibrate("NOPE", "spo2", 90)
        with self.assertRaises(ValueError):
            self.protocol.calibrate("TH-1", "spo2", 90)

    def test_missing_sensor_lowers_confidence_and_clamps_intervene(self):
        # 综合剖面缺少脑电；即便体温到干预档，置信度不足也只能现场复核。
        window = {
            "window_start": 0,
            "samples": {
                "spo2": [{"v": 98, "device_id": "OX-1", "cal_id": "c"}],
                "core_temp": [{"v": 40.5, "device_id": "TH-1", "cal_id": "c"}],
            },
            "quality": {"drifts": [], "seq_gaps": []},
            "required_signals": ["spo2", "core_temp", "eeg"],
        }
        result = evaluate_window(window, self.protocol)
        self.assertEqual(result["raw_level"], "intervene")
        self.assertEqual(result["level"], "review")
        self.assertTrue(any(f["code"] == "MISSING_SENSOR"
                            and f["signal"] == "eeg"
                            for f in result["quality_flags"]))
        self.assertEqual(result["actions"], ["现场复核"])
        self.assertTrue(result["downgrades"])

    def test_contradiction_between_devices_never_picks_a_winner(self):
        window = {
            "window_start": 0,
            "samples": {
                "spo2": [
                    {"v": 80, "device_id": "OX-1", "cal_id": "c1"},
                    {"v": 99, "device_id": "OX-2", "cal_id": "c2"},
                ],
                "core_temp": [{"v": 37.0, "device_id": "TH-1", "cal_id": "c"}],
            },
            "quality": {"drifts": [], "seq_gaps": []},
            "required_signals": ["spo2", "core_temp"],
        }
        result = evaluate_window(window, self.protocol)
        contradictions = [f for f in result["quality_flags"]
                          if f["code"] == "CONTRADICTION"]
        self.assertEqual(len(contradictions), 1)
        self.assertEqual(contradictions[0]["devices"], ["OX-1", "OX-2"])
        self.assertLess(result["confidence"], 1.0)

    def test_assessment_is_pure_data_and_deterministic(self):
        window = {
            "window_start": 100,
            "samples": {"spo2": [{"v": 85, "device_id": "OX-1", "cal_id": "c"}]},
            "quality": {"drifts": [], "seq_gaps": []},
            "required_signals": ["spo2"],
        }
        first = evaluate_window(copy.deepcopy(window), self.protocol)
        second = evaluate_window(copy.deepcopy(window), self.protocol)
        self.assertEqual(first, second)
        json.dumps(first, ensure_ascii=False)  # 可序列化

    def test_every_band_hit_cites_rule_threshold_and_calibration(self):
        window = {
            "window_start": 0,
            "samples": {"core_temp": [{"v": 39.3, "device_id": "TH-1",
                                       "cal_id": "cal-TH1-2026-09"}]},
            "quality": {"drifts": [], "seq_gaps": []},
            "required_signals": ["core_temp"],
        }
        result = evaluate_window(window, self.protocol)
        hit = next(h for h in result["rule_hits"] if h["rule_id"] == "R-TEMP-HIGH")
        self.assertEqual(hit["band"], "alert")
        self.assertEqual(hit["bands"]["alert"], 39.0)
        self.assertEqual(hit["cal_ids"], ["cal-TH1-2026-09"])


class TimelineTest(unittest.TestCase):
    def setUp(self):
        self.protocol = load_protocol()
        self.incidents = {i["incident_id"]: i for i in load_incidents()}

    def test_replay_shuffled_and_duplicated_is_consistent(self):
        cases = [
            ("INC-HEAT-01", self.protocol.profiles["高温"]),
            ("INC-ALT-01", self.protocol.profiles["高原"]),
        ]
        for incident_id, required in cases:
            incident = self.incidents[incident_id]
            runs = [
                replay(self.protocol, incident, required, seed)
                for seed in (1, 7, 42)
            ]
            baselines = [json.dumps(r.levels(), sort_keys=True, ensure_ascii=False)
                         for r in runs]
            self.assertEqual(len(set(baselines)), 1, f"{incident_id} 回放不一致")
            # 重复切片不产生重复窗口。
            starts = [e["input_window"]["start"] for e in runs[0].levels()]
            self.assertEqual(starts, sorted(starts))
            self.assertEqual(len(starts), len(set(starts)))

    def test_escalation_sequence_for_heat_incident(self):
        incident = self.incidents["INC-HEAT-01"]
        timeline = replay(self.protocol, incident,
                          self.protocol.profiles["高温"], 3)
        levels = [e["level"] for e in timeline.levels()]
        self.assertEqual(levels, ["normal", "review", "alert", "intervene"])
        self.assertEqual(
            timeline.levels()[-1]["actions"],
            ["现场复核", "降温补水", "转运"],
        )

    def test_escalation_sequence_for_altitude_incident(self):
        incident = self.incidents["INC-ALT-01"]
        timeline = replay(self.protocol, incident,
                          self.protocol.profiles["高原"], 3)
        levels = [e["level"] for e in timeline.levels()]
        self.assertEqual(levels, ["normal", "review", "alert", "intervene"])
        # 脑电惊厥信号必须在干预档留下规则依据。
        last = timeline.levels()[-1]
        self.assertIn("R-EEG-SEIZURE",
                      [h["rule_id"] for h in last["rule_hits"] if h["band"]])

    def test_finalization_latency_is_bounded(self):
        incident = self.incidents["INC-HEAT-01"]
        timeline = Timeline(incident["subject_id"], self.protocol,
                            self.protocol.profiles["高温"])
        base = parse_ts(incident["base_time"])
        max_sampled = max(parse_ts(s["sampled_at"]) for s in incident["slices"])
        # 切片按真实时刻零延迟到达，心跳按发射周期推进。
        for slice_ref in incident["slices"]:
            timeline.ingest(slice_ref, parse_ts(slice_ref["sampled_at"]))
        tick = self.protocol.emit_period_seconds
        heartbeat = base + tick
        deadline = (max_sampled + self.protocol.window_seconds
                    + self.protocol.allowed_lateness_seconds + tick)
        while heartbeat <= deadline:
            timeline.advance(heartbeat)
            heartbeat += tick
        self.assertEqual(len(timeline.finalized), 4)
        # 延迟上界 = 允许迟到 + 一个发射周期（水位线只在心跳时推进）。
        bound = (self.protocol.allowed_lateness_seconds
                 + self.protocol.emit_period_seconds)
        for record in timeline.finalized:
            window_end = record["assessment"]["input_window"]["end"]
            self.assertLessEqual(record["emitted_at"] - window_end, bound)

    def test_late_slice_after_finalization_is_logged_and_never_rewrites(self):
        incident = self.incidents["INC-HEAT-01"]
        slices = incident["slices"]
        timeline = Timeline(incident["subject_id"], self.protocol,
                            self.protocol.profiles["高温"])
        first_sampled = parse_ts(slices[0]["sampled_at"])
        timeline.ingest(slices[0], first_sampled)
        # 水位线越过第一窗口后定稿。
        timeline.advance(first_sampled + self.protocol.window_seconds
                         + self.protocol.allowed_lateness_seconds + 1)
        frozen = json.dumps(timeline.levels()[0], sort_keys=True)
        # 同窗口一个迟到的切片。
        late = copy.deepcopy(slices[1])
        late["slice_id"] = "LATE-1"
        late["seq"] = 99
        result = timeline.ingest(
            late, first_sampled + self.protocol.window_seconds
            + self.protocol.allowed_lateness_seconds + 5
        )
        self.assertEqual(result["outcome"], "late")
        self.assertEqual(json.dumps(timeline.levels()[0], sort_keys=True), frozen)
        self.assertEqual(timeline.rejected[-1]["code"], "LATE_AFTER_FINALIZED")

    def test_clock_drift_and_seq_gap_are_quality_facts(self):
        incident = self.incidents["INC-HEAT-01"]
        by_id = {s["slice_id"]: s for s in incident["slices"]}
        timeline = Timeline(incident["subject_id"], self.protocol,
                            self.protocol.profiles["高温"])
        first = copy.deepcopy(by_id["H-TH-1"])   # TH-1 seq=1
        gap = copy.deepcopy(by_id["H-TH-3"])     # TH-1 seq=3，缺 seq=2
        drift = copy.deepcopy(by_id["H-TH-5"])   # TH-1 seq=5，缺 seq=4，加漂移
        drift["observed_at"] = "2026-09-10T08:02:09Z"  # 偏差 9s > 5s 限值
        at = parse_ts(first["sampled_at"])
        timeline.ingest(first, at)
        timeline.ingest(gap, parse_ts(gap["sampled_at"]))
        timeline.ingest(drift, parse_ts(drift["sampled_at"]))
        timeline.advance(parse_ts(drift["sampled_at"])
                         + self.protocol.allowed_lateness_seconds
                         + self.protocol.window_seconds + 1)
        flags = [
            f["code"]
            for assessment in timeline.levels()
            for f in assessment["quality_flags"]
        ]
        self.assertIn("CLOCK_DRIFT", flags)
        self.assertIn("SEQ_GAP", flags)

    def test_duplicate_slice_id_is_idempotent(self):
        incident = self.incidents["INC-HEAT-01"]
        timeline = Timeline(incident["subject_id"], self.protocol,
                            self.protocol.profiles["高温"])
        slice_ref = incident["slices"][0]
        at = parse_ts(slice_ref["sampled_at"])
        first = timeline.ingest(slice_ref, at)
        second = timeline.ingest(copy.deepcopy(slice_ref), at + 1)
        self.assertEqual(first["outcome"], "accepted")
        self.assertEqual(second["outcome"], "duplicate")

    def test_same_seq_different_slice_is_conflict(self):
        incident = self.incidents["INC-HEAT-01"]
        timeline = Timeline(incident["subject_id"], self.protocol,
                            self.protocol.profiles["高温"])
        first = copy.deepcopy(incident["slices"][0])  # TH-1 seq=1
        clash = copy.deepcopy(first)
        clash["slice_id"] = "H-TH-1-B"
        at = parse_ts(first["sampled_at"])
        self.assertEqual(timeline.ingest(first, at)["outcome"], "accepted")
        self.assertEqual(timeline.ingest(clash, at + 1)["outcome"], "rejected")
        self.assertEqual(timeline.rejected[-1]["code"], "SEQ_CONFLICT")

    def test_unknown_device_is_refused(self):
        timeline = Timeline("P-01", self.protocol,
                            self.protocol.profiles["高温"])
        bad = {
            "slice_id": "X", "device_id": "EVIL", "seq": 1,
            "sampled_at": "2026-09-10T08:00:00Z",
            "observed_at": "2026-09-10T08:00:00Z",
            "samples": [{"signal": "spo2", "t": "2026-09-10T08:00:00Z", "v": 80}],
        }
        result = timeline.ingest(bad, parse_ts("2026-09-10T08:00:00Z"))
        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(result["rejection"]["code"], "UNKNOWN_DEVICE")


class SliceConflictTest(unittest.TestCase):
    """同 slice_id 携带不同内容（疑似固件回滚）的冲突判定。"""

    def setUp(self):
        self.protocol = load_protocol()
        self.incident = {i["incident_id"]: i
                         for i in load_incidents()}["INC-HEAT-01"]
        self.required = self.protocol.profiles["高温"]

    def _timeline(self):
        return Timeline(self.incident["subject_id"], self.protocol, self.required)

    def _first_slice(self):
        return copy.deepcopy(self.incident["slices"][0])  # H-TH-1，TH-1 seq=1

    def test_identical_redelivery_stays_idempotent(self):
        timeline = self._timeline()
        slice_ref = self._first_slice()
        at = parse_ts(slice_ref["sampled_at"])
        self.assertEqual(timeline.ingest(slice_ref, at)["outcome"], "accepted")
        # 逐字节语义一致的重投（含时间格式等价写法）仍然幂等。
        again = copy.deepcopy(slice_ref)
        again["sampled_at"] = "2026-09-10T08:00:00+00:00"
        result = timeline.ingest(again, at + 1)
        self.assertEqual(result["outcome"], "duplicate")
        self.assertEqual(timeline.rejected, [])

    def test_changed_reading_is_conflict_and_never_enters_window(self):
        timeline = self._timeline()
        slice_ref = self._first_slice()
        at = parse_ts(slice_ref["sampled_at"])
        timeline.ingest(slice_ref, at)
        clash = copy.deepcopy(slice_ref)
        clash["samples"][0]["v"] = 45.0  # 固件回滚后同编号携带的异常读数
        result = timeline.ingest(clash, at + 1)
        self.assertEqual(result["outcome"], "rejected")
        rejection = result["rejection"]
        self.assertEqual(rejection["code"], "SLICE_CONFLICT")
        self.assertEqual(rejection["diff"], ["samples"])
        self.assertNotEqual(rejection["expected_hash"], rejection["got_hash"])
        self.assertEqual(rejection["established"]["samples"][0]["v"], 37.4)
        self.assertEqual(rejection["received"]["samples"][0]["v"], 45.0)
        # 冲突切片不进入窗口：定稿后窗口里只有已接收版本的读数。
        timeline.advance(at + self.protocol.window_seconds
                         + self.protocol.allowed_lateness_seconds + 1)
        self.assertEqual(len(timeline.levels()), 1)
        hit = next(h for h in timeline.levels()[0]["rule_hits"]
                   if h["rule_id"] == "R-TEMP-HIGH")
        self.assertEqual(hit["value"], 37.3)  # 37.4 经冻结校准 -0.1
        self.assertEqual(hit["sample_count"], 1)
        self.assertEqual(timeline.levels()[0]["level"], "normal")

    def test_device_seq_and_sampled_at_changes_are_conflicts(self):
        cases = [
            ("device_id", lambda s: s.update(device_id="OX-1"), ["device_id"]),
            ("seq", lambda s: s.update(seq=9), ["seq"]),
            ("sampled_at",
             lambda s: s.update(sampled_at="2026-09-10T08:00:15Z"),
             ["sampled_at"]),
        ]
        for field, mutate, expected_diff in cases:
            with self.subTest(field=field):
                timeline = self._timeline()
                slice_ref = self._first_slice()
                at = parse_ts(slice_ref["sampled_at"])
                timeline.ingest(slice_ref, at)
                clash = copy.deepcopy(slice_ref)
                mutate(clash)
                result = timeline.ingest(clash, at + 1)
                self.assertEqual(result["outcome"], "rejected")
                self.assertEqual(result["rejection"]["code"], "SLICE_CONFLICT")
                self.assertEqual(result["rejection"]["diff"], expected_diff)

    def test_conflict_after_finalization_is_conflict_not_late(self):
        timeline = self._timeline()
        slice_ref = self._first_slice()
        at = parse_ts(slice_ref["sampled_at"])
        timeline.ingest(slice_ref, at)
        finalize_at = (at + self.protocol.window_seconds
                       + self.protocol.allowed_lateness_seconds + 1)
        timeline.advance(finalize_at)
        self.assertEqual(len(timeline.finalized), 1)
        frozen = json.dumps(timeline.levels(), sort_keys=True, ensure_ascii=False)
        # 窗口已定稿：同编号不同读数仍判冲突，而不是按迟到处理。
        clash = copy.deepcopy(slice_ref)
        clash["samples"][0]["v"] = 45.0
        result = timeline.ingest(clash, finalize_at + 10)
        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(result["rejection"]["code"], "SLICE_CONFLICT")
        # 内容一致的重投在定稿后仍是幂等重复，不是迟到。
        dup = timeline.ingest(copy.deepcopy(slice_ref), finalize_at + 20)
        self.assertEqual(dup["outcome"], "duplicate")
        self.assertEqual(
            json.dumps(timeline.levels(), sort_keys=True, ensure_ascii=False),
            frozen,
        )

    def test_conflict_does_not_advance_risk_level(self):
        timeline = self._timeline()
        slices = self.incident["slices"]
        base = parse_ts(self.incident["base_time"])
        for index, slice_ref in enumerate(slices):
            timeline.ingest(slice_ref, base - 1 + index * 0.001)
        max_sampled = max(parse_ts(s["sampled_at"]) for s in slices)
        timeline.advance(max_sampled + self.protocol.allowed_lateness_seconds
                         + self.protocol.window_seconds + 1)
        self.assertEqual(
            [e["level"] for e in timeline.levels()],
            ["normal", "review", "alert", "intervene"],
        )
        frozen = json.dumps(timeline.levels(), sort_keys=True, ensure_ascii=False)
        # 冲突切片携带致命读数，也不得推进风险等级或重写任何窗口。
        clash = copy.deepcopy(slices[0])
        clash["samples"][0]["v"] = 99.9
        result = timeline.ingest(clash, max_sampled + 500)
        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(result["finalized"], [])
        self.assertEqual(
            json.dumps(timeline.levels(), sort_keys=True, ensure_ascii=False),
            frozen,
        )

    def test_conflict_judgment_is_deterministic_on_redelivery(self):
        timeline = self._timeline()
        slice_ref = self._first_slice()
        at = parse_ts(slice_ref["sampled_at"])
        timeline.ingest(slice_ref, at)
        clash = copy.deepcopy(slice_ref)
        clash["samples"][0]["v"] = 41.0
        first = timeline.ingest(clash, at + 1)
        second = timeline.ingest(copy.deepcopy(clash), at + 2)
        for result in (first, second):
            self.assertEqual(result["outcome"], "rejected")
            self.assertEqual(result["rejection"]["code"], "SLICE_CONFLICT")
        conflicts = [r for r in timeline.rejected if r["code"] == "SLICE_CONFLICT"]
        self.assertEqual(len(conflicts), 2)
        self.assertEqual(conflicts[0]["got_hash"], conflicts[1]["got_hash"])

    def test_full_replay_after_device_restart_is_consistent(self):
        # 设备重启后全量重传：原切片幂等，冲突切片仍判冲突，时间线不变。
        timeline = self._timeline()
        slices = self.incident["slices"]
        base = parse_ts(self.incident["base_time"])
        for index, slice_ref in enumerate(slices):
            timeline.ingest(slice_ref, base - 1 + index * 0.001)
        max_sampled = max(parse_ts(s["sampled_at"]) for s in slices)
        timeline.advance(max_sampled + self.protocol.allowed_lateness_seconds
                         + self.protocol.window_seconds + 1)
        frozen = json.dumps(timeline.levels(), sort_keys=True, ensure_ascii=False)
        clash = copy.deepcopy(slices[2])
        clash["samples"][0]["v"] = 44.0
        timeline.ingest(clash, max_sampled + 400)
        outcomes = [
            timeline.ingest(copy.deepcopy(s), max_sampled + 500 + i)["outcome"]
            for i, s in enumerate(slices)
        ]
        self.assertEqual(set(outcomes), {"duplicate"})
        again = timeline.ingest(copy.deepcopy(clash), max_sampled + 600)
        self.assertEqual(again["rejection"]["code"], "SLICE_CONFLICT")
        self.assertEqual(
            json.dumps(timeline.levels(), sort_keys=True, ensure_ascii=False),
            frozen,
        )

    def test_uncalibrated_signal_rejected_without_side_effects(self):
        # 校准规则回归：设备未登记的信号整切片拒绝，不留副作用。
        timeline = self._timeline()
        bad = {
            "slice_id": "U-1", "device_id": "TH-1", "seq": 1,
            "sampled_at": "2026-09-10T08:00:00Z",
            "observed_at": "2026-09-10T08:00:00Z",
            "samples": [{"signal": "spo2", "t": "2026-09-10T08:00:00Z", "v": 90}],
        }
        result = timeline.ingest(bad, parse_ts("2026-09-10T08:00:00Z"))
        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(result["rejection"]["code"], "UNCALIBRATED_SIGNAL")
        self.assertEqual(timeline.levels(), [])


if __name__ == "__main__":
    unittest.main()
