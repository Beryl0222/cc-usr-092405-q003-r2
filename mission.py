"""任务聚合：冻结协议下的多人员监测、处置流转与军医覆盖。

状态（与 fixtures/domain.json 的参考状态一致）：
监测中 → 需复核 → 已预警 → 干预中 → 已解除。

每个建议动作在生成时就快照规则依据（规则号、阈值、校准编号、输入窗口、
模型与协议版本），因此事后任意时刻都能回答“为什么让他撤离”。
"""

from journal import Journal
from timeline import Timeline
from timeutil import parse_ts

STATE_NORMAL = "监测中"
STATE_REVIEW = "需复核"
STATE_ALERT = "已预警"
STATE_INTERVENE = "干预中"
STATE_RESOLVED = "已解除"

LEVEL_TO_STATE = {
    "normal": STATE_NORMAL,
    "review": STATE_REVIEW,
    "alert": STATE_ALERT,
    "intervene": STATE_INTERVENE,
}

ALL_ACTIONS = ("现场复核", "降温补水", "转运")
# 现场卫生员可执行的处置；转运必须由值班医军下令。
MEDIC_ONLY_ACTIONS = ("转运",)
ROLE_MEDIC = "值班军医"
ROLE_CORPSMAN = "现场卫生员"


class MissionError(ValueError):
    """处置越权、理由缺失或任务已关闭等业务拒绝。"""


class SubjectMonitor:
    def __init__(self, subject_id, protocol, required_signals=None):
        self.subject_id = subject_id
        self.timeline = Timeline(subject_id, protocol, required_signals)
        self.actions = {}        # 动作名 -> 状态记录
        self.override = None     # 最近一次军医覆盖
        self.resolved = None     # 解除记录
        self.latest_window = None

    def _basis(self, assessment):
        hits = [hit for hit in assessment["rule_hits"] if hit["band"]]
        return {
            "model_id": assessment["model_id"],
            "protocol_version": assessment["protocol_version"],
            "protocol_hash": assessment["protocol_hash"],
            "window_start": assessment["input_window"]["start"],
            "window_end": assessment["input_window"]["end"],
            "rule_ids": [hit["rule_id"] for hit in hits],
            "rule_bands": [
                {"rule_id": hit["rule_id"], "band": hit["band"],
                 "metric": hit["metric"], "value": hit["value"],
                 "threshold": hit["bands"][hit["band"]]}
                for hit in hits
            ],
            "cal_ids": sorted({cal for hit in hits for cal in hit["cal_ids"]}),
            "confidence": assessment["confidence"],
        }

    def apply_assessment(self, record):
        """消费一个已定稿窗口：快照建议动作。

        军医覆盖持续到军医本人解除（撤离/降级令不能被新窗口自动撤销），
        但覆盖理由与当时的系统等级都永久留在日志中。
        """
        assessment = record["assessment"]
        self.latest_window = assessment["input_window"]["start"]
        basis = self._basis(assessment)
        for action in assessment["actions"]:
            current = self.actions.get(action)
            if current is None or current["status"] == "cleared":
                self.actions[action] = {
                    "action": action,
                    "status": "suggested",
                    "basis": basis,
                    "history": [{
                        "status": "suggested",
                        "at": record["emitted_at"],
                        "by": "edge-engine",
                    }],
                }

    def adopt_assessment(self, assessment, emitted_at):
        """回连合并时采纳其他节点已定稿的窗口（内容不可变，按窗口去重）。"""
        start = assessment["input_window"]["start"]
        for existing in self.timeline.finalized:
            if existing["assessment"]["input_window"]["start"] == start:
                return False
        record = {"subject_id": self.subject_id, "emitted_at": emitted_at,
                  "assessment": assessment}
        self.timeline.finalized.append(record)
        self.timeline.finalized.sort(
            key=lambda item: item["assessment"]["input_window"]["start"]
        )
        self.apply_assessment(record)
        return True

    def apply_action_entry(self, payload):
        """回放处置日志条目，不再二次写日志（供合并器使用）。"""
        action = payload["action"]
        item = self.actions.get(action)
        if item is None:
            item = {"action": action, "status": "suggested",
                    "basis": payload.get("basis"), "history": []}
            self.actions[action] = item
        item["status"] = payload["status"]
        item["history"].append({
            "status": item["status"],
            "at": payload.get("at_hint"),
            "by": "replayed",
            "note": payload.get("note", ""),
        })

    def apply_override_entry(self, payload):
        self.override = {
            "medic_id": payload["medic_id"],
            "at": payload["at"],
            "forced_level": payload["forced_level"],
            "reason": payload["reason"],
            "system_level": payload["system_level"],
            "basis": payload.get("basis"),
            "persistent": False,
        }

    def apply_resolved_entry(self, payload):
        self.resolved = {
            "medic_id": payload["medic_id"],
            "at": payload["at"],
            "reason": payload.get("reason", ""),
        }
        for item in self.actions.values():
            item["status"] = "cleared"

    def effective_level(self):
        latest = self.timeline.finalized[-1]["assessment"] if self.timeline.finalized else None
        eval_level = latest["level"] if latest else "normal"
        if self.override is not None:
            return self.override["forced_level"], "override"
        return eval_level, "assessment"

    def state(self):
        if self.resolved is not None:
            return STATE_RESOLVED
        # 降温补水或转运已实施，才视为进入干预；仅完成现场复核不改变等级状态。
        if any(a["status"] == "applied" for a in self.actions.values()):
            return STATE_INTERVENE
        level, _source = self.effective_level()
        return LEVEL_TO_STATE[level]

    def status_payload(self):
        latest = self.timeline.finalized[-1]["assessment"] if self.timeline.finalized else None
        level, level_source = self.effective_level()
        return {
            "subject_id": self.subject_id,
            "state": self.state(),
            "effective_level": level,
            "level_source": level_source,
            "latest_window": (
                {"start": self.latest_window,
                 "end": self.latest_window + self.timeline.protocol.window_seconds}
                if self.latest_window is not None else None
            ),
            "confidence": latest["confidence"] if latest else None,
            "actions": [
                {
                    "action": name,
                    "status": item["status"],
                    "basis": item["basis"],
                }
                for name in ALL_ACTIONS
                if (item := self.actions.get(name)) is not None
            ],
            "override": self.override,
            "resolved": self.resolved,
        }


class Mission:
    def __init__(self, mission_id, protocol, subject_ids, started_at,
                 created_by="卫勤团队", profiles=None):
        self.mission_id = mission_id
        self.protocol = protocol
        self.started_at = float(parse_ts(started_at))
        self.closed_at = None
        # profiles: {subject_id: 剖面名}；剖面必须存在于冻结协议中。
        self.profiles = dict(profiles or {})
        self.subjects = {}
        for sid in subject_ids:
            profile_name = self.profiles.get(sid)
            if profile_name is not None and profile_name not in protocol.profiles:
                raise MissionError(f"冻结协议中不存在任务剖面: {profile_name}")
            required = (
                protocol.profiles[profile_name]
                if profile_name else protocol.required_signals
            )
            self.subjects[sid] = SubjectMonitor(sid, protocol, required)
        self.journal = Journal()
        self._merged_event_ids = set()
        self.journal.append("MISSION_FROZEN", created_by, self.started_at, {
            "mission_id": mission_id,
            "subject_ids": list(subject_ids),
            "profiles": dict(self.profiles),
            "protocol": protocol.describe(),
        })

    def _require_open(self):
        if self.closed_at is not None:
            raise MissionError(f"任务 {self.mission_id} 已关闭，不能再写入")

    def _subject(self, subject_id):
        try:
            return self.subjects[subject_id]
        except KeyError:
            raise MissionError(f"任务人员不在本任务编组: {subject_id}")

    def ingest(self, subject_id, slice_ref, received_at):
        self._require_open()
        monitor = self._subject(subject_id)
        result = monitor.timeline.ingest(slice_ref, received_at)
        for record in result.get("finalized", []):
            monitor.apply_assessment(record)
            self.journal.append("ASSESSMENT", "edge-engine", record["emitted_at"], {
                "mission_id": self.mission_id,
                "subject_id": subject_id,
                "assessment": record["assessment"],
            })
        for rejection in (
            [result["rejection"]] if result.get("rejection") else []
        ):
            self.journal.append("SLICE_REJECTED", "edge-engine", float(parse_ts(received_at)), {
                "mission_id": self.mission_id,
                "subject_id": subject_id,
                "rejection": rejection,
            })
        return result

    def heartbeat(self, now):
        self._require_open()
        emitted = []
        for monitor in self.subjects.values():
            for record in monitor.timeline.advance(now):
                monitor.apply_assessment(record)
                self.journal.append("ASSESSMENT", "edge-engine", record["emitted_at"], {
                    "mission_id": self.mission_id,
                    "subject_id": monitor.subject_id,
                    "assessment": record["assessment"],
                })
                emitted.append((monitor.subject_id, record))
        return emitted

    def record_action(self, subject_id, action, actor, at, role, note=""):
        """登记处置：建议 → 确认（现场复核）/ 实施（降温补水、转运）。"""
        self._require_open()
        at = float(parse_ts(at))
        if role != ROLE_MEDIC and action in MEDIC_ONLY_ACTIONS:
            raise MissionError(f"{action} 须由{ROLE_MEDIC}下令")
        if role not in (ROLE_MEDIC, ROLE_CORPSMAN):
            raise MissionError(f"角色无权登记处置: {role}")
        monitor = self._subject(subject_id)
        item = monitor.actions.get(action)
        if item is None:
            raise MissionError(f"当前没有针对 {subject_id} 的“{action}”建议")
        if item["status"] in ("acknowledged", "applied"):
            raise MissionError(f"“{action}”已在处置中，不能重复登记")
        item["status"] = "acknowledged" if action == "现场复核" else "applied"
        item["by"] = actor
        item["at"] = at
        item["history"].append({"status": item["status"], "at": at, "by": actor,
                                "role": role, "note": note})
        entry = self.journal.append("ACTION", actor, at, {
            "mission_id": self.mission_id,
            "subject_id": subject_id,
            "action": action,
            "status": item["status"],
            "basis": item["basis"],
            "note": note,
        })
        return entry

    def override(self, subject_id, medic_id, at, forced_level, reason):
        """值班军医覆盖系统建议。理由为强制项，空理由直接拒绝。"""
        self._require_open()
        at = float(parse_ts(at))
        if not reason or not str(reason).strip():
            raise MissionError("军医覆盖必须填写理由")
        if forced_level not in LEVEL_TO_STATE:
            raise MissionError(f"非法覆盖等级: {forced_level}")
        monitor = self._subject(subject_id)
        latest = monitor.timeline.finalized[-1]["assessment"] if monitor.timeline.finalized else None
        monitor.override = {
            "medic_id": medic_id,
            "at": at,
            "forced_level": forced_level,
            "reason": str(reason).strip(),
            "system_level": latest["level"] if latest else "normal",
            "basis": monitor._basis(latest) if latest else None,
            "persistent": False,
        }
        return self.journal.append("OVERRIDE", medic_id, at, {
            "mission_id": self.mission_id,
            "subject_id": subject_id,
            **monitor.override,
        })

    def resolve(self, subject_id, medic_id, at, reason=""):
        self._require_open()
        at = float(parse_ts(at))
        monitor = self._subject(subject_id)
        monitor.resolved = {
            "medic_id": medic_id,
            "at": at,
            "reason": reason,
        }
        for item in monitor.actions.values():
            if item["status"] in ("suggested", "acknowledged", "applied"):
                item["status"] = "cleared"
                item["history"].append(
                    {"status": "cleared", "at": at, "by": medic_id, "role": ROLE_MEDIC}
                )
        return self.journal.append("RESOLVED", medic_id, at, {
            "mission_id": self.mission_id,
            "subject_id": subject_id,
            "reason": reason,
        })

    def close(self, at, by=ROLE_MEDIC):
        self._require_open()
        self.closed_at = float(parse_ts(at))
        return self.journal.append("MISSION_CLOSED", by, self.closed_at, {
            "mission_id": self.mission_id,
            "protocol_version": self.protocol.version,
            "protocol_hash": self.protocol.protocol_hash,
            "note": "任务关闭，阈值更新只适用于后续任务，旧判断不可重写",
        })

    def status(self, subject_id):
        payload = self._subject(subject_id).status_payload()
        payload["slice_conflicts"] = self.slice_conflicts(subject_id)
        return payload

    def roster_status(self):
        return [self.status(sid) for sid in self.subjects]

    def slice_conflicts(self, subject_id=None):
        """从哈希链日志汇总切片冲突登记。

        日志是唯一事实来源：本地拒绝与回连合并进来的 SLICE_REJECTED 同构，
        因此离线合并、任务重建之后，视图与后续判断都保持一致。
        """
        records = []
        for entry in self.journal.entries:
            if entry["type"] != "SLICE_REJECTED":
                continue
            rejection = entry["payload"].get("rejection", {})
            if rejection.get("code") != "SLICE_CONFLICT":
                continue
            if subject_id is not None and entry["payload"].get("subject_id") != subject_id:
                continue
            records.append({
                "journal_seq": entry["seq"],
                "subject_id": entry["payload"].get("subject_id"),
                **rejection,
            })
        return records

    def explanation(self, subject_id):
        """返回某动作/当前状态的完整依据：规则、阈值、校准与输入窗口。"""
        monitor = self._subject(subject_id)
        latest = monitor.timeline.finalized[-1]["assessment"] if monitor.timeline.finalized else None
        return {
            "subject_id": subject_id,
            "state": monitor.state(),
            "protocol": {
                "version": self.protocol.version,
                "hash": self.protocol.protocol_hash,
            },
            "latest_assessment": latest,
            "actions": list(monitor.actions.values()),
            "override": monitor.override,
            "slice_conflicts": self.slice_conflicts(subject_id),
        }
