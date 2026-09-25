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
        # 该窗口可能已由先到的 SLICE_ACCEPTED 在本地重建了开放缓冲；
        # 他节点的定稿评估到达后窗口即不可变，撤下缓冲，避免被水位线
        # 重复定稿（窗口内容以原样到达的评估为准）。
        self.timeline._open.pop(start, None)
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
        received_ts = float(parse_ts(received_at))
        result = monitor.timeline.ingest(slice_ref, received_at)
        # 切片接受事实先于评估上链：指纹是重启/合并后判定“同编号异内容”
        # 的唯一依据，必须与评估同处一条哈希链，篡改任一侧都可检出。
        if result["outcome"] == "accepted":
            self.journal.append("SLICE_ACCEPTED", "edge-engine", received_ts, {
                "mission_id": self.mission_id,
                "subject_id": subject_id,
                "slice": slice_ref,
                "fingerprint": result["fingerprint"],
                "window_start": result.get("window_start"),
                "received_at": received_ts,
            })
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
            # 冲突与其它拒绝一样落哈希链；冲突不附带任何评估，
            # 因此不可能推进或改变风险等级。
            self.journal.append(
                "SLICE_REJECTED", "edge-engine",
                float(rejection.get("received_at", received_ts)),
                {
                    "mission_id": self.mission_id,
                    "subject_id": subject_id,
                    "rejection": rejection,
                },
            )
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
        return self._subject(subject_id).status_payload()

    @classmethod
    def restore(cls, protocol, journal_export):
        """从哈希链日志重建任务（进程重启/主节点换机后恢复）。

        日志是唯一事实来源：重建时先复核哈希链，再按序回放
        MISSION_FROZEN 之后的全部条目。切片身份表由 SLICE_ACCEPTED 的
        内容指纹重建，冲突记录原样保留——因此重启后对“同编号异内容”
        仍会作出与停机前完全一致的判断；未定稿窗口的切片重新入窗，
        随后的心跳仍能把它们定稿，且结果与未重启时逐字节一致。
        """
        from journal import Journal
        from timeline import TimelineConflict

        journal = Journal()
        journal.entries = [dict(entry) for entry in journal_export["entries"]]
        ok, broken = journal.verify_chain()
        if not ok:
            raise MissionError(f"日志哈希链在第 {broken} 条断裂，拒绝恢复")
        frozen = next(
            (e for e in journal.entries if e["type"] == "MISSION_FROZEN"), None
        )
        if frozen is None:
            raise MissionError("日志缺少 MISSION_FROZEN 条目，无法恢复任务")
        spec = frozen["payload"]
        frozen_hash = spec.get("protocol", {}).get("protocol_hash")
        if frozen_hash != protocol.protocol_hash:
            raise MissionError(
                "恢复所用冻结协议与日志钉死的协议哈希不一致，拒绝恢复"
            )
        mission = cls(
            spec["mission_id"], protocol, spec["subject_ids"], frozen["at"],
            created_by=frozen["actor"], profiles=spec.get("profiles"),
        )
        # 用原始条目（连同 seq/prev_hash/entry_hash/origin）整体替换：
        # 恢复后的日志必须与恢复前逐字节一致，并把链尾游标接到最后一条，
        # 使恢复后新追加的条目继续串联在同一条哈希链上。
        mission.journal = journal
        journal._last_hash = (
            journal.entries[-1]["entry_hash"] if journal.entries else
            journal.GENESIS_HASH
        )

        # 阶段一：先回放全部已定稿评估，确定不可变窗口集合。
        for entry in journal.entries:
            if entry["type"] == "ASSESSMENT":
                monitor = mission.subjects[entry["payload"]["subject_id"]]
                monitor.adopt_assessment(
                    entry["payload"]["assessment"], entry["at"]
                )
        # 阶段二：回放接受/拒绝/处置/覆盖/解除与关闭。接受事实是否
        # 重新入窗由 adopt_slice 依定稿集合自行判定。
        for entry in journal.entries:
            try:
                mission._restore_entry(entry)
            except TimelineConflict as exc:
                raise MissionError(
                    f"恢复时发现切片 {exc.slice_id} 指纹与日志不一致，"
                    "日志可能被篡改"
                )
        for monitor in mission.subjects.values():
            monitor.timeline.rebuild_gap_cursor()
        mission._merged_event_ids = {
            (entry["origin"]["device_id"], entry["origin"]["local_seq"])
            for entry in journal.entries
            if entry.get("origin")
        }
        return mission

    def _restore_entry(self, entry):
        """把一条日志条目回放到内存状态（恢复专用，不再写日志）。"""
        etype = entry["type"]
        payload = entry.get("payload", {})
        subject_id = payload.get("subject_id")
        if etype == "MISSION_CLOSED":
            self.closed_at = float(entry["at"])
            return
        if subject_id is None or etype in ("MISSION_FROZEN", "ASSESSMENT"):
            return
        monitor = self.subjects.get(subject_id)
        if monitor is None:
            raise MissionError(f"日志引用了未知任务人员: {subject_id}")
        if etype == "SLICE_ACCEPTED":
            monitor.timeline.adopt_slice(
                payload["slice"], payload["fingerprint"],
                payload.get("received_at"),
            )
        elif etype == "SLICE_REJECTED":
            rejection = dict(payload["rejection"])
            rejection.setdefault("received_at", entry["at"])
            monitor.timeline.rejected.append(rejection)
        elif etype == "ACTION":
            monitor.apply_action_entry({**payload, "at_hint": entry["at"]})
        elif etype == "OVERRIDE":
            monitor.apply_override_entry(payload)
        elif etype == "RESOLVED":
            monitor.apply_resolved_entry(payload)

    def conflicts(self, subject_id=None):
        """哈希链中登记的切片内容冲突（含本地拒绝与离线合并发现的跨节点冲突）。

        日志是唯一事实来源：即使内存中的时间线被重建，冲突记录仍然可查，
        且冲突从不伴随评估，因此不会推进任何人的风险等级。
        """
        result = []
        for entry in self.journal.entries:
            if entry["type"] != "SLICE_REJECTED":
                continue
            rejection = entry["payload"].get("rejection", {})
            if rejection.get("code") != "SLICE_CONTENT_CONFLICT":
                continue
            owner = entry["payload"].get("subject_id")
            if subject_id is not None and owner != subject_id:
                continue
            result.append({
                "journal_seq": entry["seq"],
                "at": entry["at"],
                "subject_id": owner,
                "rejection": rejection,
                "origin": entry.get("origin"),
            })
        return result

    def roster_status(self):
        return [m.status_payload() for m in self.subjects.values()]

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
        }
