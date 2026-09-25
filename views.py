"""按角色裁剪的履职视图：只呈现履职所需状态，不暴露完整健康档案。

- 指挥人员：编组状态与待执行/执行中的处置，用于调度撤离，不含原始体征；
  切片冲突只见计数与切片编号，知道“某设备链路不可信、需停用排查”即可；
- 现场卫生员：本人处置所需的动作依据与质量提示，用于现场复核；冲突可见
  差异字段类别（设备/序号/采样时间/读数）与时间，不含任何读数数值；
- 值班军医：完整评估、规则阈值、校准编号、冲突两侧内容与双侧指纹，
  用于解释、裁决与设备排查；
- 任务人员：仅本人状态与需配合的动作提示。
"""

ROLE_COMMANDER = "指挥人员"
ROLE_MEDIC = "值班军医"
ROLE_CORPSMAN = "现场卫生员"
ROLE_PERSONNEL = "任务人员"

# 差异字段的角色中立描述：只说明“哪一类信息对不上”，不暴露数值。
_FIELD_LABEL = {
    "device_id": "设备编号",
    "seq": "切片序号",
    "sampled_at": "采样时间",
    "samples": "校准前读数",
}


def _conflict_summary(entries):
    """指挥人员可见的冲突摘要：编号与计数，不含任何数值与字段差异。"""
    return [
        {
            "slice_id": item["rejection"].get("slice_id"),
            "journal_seq": item["journal_seq"],
            "at": item["at"],
        }
        for item in entries
    ]


def _conflict_for_corpsman(item):
    """卫生员可见的冲突：差异字段类别，不含任何读数数值。"""
    rejection = item["rejection"]
    return {
        "slice_id": rejection.get("slice_id"),
        "device_id": rejection.get("device_id"),
        "seq": rejection.get("seq"),
        "at": item["at"],
        "changed_fields": [
            _FIELD_LABEL.get(name, name)
            for name in rejection.get("changed_fields", [])
        ],
        "detail": "同一切片编号回传内容不一致，已拒绝入窗，请核对设备固件",
    }


def _conflict_for_medic(item):
    """军医可见完整冲突：两侧身份内容、双侧指纹、差异字段与来源节点。"""
    rejection = item["rejection"]
    return {
        "slice_id": rejection.get("slice_id"),
        "journal_seq": item["journal_seq"],
        "at": item["at"],
        "changed_fields": rejection.get("changed_fields", []),
        "stored_fingerprint": rejection.get("stored_fingerprint"),
        "incoming_fingerprint": rejection.get("incoming_fingerprint"),
        "stored_identity": rejection.get("stored_identity"),
        "incoming_identity": rejection.get("incoming_identity"),
        "origin": item.get("origin"),
        "detail": rejection.get("detail"),
    }


def _commander_row(mission, status):
    conflicts = mission.conflicts(status["subject_id"])
    return {
        "subject_id": status["subject_id"],
        "state": status["state"],
        "effective_level": status["effective_level"],
        "pending_actions": [
            a["action"] for a in status["actions"] if a["status"] == "suggested"
        ],
        "active_actions": [
            a["action"]
            for a in status["actions"] if a["status"] in ("acknowledged", "applied")
        ],
        "overridden": status["override"] is not None,
        "window": status["latest_window"],
        # 冲突只以计数/编号出现：它是链路完整性告警，不是生理事实，
        # 绝不改变 state/effective_level（等级不被冲突推进）。
        "slice_conflict_count": len(conflicts),
        "slice_conflicts": _conflict_summary(conflicts),
    }


def render_commander(mission):
    return {
        "view": ROLE_COMMANDER,
        "mission_id": mission.mission_id,
        "closed": mission.closed_at is not None,
        "roster": [_commander_row(mission, s) for s in mission.roster_status()],
    }


def render_corpsman(mission, subject_id):
    status = mission.status(subject_id)
    return {
        "view": ROLE_CORPSMAN,
        "mission_id": mission.mission_id,
        "subject_id": subject_id,
        "state": status["state"],
        "actions": [
            {
                "action": a["action"],
                "status": a["status"],
                # 卫生员执行现场复核需要知道依据哪条规则与窗口，但不展示
                # 全部生理数值。
                "basis": {
                    "rule_ids": a["basis"]["rule_ids"],
                    "window_start": a["basis"]["window_start"],
                    "window_end": a["basis"]["window_end"],
                    "protocol_version": a["basis"]["protocol_version"],
                },
            }
            for a in status["actions"]
        ],
        "quality_flags": [
            flag["code"]
            for record in mission.subjects[subject_id].timeline.finalized
            for flag in record["assessment"].get("quality_flags", [])
        ],
        "slice_conflicts": [
            _conflict_for_corpsman(item)
            for item in mission.conflicts(subject_id)
        ],
    }


def render_medic(mission, subject_id=None):
    if subject_id is None:
        roster = []
        for sid in sorted(mission.subjects):
            explanation = mission.explanation(sid)
            explanation["slice_conflicts"] = [
                _conflict_for_medic(item) for item in mission.conflicts(sid)
            ]
            roster.append(explanation)
        return {
            "view": ROLE_MEDIC,
            "mission_id": mission.mission_id,
            "roster": roster,
        }
    detail = mission.explanation(subject_id)
    detail["slice_conflicts"] = [
        _conflict_for_medic(item) for item in mission.conflicts(subject_id)
    ]
    return {
        "view": ROLE_MEDIC,
        "mission_id": mission.mission_id,
        "detail": detail,
    }


def render_personnel(mission, subject_id):
    status = mission.status(subject_id)
    return {
        "view": ROLE_PERSONNEL,
        "subject_id": subject_id,
        "state": status["state"],
        "instructions": [
            a["action"]
            for a in status["actions"] if a["status"] in ("suggested", "acknowledged")
        ],
    }


def render(mission, role, subject_id=None):
    if role == ROLE_COMMANDER:
        return render_commander(mission)
    if role == ROLE_MEDIC:
        return render_medic(mission, subject_id)
    if role == ROLE_CORPSMAN:
        if subject_id is None:
            raise ValueError("现场卫生员视图必须指定任务人员")
        return render_corpsman(mission, subject_id)
    if role == ROLE_PERSONNEL:
        if subject_id is None:
            raise ValueError("任务人员视图必须指定本人")
        return render_personnel(mission, subject_id)
    raise ValueError(f"未知角色: {role}")


def render_conflicts(mission, role, subject_id=None):
    """切片内容冲突的角色化视图：与 render 同套最小知情规则。

    冲突是链路完整性事实（固件回滚/重传损坏），不是生理读数，因此
    任务人员视图不暴露；其余角色按履职需要逐级放宽。
    """
    entries = mission.conflicts(subject_id)
    if role == ROLE_COMMANDER:
        return {
            "view": ROLE_COMMANDER,
            "mission_id": mission.mission_id,
            "subject_id": subject_id,
            "slice_conflict_count": len(entries),
            "slice_conflicts": _conflict_summary(entries),
        }
    if role == ROLE_CORPSMAN:
        if subject_id is None:
            raise ValueError("现场卫生员视图必须指定任务人员")
        return {
            "view": ROLE_CORPSMAN,
            "mission_id": mission.mission_id,
            "subject_id": subject_id,
            "slice_conflicts": [_conflict_for_corpsman(item) for item in entries],
        }
    if role == ROLE_MEDIC:
        return {
            "view": ROLE_MEDIC,
            "mission_id": mission.mission_id,
            "subject_id": subject_id,
            "slice_conflicts": [_conflict_for_medic(item) for item in entries],
        }
    raise ValueError(f"角色无权查看切片冲突: {role}")
