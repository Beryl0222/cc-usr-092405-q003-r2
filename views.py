"""按角色裁剪的履职视图：只呈现履职所需状态，不暴露完整健康档案。

- 指挥人员：编组状态与待执行/执行中的处置，用于调度撤离，不含原始体征；
- 现场卫生员：本人处置所需的动作依据与质量提示，用于现场复核；
- 值班军医：完整评估、规则阈值、校准编号与覆盖记录，用于解释与裁决；
- 任务人员：仅本人状态与需配合的动作提示。
"""

ROLE_COMMANDER = "指挥人员"
ROLE_MEDIC = "值班军医"
ROLE_CORPSMAN = "现场卫生员"
ROLE_PERSONNEL = "任务人员"


def _commander_row(status):
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
    }


def render_commander(mission):
    return {
        "view": ROLE_COMMANDER,
        "mission_id": mission.mission_id,
        "closed": mission.closed_at is not None,
        "roster": [_commander_row(s) for s in mission.roster_status()],
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
    }


def render_medic(mission, subject_id=None):
    if subject_id is None:
        return {
            "view": ROLE_MEDIC,
            "mission_id": mission.mission_id,
            "roster": [mission.explanation(sid) for sid in sorted(mission.subjects)],
        }
    return {
        "view": ROLE_MEDIC,
        "mission_id": mission.mission_id,
        "detail": mission.explanation(subject_id),
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
