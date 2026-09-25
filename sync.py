"""恢复连接后的安全汇入。

边缘设备离线期间各自维护本地日志；回连后按设备序列把日志包汇入主节点。
安全约束：
- 任务编号与冻结协议哈希必须一致，不一致整包拒绝（不同阈值下的判断
  不能混入同一条时间线）；
- 按 (设备编号, 本地日志序号) 定序后再汇入，多设备汇入结果与送达顺序
  无关，重复投递幂等；
- 切片接受事实（SLICE_ACCEPTED，含内容指纹）随包汇入，主节点据此重建
  切片身份表：同 ``slice_id`` 在不同节点指纹不一致属于跨节点数据冲突，
  整次汇入事务式中止，不写入任何条目；
- 已定稿评估只按输入窗口去重采纳，内容原样保留，不重算、不补造；
- 对端登记的切片冲突（SLICE_REJECTED/SLICE_CONTENT_CONFLICT）原样落入
  主节点哈希链，冲突只留痕，永不参与风险推进。
"""

from timeline import TimelineConflict

MERGEABLE_TYPES = {
    "ASSESSMENT", "SLICE_ACCEPTED", "SLICE_REJECTED",
    "ACTION", "OVERRIDE", "RESOLVED",
}


class MergeError(ValueError):
    pass


def export_bundle(mission, device_id):
    """导出某边缘设备持有的可汇入日志包。"""
    return {
        "schema": "field-health-alert/sync-bundle-v1",
        "device_id": device_id,
        "mission_id": mission.mission_id,
        "protocol_version": mission.protocol.version,
        "protocol_hash": mission.protocol.protocol_hash,
        "entries": [
            {
                "local_seq": entry["seq"],
                "type": entry["type"],
                "actor": entry["actor"],
                "at": entry["at"],
                "payload": entry["payload"],
            }
            for entry in mission.journal.entries
            if entry["type"] in MERGEABLE_TYPES
        ],
    }


def _replay_state(mission, entry):
    """把汇入条目应用到主节点的内存状态，不重复写日志。"""
    payload = entry["payload"]
    subject_id = payload.get("subject_id")
    if subject_id is None:
        return
    monitor = mission.subjects.get(subject_id)
    if monitor is None:
        raise MergeError(f"汇入条目引用了未知任务人员: {subject_id}")
    etype = entry["type"]
    if etype == "ASSESSMENT":
        monitor.adopt_assessment(payload["assessment"], entry["at"])
    elif etype == "SLICE_ACCEPTED":
        # 重建切片身份表（指纹已在预检中核验）；未定稿窗口重新入窗，
        # 使主节点在评估未随包到达时仍能自行定稿，已定稿窗口不重算。
        monitor.timeline.adopt_slice(
            payload["slice"], payload["fingerprint"],
            payload.get("received_at"),
        )
    elif etype == "ACTION":
        monitor.apply_action_entry({
            **payload, "at_hint": entry["at"], "actor_hint": entry["actor"],
        })
    elif etype == "OVERRIDE":
        monitor.apply_override_entry(payload)
    elif etype == "RESOLVED":
        monitor.apply_resolved_entry(payload)
    # SLICE_REJECTED 只落日志，无需状态回放。


def _host_identities(mission):
    """主节点哈希链中已有的切片指纹（含此前合并进来的他节点接受事实）。"""
    identities = {}
    for entry in mission.journal.entries:
        if entry["type"] != "SLICE_ACCEPTED":
            continue
        payload = entry["payload"]
        identities[payload["slice"]["slice_id"]] = payload["fingerprint"]
    return identities


def merge_bundles(mission, bundles):
    """把若干设备日志包安全汇入主节点，返回事务式报告。"""
    report = {"merged": [], "duplicates": [], "rejected": []}
    if mission.closed_at is not None:
        raise MergeError("任务已关闭，拒绝汇入")

    # 1) 预校验：任何一包不合规，整体不写入。
    normalized = []
    for bundle in bundles:
        device_id = bundle.get("device_id")
        if bundle.get("mission_id") != mission.mission_id:
            report["rejected"].append({
                "device_id": device_id,
                "reason": "任务编号不一致",
            })
            continue
        if bundle.get("protocol_hash") != mission.protocol.protocol_hash:
            report["rejected"].append({
                "device_id": device_id,
                "reason": "冻结协议哈希不一致，禁止跨阈值汇入",
                "expected": mission.protocol.protocol_hash,
                "got": bundle.get("protocol_hash"),
            })
            continue
        local_seqs = [e["local_seq"] for e in bundle["entries"]]
        if local_seqs != sorted(local_seqs) or len(set(local_seqs)) != len(local_seqs):
            report["rejected"].append({
                "device_id": device_id,
                "reason": "设备本地日志序号非严格有序",
            })
            continue
        normalized.append((device_id, bundle["entries"]))
    if report["rejected"]:
        # 协议/任务错配属于硬错误：中止整次汇入，已校验包也不生效。
        return report

    # 2) 定序：先按条目发生时间，再按设备编号与设备内序号，
    #    保证多设备汇入结果与送达顺序无关。
    normalized.sort(key=lambda item: item[0])
    ordered = [
        (device_id, entry)
        for device_id, entries in normalized
        for entry in entries
        if entry["type"] in MERGEABLE_TYPES
    ]
    ordered.sort(key=lambda item: (
        item[1]["at"], item[0], item[1]["local_seq"]
    ))

    # 3) 写入前的身份一致性预检：任一 slice_id 跨节点指纹不一致，
    #    整次汇入中止，不产生任何副作用（事务式）。
    fingerprints = _host_identities(mission)
    pending = {}
    for device_id, entry in ordered:
        if entry["type"] != "SLICE_ACCEPTED":
            continue
        key = (device_id, entry["local_seq"])
        if key in mission._merged_event_ids:
            continue
        payload = entry["payload"]
        slice_id = payload["slice"]["slice_id"]
        fingerprint = payload["fingerprint"]
        known = fingerprints.get(slice_id, pending.get(slice_id))
        if known is not None and known != fingerprint:
            report["rejected"].append({
                "device_id": device_id,
                "local_seq": entry["local_seq"],
                "slice_id": slice_id,
                "reason": "切片内容指纹跨节点不一致，疑似固件回滚后重传",
                "stored_fingerprint": known,
                "incoming_fingerprint": fingerprint,
            })
            return report
        pending.setdefault(slice_id, fingerprint)

    # 4) 幂等回放：同一 (设备, 本地序号) 只生效一次。
    for device_id, entry in ordered:
        key = (device_id, entry["local_seq"])
        if key in mission._merged_event_ids:
            report["duplicates"].append({"device_id": device_id,
                                         "local_seq": entry["local_seq"]})
            continue
        try:
            _replay_state(mission, entry)
        except TimelineConflict as exc:
            # 预检理论上已拦住；防御性处理，保证绝不部分写入。
            report["rejected"].append({
                "device_id": device_id,
                "local_seq": entry["local_seq"],
                "slice_id": exc.slice_id,
                "reason": "切片内容指纹跨节点不一致，疑似固件回滚后重传",
                "stored_fingerprint": exc.stored_fingerprint,
                "incoming_fingerprint": exc.incoming_fingerprint,
            })
            raise MergeError(str(exc))
        journal_entry = mission.journal.append(
            entry["type"], entry["actor"], entry["at"], entry["payload"],
            origin={"device_id": device_id, "local_seq": entry["local_seq"]},
        )
        mission._merged_event_ids.add(key)
        report["merged"].append({
            "device_id": device_id,
            "local_seq": entry["local_seq"],
            "type": entry["type"],
            "journal_seq": journal_entry["seq"],
        })
    return report
