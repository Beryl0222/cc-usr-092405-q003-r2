"""确定性规则评分引擎。

引擎本身不含任何在线学习或插补：只对窗口内真实到齐、且已按冻结校准
换算过的样本做规则判定。传感器失联、时钟漂移、序列缺口、跨设备矛盾
读数统一表现为置信度扣减与等级钳制，系统从不猜测缺失信号的取值。
"""

import statistics

from protocol import LEVEL_ORDER

MODEL_ID = "rule-engine-v1"
LEVEL_RANK = {level: index for index, level in enumerate(LEVEL_ORDER)}


def max_level(levels):
    if not levels:
        return "normal"
    return max(levels, key=lambda level: LEVEL_RANK[level])


def detect_contradictions(samples_by_signal, tolerance):
    """同一信号若由多台设备同时上报，比较各设备窗口中位数。

    差异超过冻结容差即记为矛盾读数；矛盾的处理是扣减置信度，
    而不是挑选其中“看起来正常”的一台。
    """
    contradictions = []
    for signal, items in samples_by_signal.items():
        by_device = {}
        for item in items:
            by_device.setdefault(item["device_id"], []).append(item["v"])
        if len(by_device) < 2:
            continue
        medians = {
            device_id: statistics.median(values)
            for device_id, values in by_device.items()
        }
        devices = sorted(medians)
        for i, left in enumerate(devices):
            for right in devices[i + 1:]:
                gap = abs(medians[left] - medians[right])
                if gap > tolerance.get(signal, 0):
                    contradictions.append({
                        "signal": signal,
                        "devices": [left, right],
                        "values": [round(medians[left], 3), round(medians[right], 3)],
                        "gap": round(gap, 3),
                        "tolerance": tolerance.get(signal, 0),
                    })
    return contradictions


def _aggregate(items, metric):
    values = [item["v"] for item in items]
    if metric == "min":
        return min(values)
    if metric == "max":
        return max(values)
    if metric == "mean":
        return statistics.fmean(values)
    if metric == "median":
        return statistics.median(values)
    raise ValueError(f"不支持的聚合方式: {metric}")


def _band_hit(rule, value):
    """按高/低方向返回该值触及的最高档位，未触及返回 None。"""
    bands = rule["bands"]
    direction = rule["direction"]
    hit = None
    for band in ("review", "alert", "intervene"):
        if band not in bands:
            continue
        threshold = bands[band]
        breached = (
            value >= threshold if direction == "high" else value <= threshold
        )
        if breached and (hit is None or LEVEL_RANK[band] > LEVEL_RANK[hit]):
            hit = band
    return hit


def evaluate_window(window_input, protocol):
    """对一个已定稿窗口执行规则评估，输出可解释的判定结构。

    输出为纯数据（可 JSON 序列化），同输入必得同输出，不含当前时间等
    环境依赖，便于乱序回放与节点间核对。
    """
    samples_by_signal = window_input["samples"]
    quality_in = window_input.get("quality", {})

    # 1) 规则命中：每条命中都留下规则号、聚合值、阈值与校准编号。
    rule_hits = []
    levels = []
    for rule in protocol.rules:
        items = samples_by_signal.get(rule["signal"])
        if not items:
            continue
        value = _aggregate(items, rule["metric"])
        band = _band_hit(rule, value)
        hit = {
            "rule_id": rule["id"],
            "signal": rule["signal"],
            "metric": rule["metric"],
            "direction": rule["direction"],
            "value": round(value, 3),
            "bands": rule["bands"],
            "band": band,
            "cal_ids": sorted({item["cal_id"] for item in items}),
            "devices": sorted({item["device_id"] for item in items}),
            "sample_count": len(items),
            "note": rule.get("note", ""),
        }
        rule_hits.append(hit)
        if band:
            levels.append(band)
    raw_level = max_level(levels)

    # 2) 质量事实：缺失、漂移、矛盾、缺口。只登记证据，不补造数据。
    required = tuple(window_input.get("required_signals", protocol.required_signals))
    present = set(samples_by_signal)
    missing_signals = [s for s in required if s not in present]
    contradictions = detect_contradictions(
        samples_by_signal, protocol.signal_tolerance
    )
    drifts = list(quality_in.get("drifts", ()))
    seq_gaps = list(quality_in.get("seq_gaps", ()))

    quality_flags = []
    for signal in missing_signals:
        quality_flags.append({
            "code": "MISSING_SENSOR",
            "signal": signal,
            "detail": f"窗口内未收到 {signal} 的任何已校准样本",
        })
    for drift in drifts:
        quality_flags.append({"code": "CLOCK_DRIFT", **drift})
    for contradiction in contradictions:
        quality_flags.append({"code": "CONTRADICTION", **contradiction})
    for gap in seq_gaps:
        quality_flags.append({"code": "SEQ_GAP", **gap})

    # 3) 置信度：从 1.0 起按冻结罚项扣减，下限 0。
    confidence = 1.0
    confidence -= protocol.penalties.get("missing_sensor", 0) * len(missing_signals)
    confidence -= protocol.penalties.get("clock_drift", 0) * min(len(drifts), 1)
    confidence -= protocol.penalties.get("contradiction", 0) * len(contradictions)
    confidence -= protocol.penalties.get("seq_gap", 0) * len(
        {g["device_id"] for g in seq_gaps}
    )
    confidence = round(max(0.0, min(1.0, confidence)), 3)

    # 4) 置信度不足时只降不升：高危判定被钳制到“现场复核”，并写明原因。
    level = raw_level
    downgrades = []
    floor_alert = protocol.confidence_floor.get("alert", 0.75)
    floor_intervene = protocol.confidence_floor.get("intervene", 0.85)
    if raw_level == "intervene" and confidence < floor_intervene:
        downgrades.append({
            "from": "intervene",
            "to": "review",
            "reason": f"置信度 {confidence} 低于干预下限 {floor_intervene}，降级为现场复核",
        })
        level = "review"
    elif raw_level == "alert" and confidence < floor_alert:
        downgrades.append({
            "from": "alert",
            "to": "review",
            "reason": f"置信度 {confidence} 低于预警下限 {floor_alert}，降级为现场复核",
        })
        level = "review"

    window_start = window_input["window_start"]
    return {
        "schema": "field-health-alert/assessment-v1",
        "model_id": MODEL_ID,
        "protocol_version": protocol.version,
        "protocol_hash": protocol.protocol_hash,
        "input_window": {
            "start": window_start,
            "end": window_start + protocol.window_seconds,
            "window_seconds": protocol.window_seconds,
            "signals_present": sorted(present),
            "signals_required": list(required),
            "sample_counts": {
                signal: len(items)
                for signal, items in sorted(samples_by_signal.items())
            },
        },
        "raw_level": raw_level,
        "level": level,
        "confidence": confidence,
        "rule_hits": rule_hits,
        "quality_flags": quality_flags,
        "downgrades": downgrades,
        "actions": list(protocol.actions_for(level)),
    }
