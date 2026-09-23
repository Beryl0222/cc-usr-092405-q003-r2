"""冻结协议：任务开始前冻结的阈值、设备校准与风险方案。

协议文件在任务开始前由卫勤团队审定并冻结，边缘设备只持有冻结副本。
``protocol_hash`` 为协议正文的内容哈希，会随每条评估与处置记录落盘，
任务结束后即使下发新版本，也只能用于后续任务，旧判断仍钉在旧版本上。
"""

import copy
import hashlib
import json
import os

DEFAULT_PROTOCOL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "fixtures", "protocol.json"
)

LEVEL_ORDER = ["normal", "review", "alert", "intervene"]
LEVEL_LABEL = {
    "normal": "监测中",
    "review": "需复核",
    "alert": "已预警",
    "intervene": "干预中",
}
ACTION_LABEL = {
    "现场复核": "现场复核",
    "降温补水": "降温补水",
    "转运": "转运",
}


def canonical_json(obj):
    """对对象做规范化序列化，保证同一内容在任意节点哈希一致。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_hash(obj):
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


class ProtocolError(ValueError):
    """协议缺失、设备未登记或校准不存在时抛出，绝不以默认值顶替。"""


class FrozenProtocol:
    def __init__(self, data, source_path=None):
        self._validate(data)
        self._data = copy.deepcopy(data)
        self.source_path = source_path
        self.version = data["version"]
        self.protocol_hash = content_hash(self._freeze_body(data))
        self.window_seconds = int(data["window_seconds"])
        self.allowed_lateness_seconds = int(data["allowed_lateness_seconds"])
        self.emit_period_seconds = int(data["emit_period_seconds"])
        self.clock_drift_limit_ms = int(data["clock_drift_limit_ms"])
        self.required_signals = tuple(data["required_signals"])
        self.profiles = {
            name: tuple(profile["required_signals"])
            for name, profile in data.get("profiles", {}).items()
        }
        self.penalties = dict(data["penalties"])
        self.confidence_floor = dict(data["confidence_floor"])
        self.signal_tolerance = dict(data["signal_tolerance"])
        self.rules = tuple(copy.deepcopy(r) for r in data["rules"])
        self.escalation = {k: tuple(v) for k, v in data["escalation"].items()}
        self._devices = {d["device_id"]: copy.deepcopy(d) for d in data["devices"]}

    @staticmethod
    def _freeze_body(data):
        """哈希正文排除运行期字段，仅覆盖任务前审定的配置本身。"""
        return data

    @staticmethod
    def _validate(data):
        for key in (
            "schema", "version", "window_seconds", "allowed_lateness_seconds",
            "emit_period_seconds", "clock_drift_limit_ms", "required_signals",
            "penalties", "confidence_floor", "signal_tolerance", "devices",
            "rules", "escalation",
        ):
            if key not in data:
                raise ProtocolError(f"协议缺少字段: {key}")
        for device in data["devices"]:
            for key in ("device_id", "sensors", "calibration"):
                if key not in device:
                    raise ProtocolError(f"设备登记缺少字段: {key}")
        for rule in data["rules"]:
            for key in ("id", "signal", "metric", "direction", "bands"):
                if key not in rule:
                    raise ProtocolError(f"规则缺少字段: {key}")
            if rule["direction"] not in ("high", "low"):
                raise ProtocolError(f"规则方向非法: {rule['id']}")

    @classmethod
    def load(cls, path=DEFAULT_PROTOCOL_PATH):
        with open(path, "r", encoding="utf-8") as handle:
            return cls(json.load(handle), source_path=path)

    def device(self, device_id):
        try:
            return self._devices[device_id]
        except KeyError:
            raise ProtocolError(f"未登记设备: {device_id}")

    def is_registered(self, device_id):
        return device_id in self._devices

    def calibrate(self, device_id, signal, raw_value):
        """按冻结校准把原始读数换算为工程值。

        返回 ``(校准值, 校准编号)``。设备未登记或信号不在该设备量程内时
        直接报错——宁可不评估，也不允许使用未经校准的读数。
        """
        device = self.device(device_id)
        if signal not in device["sensors"]:
            raise ProtocolError(f"设备 {device_id} 未登记信号 {signal}")
        cal = device["calibration"].get(signal)
        if cal is None:
            raise ProtocolError(f"设备 {device_id} 的 {signal} 缺少校准")
        value = float(cal.get("gain", 1.0)) * float(raw_value) + float(
            cal.get("offset", 0.0)
        )
        return value, cal["cal_id"]

    def rule_for(self, signal):
        return tuple(r for r in self.rules if r["signal"] == signal)

    def actions_for(self, level):
        return tuple(self.escalation.get(level, ()))

    def describe(self):
        return {
            "schema": self._data["schema"],
            "version": self.version,
            "protocol_hash": self.protocol_hash,
            "window_seconds": self.window_seconds,
            "allowed_lateness_seconds": self.allowed_lateness_seconds,
            "emit_period_seconds": self.emit_period_seconds,
            "clock_drift_limit_ms": self.clock_drift_limit_ms,
            "required_signals": list(self.required_signals),
            "rules": [
                {
                    "id": r["id"],
                    "signal": r["signal"],
                    "metric": r["metric"],
                    "direction": r["direction"],
                    "bands": r["bands"],
                    "note": r.get("note", ""),
                }
                for r in self.rules
            ],
            "devices": [
                {
                    "device_id": d["device_id"],
                    "sensors": d["sensors"],
                    "calibration": {
                        s: c["cal_id"] for s, c in d["calibration"].items()
                    },
                }
                for d in self._devices.values()
            ],
            "escalation": self.escalation,
        }
