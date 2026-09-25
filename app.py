"""边缘应用层：持有冻结协议与任务集合，供 HTTP 层与离线测试共用。"""

import threading

from mission import Mission, MissionError
from protocol import FrozenProtocol
from sync import MergeError, export_bundle, merge_bundles

ROLES = ("指挥人员", "值班军医", "现场卫生员", "任务人员")


class AppError(ValueError):
    """请求体非法（字段缺失、格式错误）。"""


class EdgeApp:
    def __init__(self, protocol):
        self.protocol = protocol
        self._missions = {}
        self._lock = threading.RLock()

    def create_mission(self, mission_id, subject_ids, started_at,
                       created_by="卫勤团队", profiles=None):
        if not mission_id:
            raise AppError("缺少 mission_id")
        if not subject_ids:
            raise AppError("缺少任务人员编组 subject_ids")
        with self._lock:
            if mission_id in self._missions:
                raise AppError(f"任务已存在: {mission_id}")
            mission = Mission(
                mission_id, self.protocol, tuple(subject_ids), started_at,
                created_by, profiles,
            )
            self._missions[mission_id] = mission
            return mission

    def mission(self, mission_id):
        try:
            return self._missions[mission_id]
        except KeyError:
            raise AppError(f"任务不存在: {mission_id}")

    def restore_mission(self, journal_export):
        """从哈希链日志恢复任务（进程重启后继续，判断与停机前一致）。"""
        if not isinstance(journal_export, dict) or "entries" not in journal_export:
            raise AppError("恢复请求必须包含日志 entries")
        with self._lock:
            mission = Mission.restore(self.protocol, journal_export)
            if mission.mission_id in self._missions:
                raise AppError(f"任务已存在: {mission.mission_id}")
            self._missions[mission.mission_id] = mission
            return mission

    def run_locked(self, mission_id, fn):
        with self._lock:
            return fn(self.mission(mission_id))

    def merge(self, mission_id, bundles):
        if not isinstance(bundles, list):
            raise AppError("bundles 必须为数组")
        with self._lock:
            return merge_bundles(self.mission(mission_id), bundles)

    def export(self, mission_id, device_id):
        if not device_id:
            raise AppError("缺少 device_id")
        with self._lock:
            return export_bundle(self.mission(mission_id), device_id)

    @staticmethod
    def mission_error_type(exc):
        if isinstance(exc, MissionError):
            return "mission_error"
        if isinstance(exc, MergeError):
            return "merge_error"
        return "app_error"
