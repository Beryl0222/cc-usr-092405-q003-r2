"""时间工具：全部以 UTC 纪元秒为内部单位，避免时区与格式歧义。"""

from datetime import datetime, timezone


def parse_ts(value):
    """解析 ISO 8601 时间串，Z 结尾按 UTC 处理。"""
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def format_ts(epoch_seconds):
    dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def align_window(epoch_seconds, window_seconds):
    """把任意时刻归并到固定窗口起点（向过去取整）。"""
    return (int(epoch_seconds) // window_seconds) * window_seconds
