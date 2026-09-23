"""追加式日志：只允许追加，每条记录通过前序哈希串联。

任何评估、处置、军医覆盖、任务冻结/关闭都以日志为唯一事实来源。
旧判断永远不会被后续阈值更新改写：评估条目内钉死协议版本与协议哈希，
日志本身也不提供修改/删除接口。
"""

from protocol import canonical_json, content_hash

GENESIS_HASH = "0" * 64


class Journal:
    def __init__(self):
        self.entries = []
        self._last_hash = GENESIS_HASH

    @staticmethod
    def _entry_hash(seq, entry_type, actor, at, payload, prev_hash, extra=None):
        body = {
            "seq": seq,
            "type": entry_type,
            "actor": actor,
            "at": at,
            "payload": payload,
            "prev_hash": prev_hash,
        }
        if extra:
            body["origin"] = extra
        return content_hash(body)

    def append(self, entry_type, actor, at, payload, origin=None):
        seq = len(self.entries) + 1
        entry_hash = self._entry_hash(
            seq, entry_type, actor, at, payload, self._last_hash, origin
        )
        entry = {
            "seq": seq,
            "type": entry_type,
            "actor": actor,
            "at": at,
            "payload": payload,
            "prev_hash": self._last_hash,
            "entry_hash": entry_hash,
        }
        if origin:
            entry["origin"] = origin
        self.entries.append(entry)
        self._last_hash = entry_hash
        return entry

    def verify_chain(self):
        """从头复核哈希链，返回 bool 与首个断裂位置。"""
        prev = GENESIS_HASH
        for entry in self.entries:
            origin = entry.get("origin")
            expected = self._entry_hash(
                entry["seq"], entry["type"], entry["actor"], entry["at"],
                entry["payload"], prev, origin,
            )
            if entry["entry_hash"] != expected or entry["prev_hash"] != prev:
                return False, entry["seq"]
            prev = entry["entry_hash"]
        return True, None

    def since(self, seq):
        return [e for e in self.entries if e["seq"] > seq]

    def export(self):
        return {"entries": self.entries}
