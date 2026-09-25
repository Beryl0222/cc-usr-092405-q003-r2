"""极端作业健康预警的运行入口。

除原有 /health 外，提供离线边缘预警的任务冻结、切片接收、状态视图、
处置/覆盖、日志校验与回连合并接口。服务本身不依赖任何外部服务，可在
任务现场离线运行。
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from app import AppError, EdgeApp
from protocol import FrozenProtocol
from views import render, render_conflicts

SERVICE_ID = "field-health-alert"
SERVICE_NAME = "极端作业健康预警"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


class Handler(BaseHTTPRequestHandler):
    app = None  # 由 main 注入

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise AppError("请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise AppError("请求体必须是 JSON 对象")
        return data

    def _error(self, exc):
        self._send_json(400, {"error": type(self.app).mission_error_type(exc)
                              if self.app else "app_error",
                              "message": str(exc)})

    def do_GET(self):
        parts = urlsplit(self.path)
        path, query = parts.path, parse_qs(parts.query)
        try:
            if path == "/health":
                self._send_json(200, health_payload())
                return
            if path == "/protocol":
                self._send_json(200, self.app.protocol.describe())
                return
            segments = [s for s in path.split("/") if s]
            if len(segments) == 3 and segments[0] == "missions" and segments[2] == "status":
                subject_id = (query.get("subject_id") or [None])[0]
                mission = self.app.mission(segments[1])
                if subject_id:
                    payload = mission.status(subject_id)
                else:
                    from views import render_commander
                    payload = render_commander(mission)
                self._send_json(200, payload)
                return
            if len(segments) == 3 and segments[0] == "missions" and segments[2] == "view":
                role = (query.get("role") or [""])[0]
                subject_id = (query.get("subject_id") or [None])[0]
                payload = render(self.app.mission(segments[1]), role, subject_id)
                self._send_json(200, payload)
                return
            if len(segments) == 3 and segments[0] == "missions" and segments[2] == "conflicts":
                # 切片内容冲突按角色裁剪：指挥人员见计数/编号，卫生员见
                # 差异字段类别，军医见两侧内容与指纹；任务人员无权查看。
                role = (query.get("role") or [""])[0]
                subject_id = (query.get("subject_id") or [None])[0]
                payload = render_conflicts(
                    self.app.mission(segments[1]), role, subject_id
                )
                self._send_json(200, payload)
                return
            if len(segments) == 3 and segments[0] == "missions" and segments[2] == "journal":
                mission = self.app.mission(segments[1])
                ok, broken_at = mission.journal.verify_chain()
                self._send_json(200, {
                    "intact": ok,
                    "broken_at_seq": broken_at,
                    "journal": mission.journal.export(),
                })
                return
            self.send_error(404)
        except (AppError, ValueError) as exc:
            self._error(exc)
        except KeyError as exc:
            self._error(AppError(f"缺少字段: {exc.args[0]}"))

    def do_POST(self):
        parts = urlsplit(self.path)
        path = parts.path
        segments = [s for s in path.split("/") if s]
        try:
            data = self._read_json()
            if len(segments) == 1 and segments[0] == "missions":
                mission = self.app.create_mission(
                    data["mission_id"], data["subject_ids"], data["started_at"],
                    data.get("created_by", "卫勤团队"), data.get("profiles"),
                )
                self._send_json(201, {
                    "mission_id": mission.mission_id,
                    "started_at": mission.started_at,
                    "protocol": mission.protocol.describe(),
                })
                return

            if len(segments) == 2 and segments[0] == "missions" and segments[1] == "restore":
                # 进程重启后从哈希链日志恢复任务：身份表、冲突记录与
                # 未定稿窗口全部重建，后续判断与停机前一致。
                mission = self.app.restore_mission(data["journal"])
                self._send_json(201, {
                    "mission_id": mission.mission_id,
                    "restored_entries": len(mission.journal.entries),
                    "closed": mission.closed_at is not None,
                })
                return

            if len(segments) >= 2 and segments[0] == "missions":
                mission_id = segments[1]
                tail = segments[2:]
                if tail == ["ingest"]:
                    result = self.app.run_locked(
                        mission_id,
                        lambda m: m.ingest(data["subject_id"], data["slice"],
                                           data["received_at"]),
                    )
                    body = {"outcome": result["outcome"],
                            "slice_id": result.get("slice_id"),
                            "finalized_count": len(
                                result.get("finalized", []))}
                    rejection = result.get("rejection")
                    if rejection:
                        body["rejection_code"] = rejection["code"]
                        if rejection.get("changed_fields"):
                            body["changed_fields"] = rejection["changed_fields"]
                    self._send_json(200, body)
                    return
                if tail == ["heartbeat"]:
                    emitted = self.app.run_locked(
                        mission_id, lambda m: m.heartbeat(data["now"]))
                    self._send_json(200, {"finalized_count": len(emitted)})
                    return
                if tail == ["actions"]:
                    entry = self.app.run_locked(mission_id, lambda m: m.record_action(
                        data["subject_id"], data["action"], data["actor"],
                        data["at"], data["role"], data.get("note", ""),
                    ))
                    self._send_json(200, {"journal_seq": entry["seq"]})
                    return
                if tail == ["override"]:
                    entry = self.app.run_locked(mission_id, lambda m: m.override(
                        data["subject_id"], data["medic_id"], data["at"],
                        data["forced_level"], data["reason"],
                    ))
                    self._send_json(200, {"journal_seq": entry["seq"]})
                    return
                if tail == ["resolve"]:
                    entry = self.app.run_locked(mission_id, lambda m: m.resolve(
                        data["subject_id"], data["medic_id"], data["at"],
                        data.get("reason", ""),
                    ))
                    self._send_json(200, {"journal_seq": entry["seq"]})
                    return
                if tail == ["close"]:
                    entry = self.app.run_locked(
                        mission_id, lambda m: m.close(data["at"],
                                                     data.get("by", "值班军医")))
                    self._send_json(200, {"journal_seq": entry["seq"],
                                          "closed_at": self.app.mission(mission_id).closed_at})
                    return
                if tail == ["sync", "export"]:
                    self._send_json(200, self.app.export(mission_id, data["device_id"]))
                    return
                if tail == ["sync", "merge"]:
                    report = self.app.merge(mission_id, data["bundles"])
                    self._send_json(200, report)
                    return

            self.send_error(404)
        except (AppError, ValueError) as exc:
            self._error(exc)
        except KeyError as exc:
            self._error(AppError(f"缺少字段: {exc.args[0]}"))

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--protocol", default=None, help="冻结协议文件路径")
    args = parser.parse_args()

    protocol = FrozenProtocol.load(args.protocol) if args.protocol else FrozenProtocol.load()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        assert protocol.protocol_hash and len(protocol.protocol_hash) == 64
        assert protocol.rules, "冻结协议至少包含一条规则"
        print(f"基础检查通过；协议 {protocol.version} 哈希 {protocol.protocol_hash[:12]}…")
        return

    Handler.app = EdgeApp(protocol)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
