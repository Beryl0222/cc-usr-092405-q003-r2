"""离线边缘时间线：乱序到达、重复投递与迟到切片的确定性处理。

核心约定：
- 事件时间以样本 ``t``/切片 ``sampled_at`` 为准归并固定窗口；
- 处理时间由边缘节点在收包/心跳时显式注入（``received_at``/``now``），
  水位线 = 已见最大处理时间 - 允许迟到时长；
- 窗口一旦越过水位线即定稿，评估结果不可变；之后到达的切片只登记为
  迟到拒绝，绝不重算旧窗口；
- 同一 ``slice_id`` 且设备、序号、采样时间与全部校准前读数逐字节语义
  一致的重复投递幂等丢弃；同 ``slice_id`` 但上述任一内容不同的投递
  （固件回滚会复用旧编号携带新读数）登记 ``SLICE_CONFLICT`` 并拒绝
  进入窗口，已接收版本与风险等级都不被它推进；
- 同一设备同序号换切片投递拒绝，防止旧数据被“新副本”顶掉；
- 设备时钟漂移、序列缺口只作为质量事实挂到对应窗口，由引擎扣置信度。
"""

import copy

from protocol import ProtocolError, content_hash
from timeutil import align_window, parse_ts


def slice_semantics(slice_ref):
    """提取切片参与判重/判冲突的语义内容并规范化。

    只覆盖决定窗口内容的字段：设备、序号、采样时刻与全部校准前读数。
    时间统一解析为纪元秒、读数统一为浮点，传输层的格式差异不会造成误判。
    """
    return {
        "device_id": slice_ref.get("device_id"),
        "seq": slice_ref.get("seq"),
        "sampled_at": float(parse_ts(slice_ref["sampled_at"])),
        "samples": [
            {
                "signal": sample["signal"],
                "t": float(parse_ts(sample["t"])),
                "v": float(sample["v"]),
            }
            for sample in slice_ref.get("samples", [])
        ],
    }


def slice_diff(established, received):
    """按固定顺序列出两份语义内容不一致的字段，保证输出确定。"""
    return [
        field
        for field in ("device_id", "seq", "sampled_at", "samples")
        if established[field] != received[field]
    ]


class Timeline:
    def __init__(self, subject_id, protocol, required_signals=None):
        self.subject_id = subject_id
        self.protocol = protocol
        # 任务剖面（高温/高原）在任务开始前随协议一起冻结，决定本任务
        # 哪些信号算“应到未到”。
        self.required_signals = tuple(required_signals or protocol.required_signals)
        self._slice_index = {}   # slice_id -> 已接收切片的语义内容（判重/判冲突）
        self._device_seq = {}
        self._open = {}          # window_start -> 窗口缓冲
        self.finalized = []      # 已定稿评估（含 emitted_at），不可变
        self.rejected = []       # 迟到/冲突/非法切片登记
        self._max_received = None
        self._gap_cursor = {}    # device_id -> 已归因到窗口的最大序号

    def _watermark(self, now):
        if self._max_received is None:
            return None
        return self._max_received - self.protocol.allowed_lateness_seconds

    @staticmethod
    def _blank_window(window_start, required_signals):
        return {
            "window_start": window_start,
            "samples": {},
            "quality": {"drifts": [], "seq_gaps": []},
            "required_signals": tuple(required_signals),
            "slice_seqs": {},   # device_id -> 落入本窗口的切片序号集合
        }

    def _bucket(self, window_start):
        if window_start not in self._open:
            self._open[window_start] = self._blank_window(
                window_start, self.required_signals
            )
        return self._open[window_start]

    def _record_rejection(self, slice_ref, code, detail, received_at, extra=None):
        record = {
            "code": code,
            "slice_id": slice_ref.get("slice_id"),
            "device_id": slice_ref.get("device_id"),
            "seq": slice_ref.get("seq"),
            "detail": detail,
            "received_at": received_at,
        }
        if extra:
            record.update(extra)
        self.rejected.append(record)
        return record

    def ingest(self, slice_ref, received_at):
        """接收一个切片。返回处理结果与本次新定稿的窗口列表。"""
        received_at = float(parse_ts(received_at))
        self._max_received = (
            received_at
            if self._max_received is None
            else max(self._max_received, received_at)
        )

        slice_id = slice_ref.get("slice_id")
        device_id = slice_ref.get("device_id")
        seq = slice_ref.get("seq")

        # 同 slice_id 再次到达：逐字段比对语义内容。
        # 内容一致：完全幂等，不产生任何副作用，但仍借这次到达推进水位线，
        # 保证定稿延迟有界；内容不一致（固件回滚会复用旧编号携带新读数）：
        # 登记 SLICE_CONFLICT 并拒绝进入窗口，已接收版本与风险等级都不被
        # 这次投递推进。
        established = self._slice_index.get(slice_id)
        if established is not None:
            received = slice_semantics(slice_ref)
            if received == established:
                return {
                    "outcome": "duplicate",
                    "slice_id": slice_id,
                    "detail": "切片已接收过，重复投递被忽略",
                    "finalized": self._maybe_finalize(received_at),
                }
            diff = slice_diff(established, received)
            return {
                "outcome": "rejected",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "SLICE_CONFLICT",
                    f"切片 {slice_id} 编号已登记，但本次投递的 "
                    f"{','.join(diff)} 与已接收内容不一致，"
                    f"疑似固件回滚或缓存损坏，拒绝进入窗口",
                    received_at,
                    extra={
                        "diff": diff,
                        "expected_hash": content_hash(established),
                        "got_hash": content_hash(received),
                        "established": established,
                        "received": received,
                    },
                ),
                "finalized": [],
            }

        # 设备未登记：拒绝，不允许未经校准的数据进入时间线。
        if not self.protocol.is_registered(device_id):
            return {
                "outcome": "rejected",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "UNKNOWN_DEVICE",
                    f"设备 {device_id} 不在冻结协议中", received_at,
                ),
                "finalized": self._maybe_finalize(received_at),
            }

        sampled_at = parse_ts(slice_ref["sampled_at"])
        window_seconds = self.protocol.window_seconds
        window_start = align_window(sampled_at, window_seconds)

        # 迟到：窗口已定稿，旧判断不可重写。
        if any(
            record["assessment"]["input_window"]["start"] == window_start
            for record in self.finalized
        ):
            return {
                "outcome": "late",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "LATE_AFTER_FINALIZED",
                    f"窗口 {window_start} 已定稿，迟到切片不参与重算", received_at,
                ),
                "finalized": self._maybe_finalize(received_at),
            }

        # 同设备同序号但 slice_id 不同：疑似重发冲突，拒绝顶号。
        state = self._device_seq.get(device_id)
        if seq is not None and state is not None and seq in state["accepted"]:
            return {
                "outcome": "rejected",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "SEQ_CONFLICT",
                    f"设备 {device_id} 序号 {seq} 已由其他切片占用", received_at,
                ),
                "finalized": [],
            }

        # 校准校验必须在任何状态写入之前完成：任一信号未登记/未校准，
        # 整切片拒绝且不留下副作用。
        try:
            calibrated = []
            for sample in slice_ref.get("samples", []):
                value, cal_id = self.protocol.calibrate(
                    device_id, sample["signal"], sample["v"]
                )
                sample_window = align_window(parse_ts(sample["t"]), window_seconds)
                calibrated.append((sample_window, sample["signal"], value, cal_id))
        except ProtocolError as exc:
            return {
                "outcome": "rejected",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "UNCALIBRATED_SIGNAL", str(exc), received_at
                ),
                "finalized": [],
            }

        # 时钟漂移：设备时钟与采样基准偏差超过冻结限值。
        observed_at = parse_ts(slice_ref["observed_at"])
        drift_ms = abs(observed_at - sampled_at) * 1000.0
        if drift_ms > self.protocol.clock_drift_limit_ms:
            self._bucket(window_start)["quality"]["drifts"].append({
                "device_id": device_id,
                "observed_at": slice_ref["observed_at"],
                "sampled_at": slice_ref["sampled_at"],
                "drift_ms": round(drift_ms, 1),
                "limit_ms": self.protocol.clock_drift_limit_ms,
            })

        # 记录切片归属，序列缺口推迟到定稿时统一判定，
        # 使“离线缓存后乱序送达”不会被误判成丢片。
        if seq is not None:
            bucket = self._bucket(window_start)
            bucket["slice_seqs"].setdefault(device_id, set()).add(seq)
            state = self._device_seq.setdefault(
                device_id, {"accepted": {}}
            )
            state["accepted"][seq] = slice_id

        self._slice_index[slice_id] = slice_semantics(slice_ref)
        for sample_window, signal, value, cal_id in calibrated:
            bucket = self._bucket(sample_window)
            bucket["samples"].setdefault(signal, []).append({
                "v": value,
                "device_id": device_id,
                "cal_id": cal_id,
            })

        return {
            "outcome": "accepted",
            "slice_id": slice_id,
            "window_start": window_start,
            "finalized": self._maybe_finalize(received_at),
        }

    def advance(self, now):
        """处理时间心跳：没有新切片时也能推动水位线，保证延迟有界。"""
        now = float(parse_ts(now))
        self._max_received = (
            now if self._max_received is None else max(self._max_received, now)
        )
        return self._maybe_finalize(now)

    def note_merged_rejection(self, rejection):
        """回连合并时回放他节点的拒绝登记。

        冲突登记携带着已接收版本的语义内容，本节点据此重建切片指纹，
        使合并（或任务重建）之后对同一编号切片的重复/冲突判断与首次
        接收节点一致；已登记内容以先到者为准，绝不被后来的合并包顶掉。
        """
        record = copy.deepcopy(rejection)
        self.rejected.append(record)
        if record.get("code") == "SLICE_CONFLICT":
            established = record.get("established")
            slice_id = record.get("slice_id")
            if established is not None and slice_id is not None:
                self._slice_index.setdefault(slice_id, established)

    def _attribute_gaps(self, buffer):
        """窗口定稿时，按全局已收序号确定性判定真实丢片。

        乱序送达的切片此刻都已入册，因此“离线缓存造成的乱序”不会被误判；
        只有在后续切片已到、而中间序号确实从未出现时才登记缺口。
        """
        for device_id, present_in_window in buffer["slice_seqs"].items():
            max_present = max(present_in_window)
            cursor = self._gap_cursor.get(device_id)
            if cursor is None:
                # 首个有数据的窗口，首序号之前的历史不臆断为缺口。
                self._gap_cursor[device_id] = max_present
                continue
            accepted = self._device_seq[device_id]["accepted"]
            missing = [
                seq for seq in range(cursor + 1, max_present + 1)
                if seq not in accepted
            ]
            if missing:
                buffer["quality"]["seq_gaps"].append({
                    "device_id": device_id,
                    "missing_seq": missing,
                    "detail": (
                        f"设备 {device_id} 序号 {cursor} 之后至 {max_present} "
                        f"间缺失 {missing}"
                    ),
                })
            self._gap_cursor[device_id] = max(max_present, cursor)

    def _maybe_finalize(self, now):
        watermark = self._watermark(now)
        if watermark is None:
            return []
        due = [
            start
            for start in self._open
            if start + self.protocol.window_seconds <= watermark
        ]
        finalized_now = []
        for start in sorted(due):
            buffer = self._open.pop(start)
            self._attribute_gaps(buffer)
            from engine import evaluate_window
            assessment = evaluate_window(buffer, self.protocol)
            record = {
                "subject_id": self.subject_id,
                "emitted_at": float(now),
                "assessment": assessment,
            }
            # 按窗口起点有序插入，定稿顺序只取决于事件时间。
            self.finalized.append(record)
            self.finalized.sort(key=lambda item: item["assessment"]["input_window"]["start"])
            finalized_now.append(record)
        finalized_now.sort(key=lambda item: item["assessment"]["input_window"]["start"])
        return finalized_now

    def levels(self):
        """供回放比对：只取与处理无关的评估序列。"""
        return [record["assessment"] for record in self.finalized]
