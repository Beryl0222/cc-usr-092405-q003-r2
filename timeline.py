"""离线边缘时间线：乱序到达、重复投递与迟到切片的确定性处理。

核心约定：
- 事件时间以样本 ``t``/切片 ``sampled_at`` 为准归并固定窗口；
- 处理时间由边缘节点在收包/心跳时显式注入（``received_at``/``now``），
  水位线 = 已见最大处理时间 - 允许迟到时长；
- 窗口一旦越过水位线即定稿，评估结果不可变；之后到达的切片只登记为
  迟到拒绝，绝不重算旧窗口；
- 切片身份 = ``slice_id`` + 规范化内容指纹（设备、序号、采样时间与
  **校准前**原始读数的字节哈希）。指纹一致的重复投递幂等丢弃；指纹
  不一致（固件回滚后同编号携带不同读数等）一律登记为
  ``SLICE_CONTENT_CONFLICT`` 并拒绝入窗，绝不静默顶号，也绝不推进
  风险等级；
- 同设备同序号但编号不同的投递按 ``SEQ_CONFLICT`` 拒绝；
- 设备时钟漂移、序列缺口只作为质量事实挂到对应窗口，由引擎扣置信度。
"""

from protocol import ProtocolError, canonical_json, content_hash
from timeutil import align_window, parse_ts

# 参与切片身份指纹的字段：任一变化都意味着不是同一份数据。
# 刻意保留校准前（原始）读数 v：校准在冻结协议下是纯换算，校准前后
# 的等价关系完全确定，而原始读数才是设备实际发出的字节。
# ``observed_at`` 不参与指纹：它是设备时钟质量事实，不是读数内容。
_SLICE_FINGERPRINT_FIELDS = ("device_id", "seq", "sampled_at", "samples")


def slice_fingerprint(slice_ref):
    """计算切片的规范化内容指纹（SHA-256，十六进制）。

    指纹只覆盖设备、序号、采样时间与校准前样本（信号、采样时刻、原始
    读数），不包含 ``observed_at``/``slice_id`` 等信封字段：前者属于
    时钟质量事实，后者是查找键本身。
    """
    body = {key: slice_ref.get(key) for key in _SLICE_FINGERPRINT_FIELDS}
    return content_hash(body)


def _fingerprint_diff_fields(stored_ref, incoming_ref):
    """对比两份切片，返回发生变化的身份字段名（确定性排序）。"""
    changed = []
    for key in _SLICE_FINGERPRINT_FIELDS:
        if canonical_json(stored_ref.get(key)) != canonical_json(
            incoming_ref.get(key)
        ):
            changed.append(key)
    return sorted(changed)


class Timeline:
    def __init__(self, subject_id, protocol, required_signals=None):
        self.subject_id = subject_id
        self.protocol = protocol
        # 任务剖面（高温/高原）在任务开始前随协议一起冻结，决定本任务
        # 哪些信号算“应到未到”。
        self.required_signals = tuple(required_signals or protocol.required_signals)
        self._identities = {}     # slice_id -> {"fingerprint", "slice"}
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

    def _calibrate_samples(self, slice_ref):
        """把切片内全部样本按冻结校准换算（纯换算，不写任何状态）。"""
        device_id = slice_ref.get("device_id")
        window_seconds = self.protocol.window_seconds
        calibrated = []
        for sample in slice_ref.get("samples", []):
            value, cal_id = self.protocol.calibrate(
                device_id, sample["signal"], sample["v"]
            )
            sample_window = align_window(parse_ts(sample["t"]), window_seconds)
            calibrated.append((sample_window, sample["signal"], value, cal_id))
        return calibrated

    def _absorb(self, slice_ref, calibrated):
        """把已通过全部校验的切片写入窗口缓冲与序号册。

        ingest 与重启恢复共用：恢复时身份表已由 adopt_identity 重建，
        这里只补齐窗口内容、漂移事实与窗口内序号归属，使恢复后继续
        定稿的窗口与未重启时逐字节一致。
        """
        device_id = slice_ref.get("device_id")
        seq = slice_ref.get("seq")
        window_seconds = self.protocol.window_seconds
        sampled_at = parse_ts(slice_ref["sampled_at"])
        window_start = align_window(sampled_at, window_seconds)

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
            state["accepted"][seq] = slice_ref.get("slice_id")

        for sample_window, signal, value, cal_id in calibrated:
            bucket = self._bucket(sample_window)
            bucket["samples"].setdefault(signal, []).append({
                "v": value,
                "device_id": device_id,
                "cal_id": cal_id,
            })
        return window_start

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
        maybe_finalize = lambda: self._maybe_finalize(received_at)

        # 身份核验：同 slice_id 必须是字节语义一致的同一份切片。
        # - 指纹一致：重复投递，完全幂等（不产生任何副作用），
        #   但仍借这次到达推进水位线，保证定稿延迟有界；
        # - 指纹不一致：固件回滚/重传损坏导致同编号异内容，登记冲突、
        #   拒绝入窗，且不占用序号、不写任何窗口状态——冲突不是读数，
        #   永远不参与评分与风险推进。
        known = self._identities.get(slice_id)
        if known is not None:
            fingerprint = slice_fingerprint(slice_ref)
            if fingerprint == known["fingerprint"]:
                return {
                    "outcome": "duplicate",
                    "slice_id": slice_id,
                    "fingerprint": fingerprint,
                    "detail": "切片已接收过，重复投递被忽略",
                    "finalized": maybe_finalize(),
                }
            changed = _fingerprint_diff_fields(known["slice"], slice_ref)
            rejection = self._record_rejection(
                slice_ref, "SLICE_CONTENT_CONFLICT",
                f"切片 {slice_id} 与已接收内容指纹不一致，差异字段: {changed}",
                received_at,
                extra={
                    "changed_fields": changed,
                    "stored_fingerprint": known["fingerprint"],
                    "incoming_fingerprint": fingerprint,
                    # 仅登记身份字段，不重复整包，避免日志被重放撑大。
                    "stored_identity": {
                        key: known["slice"].get(key)
                        for key in _SLICE_FINGERPRINT_FIELDS
                    },
                    "incoming_identity": {
                        key: slice_ref.get(key)
                        for key in _SLICE_FINGERPRINT_FIELDS
                    },
                },
            )
            return {
                "outcome": "conflict",
                "slice_id": slice_id,
                "rejection": rejection,
                # 冲突切片不入窗，但本次到达仍属处理时间推进，
                # 定稿延迟上界不因冲突而失效。
                "finalized": maybe_finalize(),
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
                "finalized": maybe_finalize(),
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
                "finalized": maybe_finalize(),
            }

        # 同设备同序号但 slice_id 不同：疑似重发冲突，拒绝顶号。
        state = self._device_seq.get(device_id)
        if seq is not None and state is not None and seq in state["accepted"]:
            return {
                "outcome": "rejected",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "SEQ_CONFLICT",
                    f"设备 {device_id} 序号 {seq} 已由切片 "
                    f"{state['accepted'][seq]} 占用", received_at,
                    extra={"occupied_by": state["accepted"][seq]},
                ),
                "finalized": [],
            }

        # 校准校验必须在任何状态写入之前完成：任一信号未登记/未校准，
        # 整切片拒绝且不留下副作用。
        try:
            calibrated = self._calibrate_samples(slice_ref)
        except ProtocolError as exc:
            return {
                "outcome": "rejected",
                "slice_id": slice_id,
                "rejection": self._record_rejection(
                    slice_ref, "UNCALIBRATED_SIGNAL", str(exc), received_at
                ),
                "finalized": [],
            }

        fingerprint = slice_fingerprint(slice_ref)
        window_start = self._absorb(slice_ref, calibrated)
        self._identities[slice_id] = {
            "fingerprint": fingerprint,
            "slice": dict(slice_ref),
        }

        return {
            "outcome": "accepted",
            "slice_id": slice_id,
            "window_start": window_start,
            "fingerprint": fingerprint,
            "finalized": maybe_finalize(),
        }

    def adopt_identity(self, slice_ref, fingerprint, received_at=None):
        """登记一条已在其他节点接受的切片身份（合并/重启回放用）。

        内容原样在册但不重新入窗、不重算评估；若与本节点已有身份冲突，
        抛出 :class:`TimelineConflict`，由调用方按整包拒绝处理。返回
        ``True`` 表示新登记，``False`` 表示指纹一致的重复身份。
        """
        slice_id = slice_ref.get("slice_id")
        known = self._identities.get(slice_id)
        if known is not None:
            if known["fingerprint"] != fingerprint:
                raise TimelineConflict(slice_id, known["fingerprint"], fingerprint)
            return False
        self._identities[slice_id] = {
            "fingerprint": fingerprint,
            "slice": dict(slice_ref),
        }
        seq = slice_ref.get("seq")
        device_id = slice_ref.get("device_id")
        if seq is not None and device_id is not None:
            state = self._device_seq.setdefault(device_id, {"accepted": {}})
            # 后到的回放不顶占已有序号；同序号异编号已由内容冲突路径覆盖。
            state["accepted"].setdefault(seq, slice_id)
        if received_at is not None:
            received_at = float(parse_ts(received_at))
            self._max_received = (
                received_at
                if self._max_received is None
                else max(self._max_received, received_at)
            )
        return True

    def adopt_slice(self, slice_ref, fingerprint, received_at=None):
        """登记一条他处已接受的切片并在需要时放回开放窗口（合并/恢复用）。

        - 身份已在册且指纹一致：完全幂等，返回 False，绝不重复加样本；
        - 身份冲突：抛 :class:`TimelineConflict`，由调用方整体拒绝；
        - 新身份：登记身份与全局序号册；当其采样窗口尚未定稿时重新校准
          并入窗（使本节点能继续定稿），已定稿窗口则只登记身份，旧评估
          保持不可变。
        """
        is_new = self.adopt_identity(slice_ref, fingerprint, received_at)
        if not is_new:
            return False
        window_start = align_window(
            parse_ts(slice_ref["sampled_at"]), self.protocol.window_seconds
        )
        if any(
            record["assessment"]["input_window"]["start"] == window_start
            for record in self.finalized
        ):
            return True
        calibrated = self._calibrate_samples(slice_ref)
        self._absorb(slice_ref, calibrated)
        return True

    def rebuild_gap_cursor(self):
        """重启恢复后重建缺口游标。

        游标语义为“已定稿窗口内各设备的最大已收序号”：恢复时按身份表
        中采样窗口已定稿的切片重算，与未重启时的取值完全一致，保证
        恢复后首个新窗口的缺口判定不漂移。
        """
        finalized_starts = {
            record["assessment"]["input_window"]["start"]
            for record in self.finalized
        }
        cursor = {}
        for identity in self._identities.values():
            slice_ref = identity["slice"]
            seq = slice_ref.get("seq")
            device_id = slice_ref.get("device_id")
            if seq is None or device_id is None:
                continue
            start = align_window(
                parse_ts(slice_ref["sampled_at"]), self.protocol.window_seconds
            )
            if start in finalized_starts:
                cursor[device_id] = max(cursor.get(device_id, seq), seq)
        self._gap_cursor = cursor

    def advance(self, now):
        """处理时间心跳：没有新切片时也能推动水位线，保证延迟有界。"""
        now = float(parse_ts(now))
        self._max_received = (
            now if self._max_received is None else max(self._max_received, now)
        )
        return self._maybe_finalize(now)

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


class TimelineConflict(ValueError):
    """同一 slice_id 在不同节点携带不同内容指纹，合并必须整体拒绝。"""

    def __init__(self, slice_id, stored_fingerprint, incoming_fingerprint):
        self.slice_id = slice_id
        self.stored_fingerprint = stored_fingerprint
        self.incoming_fingerprint = incoming_fingerprint
        super().__init__(
            f"切片 {slice_id} 内容指纹跨节点不一致: "
            f"{stored_fingerprint[:12]}… ≠ {incoming_fingerprint[:12]}…"
        )
