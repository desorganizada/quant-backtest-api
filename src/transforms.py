import re
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple

# ============================================================
# GENERIC SAFE PARSE HELPERS (kept)
# ============================================================

def safe_float(value: str):
    try:
        return float(value)
    except Exception:
        return None

def safe_int(value: str):
    try:
        return int(value)
    except Exception:
        return None

def _strip_quotes(s: str) -> str:
    s = str(s).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1].strip()
    return s

def _coerce_number_or_bool(s: str):
    """
    Conservative coercion:
      - True/False -> bool
      - int-like -> int
      - float/decimal/scientific -> float
      - null/None/nan/empty -> None
      - else -> original string
    """
    if s is None:
        return None
    s = _strip_quotes(s)
    if s == "" or s.lower() in ("none", "null", "nan"):
        return None
    sl = s.lower()
    if sl == "true":
        return True
    if sl == "false":
        return False

    if re.fullmatch(r"-?\d+", s):
        try:
            return int(s)
        except Exception:
            pass

    if re.fullmatch(r"-?\d+(\.\d+)?([eE]-?\d+)?", s):
        try:
            return float(s)
        except Exception:
            pass

    return s

def camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()

# ============================================================
# TIMESTAMP HELPERS
# ============================================================

TS_PREFIX = re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{6})\s+\[")

def epoch_ms_dubai(ts: str) -> Optional[int]:
    """
    Old Data ETL uses a Dubai(+04) epoch-ms derived from timestamp.
    Here we keep it stable by interpreting as naive local and converting to UTC.
    """
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        dt = dt.replace(tzinfo=timezone(timedelta(hours=4)))
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)
    except Exception:
        return None

def ts_to_ms3(ts: str) -> str:
    """
    Data-ETL parity:
      - timestamp formatted with 3 decimals (milliseconds)
      - milliseconds are **rounded** (not truncated) from microseconds.
    """
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        ms = int(round(dt.microsecond / 1000.0))
        if ms >= 1000:
            dt = dt.replace(microsecond=0) + timedelta(seconds=1)
            ms = 0
        return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
    except Exception:
        return ts

def ts_to_ms3_trunc(ts: str) -> str:
    """
    Data-ETL parity for xBots indicators:
      truncate microseconds -> milliseconds (no rounding).
    This stabilizes merging of A/B indicator lines that can fall on adjacent ms when rounded.
    """
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        ms = int(dt.microsecond / 1000)  # TRUNC
        return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
    except Exception:
        return ts

# ============================================================
# ORDERS (legacy kept) + FINAL (old Data ETL parity)
# ============================================================

REST_LIMIT_ORDER_RX = re.compile(
    r"Rest Limit Order\s+\d+:\s+(?P<side>BUY|SELL)\s+price=(?P<price>[0-9.]+)\s+qty=(?P<qty>[0-9.]+)\s+clOrdId=\"(?P<clOrdId>[^\"]+)\"",
    re.IGNORECASE,
)

SENDING_LIMIT_RX = re.compile(
    r"Sending Limit Order\s+\d+:\s+(?P<side>BUY|SELL)\s+price=(?P<price>[0-9.]+)\s+qty=(?P<qty>[0-9.]+)\s+clOrdId=\"(?P<clOrdId>[^\"]+)\"",
    re.IGNORECASE,
)

CANCEL_ORDER_RX = re.compile(r"CancelOrder.*?clOrdId=\"(?P<clOrdId>[^\"]+)\"", re.IGNORECASE)

def _grid_from_clordid(clordid: Optional[str]) -> Optional[int]:
    """
    Data-ETL parity:
      grid_number comes from the strategy layer ("strataN") embedded in clOrdId.
      Example: CBROBTCxstrata1x999 -> grid_number = 1
    """
    if not clordid:
        return None
    m = re.search(r"(?:^|x)strata(\d+)(?:x|$)", clordid, re.IGNORECASE)
    if m:
        return safe_int(m.group(1))
    return None

def parse_orders(lines: List[str]) -> List[Dict]:
    """
    Legacy output kept.
    """
    rows = []
    for line in lines:
        ts_m = TS_PREFIX.search(line)
        ts = ts_m.group("ts") if ts_m else None

        m1 = REST_LIMIT_ORDER_RX.search(line)
        if m1:
            g = m1.groupdict()
            rows.append({
                "timestamp": ts,
                "event": "rest_limit",
                "order_id": None,
                "clOrdId": g.get("clOrdId"),
                "side": (g.get("side") or "").upper(),
                "price": safe_float(g.get("price", "")),
                "qty": safe_float(g.get("qty", "")),
                "raw": line.strip(),
            })
            continue

        m2 = SENDING_LIMIT_RX.search(line)
        if m2:
            g = m2.groupdict()
            rows.append({
                "timestamp": ts,
                "event": "sending_limit",
                "order_id": None,
                "clOrdId": g.get("clOrdId"),
                "side": (g.get("side") or "").upper(),
                "price": safe_float(g.get("price", "")),
                "qty": safe_float(g.get("qty", "")),
                "raw": line.strip(),
            })
            continue

        m3 = CANCEL_ORDER_RX.search(line)
        if m3:
            g = m3.groupdict()
            rows.append({
                "timestamp": ts,
                "event": "cancel",
                "order_id": None,
                "clOrdId": g.get("clOrdId"),
                "side": None,
                "price": None,
                "qty": None,
                "raw": line.strip(),
            })
            continue

    return rows

def parse_orders_final(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Data-ETL parity schema:
      timestamp, grid_number, side, price, quantity, clOrdId

    IMPORTANT (parity):
      - Only "Rest Limit Order ..." lines generate rows.
      - "Sending Limit Order ..." lines are ignored to avoid double-counting.
    """
    out: List[Dict[str, Any]] = []
    for line in lines:
        m = REST_LIMIT_ORDER_RX.search(line)
        if not m:
            continue

        g = m.groupdict()
        ts_m = TS_PREFIX.search(line)
        ts = ts_m.group("ts") if ts_m else None
        cl = g.get("clOrdId")
        out.append({
            "timestamp": ts,
            "grid_number": _grid_from_clordid(cl),
            "side": (g.get("side") or "").upper(),
            "price": safe_float(g.get("price") or ""),
            "quantity": safe_float(g.get("qty") or ""),
            "clOrdId": cl,
        })
    return out

# ============================================================
# DURATION STATISTICS (legacy kept) + FINAL (tolerant parity)
# ============================================================

DURATION_REGEX_LEGACY = re.compile(
    r"requestId=(?P<request_id>\d+).*?"
    r"latencyMs=(?P<latency>[0-9.]+)",
    re.IGNORECASE,
)

def parse_duration_statistics(lines: List[str]) -> List[Dict]:
    rows = []
    for line in lines:
        match = DURATION_REGEX_LEGACY.search(line)
        if not match:
            continue
        rows.append(match.groupdict())
    return rows

def parse_duration_statistics_final(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Data-ETL parity with the legacy extractor used for the old/correct CSV.

    Real log example:
      2025-05-12 13:25:20.525256 [WRN] [OKXMarket]
      HttpRoundTrip:[2,NEW:-1] Duration 41.4 ms, d2d = 15.008
      tq:6.24 to:11.37 oq: 3.64 ms,
      tick_ts=1837092847243356,ord_ts=1837092858610744,deque_ts=1837092862251305.
      clOrdId:"CBROBTCxstrata1x2"

    Notes:
      - field names are not homogeneous (Duration / d2d = / tq: / ord_ts / clOrdId:)
      - we must accept both the old and the newer spellings when present
      - output schema must stay exactly:
        timestamp, request_type, duration, d2d, tq, to, oq,
        tick_ts, order_ts, deque_ts, clOrdId
    """
    out: List[Dict[str, Any]] = []

    line_rx = re.compile(
        r"^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{6}).*?"
        r"HttpRoundTrip:\[\d+,(?P<request_type>NEW|CANCEL|AMEND|MODIFY|REPLACE)"
        r"(?::[^\]]*)?\].*?"
        r"Duration\s+(?P<duration>-?\d+(?:\.\d+)?)\s+ms,\s*"
        r"d2d\s*=\s*(?P<d2d>-?\d+(?:\.\d+)?)\s+"
        r"tq:(?P<tq>-?\d+(?:\.\d+)?)\s+"
        r"to:(?P<to>-?\d+(?:\.\d+)?)\s+"
        r"oq:\s*(?P<oq>-?\d+(?:\.\d+)?)\s+ms,"
        r"tick_ts=(?P<tick_ts>-?\d+),"
        r"(?:ord_ts|order_ts)=(?P<order_ts>-?\d+),"
        r"deque_ts=(?P<deque_ts>-?\d+)\.?\s*"
        r"clOrdId:(?:\"(?P<clord_quoted>[^\"]+)\"|(?P<clord_plain>[^,\s]+))",
        re.IGNORECASE,
    )

    for line in lines:
        if "HttpRoundTrip:[" not in line:
            continue

        m = line_rx.search(line)
        if not m:
            continue

        g = m.groupdict()
        clord = g.get("clord_quoted") or g.get("clord_plain")

        out.append({
            "timestamp": g.get("timestamp"),
            "request_type": (g.get("request_type") or "").upper() or None,
            "duration": safe_float(g.get("duration") or ""),
            "d2d": safe_float(g.get("d2d") or ""),
            "tq": safe_float(g.get("tq") or ""),
            "to": safe_float(g.get("to") or ""),
            "oq": safe_float(g.get("oq") or ""),
            "tick_ts": safe_int(g.get("tick_ts") or ""),
            "order_ts": safe_int(g.get("order_ts") or ""),
            "deque_ts": safe_int(g.get("deque_ts") or ""),
            "clOrdId": clord,
        })

    return out

# ============================================================
# EXECUTION INDICATORS (legacy kept) + FINAL (merge parity)
# ============================================================

EXEC_IND_EXPECTED = [
    "timestamp",
    "central_price",
    "ewma_price",
    "ewma_deviation",
    "forward_mid",
    "position_bias",
    "forward_high_order_price",
    "forward_low_order_price",
    "prev_target_position",
    "target_position",
    "aep_adjustment_high",
    "aep_adjustment_low",
    "avg_entry_price",
    "bar_height",
    "sell_rate",
    "buy_rate",
    "sell_trade",
    "sell_trade_price",
    "buy_trade",
    "buy_trade_price",
]

EXEC_IND_MAP = {
    "centralprice": "central_price",
    "ewmaprice": "ewma_price",
    "ewmadeviation": "ewma_deviation",
    "forwardmid": "forward_mid",
    "positionbias": "position_bias",
    "forwardhighorderprice": "forward_high_order_price",
    "forwardloworderprice": "forward_low_order_price",
    "prevtargetposition": "prev_target_position",
    "targetposition": "target_position",
    "aepadjustmenthigh": "aep_adjustment_high",
    "aepadjustmentlow": "aep_adjustment_low",
    "avgentryprice": "avg_entry_price",
    "barheight": "bar_height",
    "sellrate": "sell_rate",
    "buyrate": "buy_rate",
    "selltrade": "sell_trade",
    "selltradeprice": "sell_trade_price",
    "buytrade": "buy_trade",
    "buytradeprice": "buy_trade_price",
}

def _parse_kv_comma_tail(tail: str) -> Dict[str, str]:
    kv: Dict[str, str] = {}
    for part in tail.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        kv[k.strip()] = v.strip()
    return kv

def _ts_to_ms3_half_up(ts: str) -> str:
    """
    Excel/Data-ETL parity for Execution Indicators timestamps:
    round microseconds to milliseconds using HALF-UP semantics
    (0.500 rounds up). Python's built-in round uses bankers rounding,
    which is why values such as .752500 were becoming .752 instead of .753.
    """
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        ms = int((dt.microsecond + 500) // 1000)
        if ms >= 1000:
            dt = dt.replace(microsecond=0) + timedelta(seconds=1)
            ms = 0
        return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
    except Exception:
        return ts

def _coerce_execution_indicator_value(v: Any):
    """
    Execution Indicators must preserve the numeric text exactly as it appears
    in the log so downstream CSV exports keep the same precision as the
    legacy/old correct extractor.

    Rules:
      - int-like -> int
      - decimal / float / scientific -> original STRING
      - True/False -> bool
      - None/null/empty -> None
      - everything else -> stripped string
    """
    if v is None:
        return None

    s = str(v).strip()

    if s == "" or s.lower() in ("none", "null", "nan"):
        return None
    if s == "True":
        return True
    if s == "False":
        return False

    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return int(s)
        except Exception:
            return s

    try:
        Decimal(s)
        return s
    except Exception:
        return s

def execution_indicators(lines: List[str]) -> List[Dict]:
    """
    Legacy kept.
    """
    rows = []
    for line in lines:
        ts_m = TS_PREFIX.search(line)
        ts = ts_m.group("ts") if ts_m else None
        rows.append({"timestamp": ts, "raw": line.strip()})
    return rows

def parse_execution_indicators_final(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Data-ETL parity with the old/correct Execution Indicators CSV:

      - Merge the 2 consecutive log lines into 1 row:
          A) centralPrice / ewma / forward* ...
          B) barHeight / sellRate / buyRate / trades ...
      - Use the LAST "] " marker, not the first one. The log prefix contains
        two bracket blocks ("[INF] [CB_RO]"), and using the first one causes
        keys like "[CB_RO] centralPrice", which is why central_price and
        bar_height were coming out empty.
      - Use rounded milliseconds (ts_to_ms3), matching the legacy CSV.
      - Preserve decimal/scientific values as strings so the exported CSV keeps
        the original precision and does not get shortened by float coercion.
    """
    out: List[Dict[str, Any]] = []
    pending: Optional[Dict[str, Any]] = None

    def _new_rec(ts3: Optional[str]) -> Dict[str, Any]:
        rec: Dict[str, Any] = {"timestamp": ts3}
        for k in EXEC_IND_EXPECTED:
            if k == "timestamp":
                continue
            rec[k] = None
        return rec

    def _apply_kv(rec: Dict[str, Any], kv: Dict[str, str]) -> None:
        for k_raw, v_raw in (kv or {}).items():
            k_norm = k_raw.strip()
            k_low = k_norm.lower()

            if k_low in EXEC_IND_MAP:
                k_final = EXEC_IND_MAP[k_low]
            else:
                k_final = camel_to_snake(k_norm)

            if k_final in EXEC_IND_EXPECTED:
                rec[k_final] = _coerce_execution_indicator_value(v_raw)

    for line in lines:
        if "=" not in line:
            continue

        ts_m = TS_PREFIX.search(line)
        ts = ts_m.group("ts") if ts_m else None
        ts3 = _ts_to_ms3_half_up(ts) if ts else None

        idx = line.rfind("] ")
        tail = line[idx + 2:] if idx >= 0 else line
        tail = tail.strip()
        if not tail:
            continue

        kv = _parse_kv_comma_tail(tail)
        if not kv:
            continue

        tail_lower = tail.lower()
        is_header_line = ("centralprice" in tail_lower)

        is_supplemental_line = ("barheight" in tail_lower)

        if is_header_line:
            if pending is not None:
                out.append(pending)
            pending = _new_rec(ts3)
            _apply_kv(pending, kv)
            continue

        if pending is not None and is_supplemental_line:
            _apply_kv(pending, kv)

    if pending is not None:
        out.append(pending)

    return out

# ============================================================
# STRATEGY INDICATORS (legacy kept) + FINAL (robust parity)
# ============================================================

STRAT_IND_EXPECTED = [
    "trend_strength_value",
    "trend_strength_ewma",
    "trend_strength_double_ewma",
    "trend_strength_double_ewma_slope",
    "buildup_in_progress",
    "reduction_in_progress",
    "buildup_step_size",
    "reduction_step_size",
    "position_multiplier",
    "target_position",
    "slope_sign_count",
    "trend_status",
]

def parse_strategy_indicators(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Compatibilidade retroativa:
      - mantém o nome legado esperado pelo servidor
      - mas retorna o schema correto do Strategy Indicators
      - usa a mesma lógica robusta do extractor final
    """
    return parse_strategy_indicators_final(lines)

def _normalize_trend_status(v) -> str:
    """
    trend_status deve sempre sair como STRING:
      - sem dado -> "None"
      - buildup -> "BuildUp"
      - reduction -> "Reduction"
      - qualquer valor inválido -> "None"
    """
    if v is None:
        return "None"

    s = _strip_quotes(str(v))

    if s == "" or s.lower() in ("none", "null", "nan"):
        return "None"

    sl = s.lower()

    if sl in ("reduction", "reduce", "reduçao", "redução", "reducao"):
        return "Reduction"

    if sl in ("buildup", "build_up", "build-up", "build up", "beatup", "betup"):
        return "BuildUp"

    return "None"

_KV_RX = re.compile(
    r"(?P<k>[A-Za-z][A-Za-z0-9_]*)\s*=\s*(?P<v>\"[^\"]*\"|'[^']*'|[^,]+)"
)

def _parse_kv_regex(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in _KV_RX.finditer(text):
        k = (m.group("k") or "").strip()
        v = (m.group("v") or "").strip()
        if k:
            out[k] = v
    return out

def _is_number_like_str(s: str) -> bool:
    s = s.strip()
    return bool(re.fullmatch(r"-?\d+(\.\d+)?([eE]-?\d+)?", s))

def parse_strategy_indicators_final(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Data-ETL parity for Strategy Indicators.

    Suporta os 2 formatos reais encontrados nos logs:
      1) Posicional (o formato antigo/correto):
         TrendStrength=_trendStrength,_trendStrength.Ewma,...,TrendStatus
         TrendStrength=0,0,0,0,False,False,1,1,0,0.00,0,None

      2) key=value explícito:
         trend_strength_value=...,trend_strength_ewma=...,...

    Regras de compatibilidade:
      - mantém timestamp em ms truncado
      - não deduplica por timestamp
      - trend_status fica normalizado em BuildUp / Reduction / None
      - ignora linhas-header/token como _trendStrength
    """
    out: List[Dict[str, Any]] = []

    key_map = {
        "trendstrength": "trend_strength_value",
        "trendstrengthewma": "trend_strength_ewma",
        "trendstrengthdoubleewma": "trend_strength_double_ewma",
        "trendstrengthdoubleewmaslope": "trend_strength_double_ewma_slope",
        "buildupinprogress": "buildup_in_progress",
        "reductioninprogress": "reduction_in_progress",
        "buildupstepsize": "buildup_step_size",
        "reductionstepsize": "reduction_step_size",
        "positionmultiplier": "position_multiplier",
        "targetposition": "target_position",
        "slopesigncount": "slope_sign_count",
        "trendstatus": "trend_status",
        "trend_strength_value": "trend_strength_value",
        "trend_strength_ewma": "trend_strength_ewma",
        "trend_strength_double_ewma": "trend_strength_double_ewma",
        "trend_strength_double_ewma_slope": "trend_strength_double_ewma_slope",
        "buildup_in_progress": "buildup_in_progress",
        "reduction_in_progress": "reduction_in_progress",
        "buildup_step_size": "buildup_step_size",
        "reduction_step_size": "reduction_step_size",
        "position_multiplier": "position_multiplier",
        "target_position": "target_position",
        "slope_sign_count": "slope_sign_count",
        "trend_status": "trend_status",
    }

    positional_columns = [
        "trend_strength_value",
        "trend_strength_ewma",
        "trend_strength_double_ewma",
        "trend_strength_double_ewma_slope",
        "buildup_in_progress",
        "reduction_in_progress",
        "buildup_step_size",
        "reduction_step_size",
        "position_multiplier",
        "target_position",
        "slope_sign_count",
        "trend_status",
    ]

    def _empty_rec(ts3: Optional[str]) -> Dict[str, Any]:
        rec: Dict[str, Any] = {"timestamp": ts3}
        for k in STRAT_IND_EXPECTED:
            rec[k] = "None" if k == "trend_status" else None
        return rec

    def _coerce_strategy_value(target: str, raw_value: Any):
        if target == "trend_status":
            return _normalize_trend_status(raw_value)
        return _coerce_number_or_bool(raw_value)

    def _valid_record(rec: Dict[str, Any]) -> bool:
        non_empty = any(rec.get(k) is not None for k in STRAT_IND_EXPECTED)
        if not non_empty:
            return False

        tsv = rec.get("trend_strength_value")
        if isinstance(tsv, str):
            s = tsv.strip()
            if not _is_number_like_str(s):
                return False
            try:
                rec["trend_strength_value"] = int(s) if re.fullmatch(r"-?\d+", s) else float(s)
            except Exception:
                return False
        return True

    for line in lines:
        if "TrendStrength=" not in line and "trend_strength" not in line:
            continue

        ts_m = TS_PREFIX.search(line)
        ts = ts_m.group("ts") if ts_m else None
        ts3 = ts_to_ms3_trunc(ts) if ts else None

        idx = line.rfind("] ")
        tail = line[idx + 2 :] if idx >= 0 else line
        tail = tail.strip()
        if not tail:
            continue

        if tail.startswith("TrendStrength="):
            body = tail.split("=", 1)[1].strip()
            values = [part.strip() for part in body.split(",")]
            if len(values) == len(positional_columns):
                rec = _empty_rec(ts3)
                for target, raw_value in zip(positional_columns, values):
                    rec[target] = _coerce_strategy_value(target, raw_value)
                if _valid_record(rec):
                    out.append(rec)
                continue

        kv = _parse_kv_regex(tail)
        if not kv:
            continue

        normalized_keys = {k.strip().lower(): v for k, v in kv.items()}
        if not any(k in key_map for k in normalized_keys.keys()):
            continue

        rec = _empty_rec(ts3)
        for k_low, v_raw in normalized_keys.items():
            target = key_map.get(k_low)
            if not target:
                continue
            rec[target] = _coerce_strategy_value(target, v_raw)

        if _valid_record(rec):
            out.append(rec)

    return out

# ============================================================
# PARAMS (legacy kept) + FINAL
# ============================================================

PARAM_RX = re.compile(
    r'Init Param\s+"?(?P<key>[^"=]+?)"?\s*=\s*"?(?P<value>.*?)"?\s*(?:,\s*rules=.*)?$',
    re.IGNORECASE,
)

PARAM_OUTPUT_MAP = {
    "emission_stat_period": "TrendStrength#emissionStatPeriod",
    "bull_quantile": "TrendStrength#bullQuantile",
    "bear_quantile": "TrendStrength#bearQuantile",
    "initial_prob_bullish": "TrendStrength#initialProbBullish",
    "initial_prob_bearish": "TrendStrength#initialProbBearish",
    "trans_prob_stable": "TrendStrength#transProbStable",
    "trans_prob_switch": "TrendStrength#transProbSwitch",
    "slope_lookback_period": "TrendStrength#slopeLookbackPeriod",
    "double_ewma_history_len": "TrendStrength#slopeLookbackPeriod",
    "position_build_up_factor": "TrendStrength#positionBuildUpFactor",
    "position_reduction_factor": "TrendStrength#positionReductionFactor",
    "confirmation_factor": "TrendStrength#ConfirmationFactor",
    "ewma_lambda": "TrendStrength#ewmaLambda",
    "double_ewma_lambda": "TrendStrength#doubleEwmaLambda",
    "max_abs_position": "TrendStrength#maxAbsPosition",
    "price_precision": "pricePrecision",
    "size_precision": "sizePrecision",
    "central_close_proportion": "centralCloseProportion",
    "price_lambda": "priceLambda",
    "deviation_lambda": "deviationLambda",
    "env_factor": "envFactor",
    "position_bias_param": "positionBiasParam",
    "hard_limit_factor": "hardLimitFactor",
    "min_deviation": "minDeviation",
    "trade_rate_lambda": "tradeRateLambda",
    "target_trade_rate": "targetTradeRate",
    "base_order_size": "baseOrdersize",
    "grid_points": "gridPoints",
    "grid_interval": "gridInterval",
    "grid_value_factor": "gridValueFactor",
    "grid_power": "gridPower",
    "order_size_percentage": "orderSizePercentage",
}

IGNORED_AUTO_PARAMS = {
    "name",
    "status",
    "tool_status",
    "account",
    "symbol",
    "rules",
}

def _normalize_raw_param_key(key: str) -> str:
    if key is None:
        return ""
    return _strip_quotes(str(key).strip())

def _normalize_raw_param_value(value: str):
    if value is None:
        return None

    s = str(value).strip()

    for sep in [', rules=', ',rules=', ' rules=']:
        idx = s.find(sep)
        if idx != -1:
            s = s[:idx].strip()
            break

    s = s.rstrip(",").strip()
    s = _strip_quotes(s)

    return _coerce_number_or_bool(s)

def _extract_raw_params(lines: List[str]) -> Dict[str, Any]:
    raw: Dict[str, Any] = {}

    for line in lines:
        m = PARAM_RX.search(line.strip())
        if not m:
            continue

        raw_key = _normalize_raw_param_key(m.group("key"))
        raw_val = _normalize_raw_param_value(m.group("value"))

        if not raw_key:
            continue

        raw[raw_key] = raw_val

    return raw

def _fixed_decimal_string(v, decimals: int = 12, default=None):
    if v is None:
        return default
    try:
        f = float(v)
    except Exception:
        return default

    s = f"{f:.{decimals}f}".rstrip("0").rstrip(".")
    if s in ("", "-"):
        s = "0"
    if s.startswith("."):
        s = "0" + s
    if s.startswith("-."):
        s = s.replace("-.", "-0.", 1)
    return s

def _auto_output_key_from_raw(raw_key: str) -> str:
    k = _strip_quotes(str(raw_key).strip())
    k = k.lstrip("_")

    if "#" in k:
        k = k.split("#", 1)[1]

    k = k.strip().replace("-", "_").replace(" ", "_")
    return camel_to_snake(k)

def _should_auto_include(raw_key: str, value: Any) -> bool:
    if not raw_key:
        return False

    normalized = _auto_output_key_from_raw(raw_key)
    if not normalized:
        return False

    if normalized in IGNORED_AUTO_PARAMS:
        return False

    if isinstance(value, str):
        v = value.strip()
        if not v:
            return False
        if v.startswith("_"):
            return False

    return True

def params(lines: List[str]) -> Dict[str, Any]:
    raw_params = _extract_raw_params(lines)

    out: Dict[str, Any] = {
        out_key: raw_params.get(raw_key)
        for out_key, raw_key in PARAM_OUTPUT_MAP.items()
    }

    mapped_raw_keys = set(PARAM_OUTPUT_MAP.values())
    used_output_keys = set(out.keys())

    auto_params: Dict[str, Any] = {}
    for raw_key, value in raw_params.items():
        if raw_key in mapped_raw_keys:
            continue

        auto_key = _auto_output_key_from_raw(raw_key)
        if not _should_auto_include(raw_key, value):
            continue

        if auto_key in used_output_keys:
            continue

        auto_params[auto_key] = value
        used_output_keys.add(auto_key)

    final_out: Dict[str, Any] = {}
    final_out.update(out)
    final_out.update(auto_params)

    final_out["epsilon"] = 0.0000000001
    
    
    final_out["d2_ewma_lambda"] = 0.9995
    final_out["d2_ewma_bullish_threshold"] = 1e-10
    final_out["d2_ewma_bearish_threshold"] = -1e-10
    final_out["use_bid_ask_for_bar_height"] = True
    final_out["use_only_volume_bars"] = False

    final_out["d2_ewma_bullish_threshold_str"] = _fixed_decimal_string(
        final_out.get("d2_ewma_bullish_threshold"),
        decimals=12,
        default="0.0000000001",
    )
    final_out["d2_ewma_bearish_threshold_str"] = _fixed_decimal_string(
        final_out.get("d2_ewma_bearish_threshold"),
        decimals=12,
        default="-0.0000000001",
    )

    if final_out.get("aep_factor") is None:
        final_out["aep_factor"] = 0.0

    return final_out

def params_final(lines: List[str]) -> Dict[str, Any]:
    return params(lines)

# ============================================================
# ERRORS & WARNINGS (kept)
# ============================================================

def errors_and_warnings(lines: List[str]) -> List[Dict[str, Any]]:
    rows = []
    for ln in lines:
        rows.append({"raw": ln.strip()})
    return rows

# ============================================================
# BARS (execution_bar + strategy_bar) - parity column order
# ============================================================

def _parse_ohlc(text: str) -> Optional[Tuple[float, float, float, float]]:
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 4:
        return None
    o = safe_float(parts[0])
    h = safe_float(parts[1])
    l = safe_float(parts[2])
    c = safe_float(parts[3])
    return (o, h, l, c)

def _put_ohlc(rec: Dict[str, Any], prefix: str, ohlc: Optional[Tuple[float, float, float, float]]):
    if not ohlc:
        rec[f"{prefix}_open"] = None
        rec[f"{prefix}_high"] = None
        rec[f"{prefix}_low"] = None
        rec[f"{prefix}_close"] = None
        return
    rec[f"{prefix}_open"] = ohlc[0]
    rec[f"{prefix}_high"] = ohlc[1]
    rec[f"{prefix}_low"] = ohlc[2]
    rec[f"{prefix}_close"] = ohlc[3]

def _parse_bar_common(lines: List[str], tag: str) -> List[Dict[str, Any]]:
    ts = r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{6}"

    extra_re = re.compile(
        rf"(?P<timestamp>{ts})\s+\[INF\]\s+\[{re.escape(tag)}\]\s+extra\s+"
        r"bid:ohlc\((?P<bid>[^)]+)\),"
        r"ask:ohlc\((?P<ask>[^)]+)\),"
        r"fair:ohlc\((?P<fair>[^)]+)\),"
        r"trades:ohlc\((?P<trades>[^)]+)\)"
    )

    # Mantido compatível com o log antigo/correto:
    # - captura o OHLC principal mesmo que o restante da linha varie
    # - campos opcionais são extraídos separadamente
    bar_re = re.compile(
        rf"(?P<timestamp>{ts})\s+\[INF\]\s+\[{re.escape(tag)}\]\s+"
        r"bar\[(?P<bar_number>\d+)\]\s+"
        r"ohlc\((?P<main>[^)]+)\).*",
        re.IGNORECASE,
    )

    BAR_KEYS = [
        "timestamp",
        "last_bbo_local_timestamp",
        "bar_number",
        "open",
        "high",
        "low",
        "close",
        "bar_numtick",
        "last_trade_id",
        "bar_counter",
        "trades_volume",
        "time_elapsed_ms",
        "price_diff",
        "raw_price_diff",
        "is_price_bar",
        "is_volume_bar",
        "is_final_split_bar",
        "last_sequence_id",
        "fair_open",
        "fair_high",
        "fair_low",
        "fair_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "trades_open",
        "trades_high",
        "trades_low",
        "trades_close",
    ]

    out: List[Dict[str, Any]] = []
    pending_extra: Optional[Dict[str, Any]] = None

    def _find_int(pattern: str, s: str):
        m = re.search(pattern, s)
        return int(m.group(1)) if m else None

    def _find_float(pattern: str, s: str):
        m = re.search(pattern, s)
        return float(m.group(1)) if m else None

    def _find_bool(pattern: str, s: str):
        m = re.search(pattern, s)
        if not m:
            return None
        return m.group(1) == "True"

    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue

        m_extra = extra_re.search(ln)
        if m_extra:
            g = m_extra.groupdict()
            pending_extra = {
                "bid": _parse_ohlc(g.get("bid") or ""),
                "ask": _parse_ohlc(g.get("ask") or ""),
                "fair": _parse_ohlc(g.get("fair") or ""),
                "trades": _parse_ohlc(g.get("trades") or ""),
            }
            continue

        m_bar = bar_re.search(ln)
        if not m_bar:
            continue

        g = m_bar.groupdict()
        ts_val = g.get("timestamp")

        main_ohlc = _parse_ohlc(g.get("main") or "")
        if main_ohlc:
            o, h, l, c = main_ohlc
        else:
            o = h = l = c = None

        rec: Dict[str, Any] = {k: None for k in BAR_KEYS}
        rec["timestamp"] = ts_val
        rec["last_bbo_local_timestamp"] = str(epoch_ms_dubai(ts_val)) if ts_val else None
        rec["bar_number"] = safe_int(g.get("bar_number") or "")
        rec["open"], rec["high"], rec["low"], rec["close"] = o, h, l, c

        rec["bar_numtick"] = _find_int(r"\bnumtick=(\d+)\b", ln)
        rec["last_trade_id"] = _find_int(r"\blast_tradeid=(\d+)\b", ln)

        m_bc = re.search(r"\bbar_counter=(null|-?\d+)\b", ln)
        rec["bar_counter"] = None if (not m_bc or m_bc.group(1) == "null") else int(m_bc.group(1))

        rec["trades_volume"] = _find_float(r"\bvolume=(-?\d+(?:\.\d+)?)\b", ln)
        rec["time_elapsed_ms"] = _find_int(r"\btime_elapsed_ms=(\d+)\b", ln)

        m_co = re.search(r"(?:^|[,\s])c-o=(-?\d+(?:\.\d+)?)(?:[,\s]|$)", ln)
        rec["price_diff"] = float(m_co.group(1)) if m_co else None

        # Compatibilidade com o execution_bar antigo/correto:
        # - missing rawc-o => None
        # - rawc-o=null => "null"
        # - rawc-o=<number> => float
        m_rawco = re.search(r"(?:^|[,\s])rawc-o=([-\d\.]+|null)(?:[,\s]|$)", ln)
        if not m_rawco:
            rec["raw_price_diff"] = None
        else:
            val = m_rawco.group(1)
            if val == "null":
                rec["raw_price_diff"] = "null"
            else:
                try:
                    rec["raw_price_diff"] = float(val)
                except Exception:
                    rec["raw_price_diff"] = val

        rec["is_price_bar"] = _find_bool(r"\bisPriceBar=(True|False)\b", ln)
        rec["is_volume_bar"] = _find_bool(r"\bisVolumeBar=(True|False)\b", ln)
        rec["is_final_split_bar"] = _find_bool(r"\bisFinalSplitBar=(True|False)\b", ln)

        m_seq = re.search(r"\bOBSeq=(\d+)\b", ln)
        rec["last_sequence_id"] = m_seq.group(1) if m_seq else None

        # Regra antiga/correta:
        # pending_extra NÃO é limpo após cada barra
        if pending_extra:
            _put_ohlc(rec, "fair", pending_extra.get("fair"))
            _put_ohlc(rec, "ask", pending_extra.get("ask"))
            _put_ohlc(rec, "bid", pending_extra.get("bid"))
            _put_ohlc(rec, "trades", pending_extra.get("trades"))
        else:
            _put_ohlc(rec, "fair", None)
            _put_ohlc(rec, "ask", None)
            _put_ohlc(rec, "bid", None)
            _put_ohlc(rec, "trades", None)

        out.append(rec)

    return out

def parse_execution_bar(lines: List[str]) -> List[Dict[str, Any]]:
    return _parse_bar_common(lines, "CB_RO")

def parse_strategy_bar(lines: List[str]) -> List[Dict[str, Any]]:
    return _parse_bar_common(lines, "CB_RO#trend")

# ============================================================
# FILLS (best-effort; schema parity with old)
# ============================================================

FILLS_RX = re.compile(
    r"trade_id=(?P<trade_id>\d+).*?"
    r"side=(?P<side>BUY|SELL).*?"
    r"price=(?P<price>[0-9.]+).*?"
    r"qty=(?P<qty>[0-9.]+).*?"
    r"fee=(?P<fee>[0-9.]+)",
    re.IGNORECASE,
)

def fills(lines: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for line in lines:
        ts_m = TS_PREFIX.search(line)
        ts = ts_m.group("ts") if ts_m else None
        m = FILLS_RX.search(line)
        if not m:
            continue
        g = m.groupdict()
        rec = {
            "timestamp": ts,
            "trade_id": safe_int(g.get("trade_id") or ""),
            "side": (g.get("side") or "").upper(),
            "price": safe_float(g.get("price") or ""),
            "qty": safe_float(g.get("qty") or ""),
            "fee": safe_float(g.get("fee") or ""),
            "raw": line.strip(),
        }
        out.append(rec)
    return out

# ============================================================
# PUBLIC EXPORTS (server uses these)
# ============================================================

def orders_final(lines: List[str]) -> List[Dict[str, Any]]:
    return parse_orders_final(lines)

def duration_statistics_final(lines: List[str]) -> List[Dict[str, Any]]:
    return parse_duration_statistics_final(lines)

def execution_indicators_final(lines: List[str]) -> List[Dict[str, Any]]:
    return parse_execution_indicators_final(lines)

def strategy_indicators_final(lines: List[str]) -> List[Dict[str, Any]]:
    return parse_strategy_indicators_final(lines)

def execution_bar(lines: List[str]) -> List[Dict[str, Any]]:
    return parse_execution_bar(lines)

def strategy_bar(lines: List[str]) -> List[Dict[str, Any]]:
    return parse_strategy_bar(lines)

# =========================
# LEGACY EXPORTS (server expects these names)
# =========================

def orders(lines):
    return parse_orders(lines)

def duration_statistics(lines):
    return parse_duration_statistics(lines)

def strategy_indicators(lines):
    return parse_strategy_indicators(lines)
