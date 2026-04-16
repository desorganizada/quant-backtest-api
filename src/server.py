import os
import json
import csv
import shutil
import traceback
import re
import strategy_registry
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List

from logfilesplit import split_log_bucket, SectionRule
from transforms import (
    # legacy exports kept (do not remove)
    orders,
    duration_statistics,
    execution_indicators,
    strategy_indicators,
    params,
    errors_and_warnings,
    # NEW parity exports used by this server
    orders_final,
    duration_statistics_final,
    execution_indicators_final,
    strategy_indicators_final,
    params_final,
    execution_bar,
    strategy_bar,
    fills,
)

from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field, ConfigDict

app = FastAPI()
print(">>> SERVER VERSION: /process (SYNC) + /backtest-path + /run EXEC BACKTEST + XBOTS BUCKET SPLIT + NORMALIZATION + FULL DATA-ETL PARITY OUTPUTS <<<")

# ============================================================
# ROOT CONFIG
# ============================================================
BACKTESTING_ROOT = Path(os.getenv("BACKTESTING_ROOT", "/data/backtesting")).resolve()
print(f">>> BACKTESTING_ROOT = {BACKTESTING_ROOT}")

PARAM_INDEX_PATH = BACKTESTING_ROOT / "parameters" / "index" / "last_valid_params.json"

SUBFOLDERS = {
    "raw": "raw_logs",
    "orders": "orders",
    "params": "parameters",
    "duration_statistics": "duration_statistics",
    "execution_indicators": "execution_indicators",
    "strategy_indicators": "strategy_indicators",
    "execution_bar": "execution_bar",
    "strategy_bar": "strategy_bar",
    "fills": "fills",
    "errors_and_warnings": "errors_and_warnings",
    "unmatched": "unmatched",
    "remain": "remain",
    "anomalies": "anomalies",
    "trend": "trend",
}

ALL_BUCKET_KEYS = [
    "orders",
    "params",
    "duration_statistics",
    "execution_indicators",
    "strategy_indicators",
    "execution_bar",
    "strategy_bar",
    "fills",
    "errors_and_warnings",
    "unmatched",
    "remain",
    "anomalies",
    "trend",
]

CSV_PARSERS = {
    "orders": orders_final,
    "fills": fills,
    "execution_indicators": execution_indicators_final,
    "strategy_indicators": strategy_indicators_final,
    "duration_statistics": duration_statistics_final,
    "execution_bar": execution_bar,
    "strategy_bar": strategy_bar,
    "errors_and_warnings": errors_and_warnings,
}

JSON_PARSERS = {
    "params": params_final,
}

# =============================
# XBOTS RULES (bucket mode)
# =============================

XBOTS_RULES = [
    SectionRule("orders", re.compile(r"(Rest Limit Order|Sending Limit Order|NewOrder created|CancelAllOpenOrders|CancelOrder)", re.IGNORECASE)),
    SectionRule("params", re.compile(r"\bInit Param\b", re.IGNORECASE)),
    SectionRule("duration_statistics", re.compile(r"HttpRoundTrip:\[\d+,\s*(NEW|CANCEL|AMEND|MODIFY|REPLACE)", re.IGNORECASE)),
    SectionRule("errors_and_warnings", re.compile(r"\[(WRN|ERR)\]", re.IGNORECASE)),
    SectionRule(
        "execution_indicators",
        re.compile(
            r"(centralPrice=|ewmaPrice=|ewmaDeviation=|forwardMid=|positionBias=|forwardHighOrderPrice=|forwardLowOrderPrice=|"
            r"prevTargetPosition=|targetPosition=|avgEntryPrice=|aepAdjustmentHigh=|aepAdjustmentLow=|"
            r"barHeight=|sellRate=|buyRate=|sellTrade=|sellTradePrice=|buyTrade=|buyTradePrice=)",
            re.IGNORECASE,
        ),
    ),
    SectionRule("strategy_indicators", re.compile(r"\bTrendStrength=", re.IGNORECASE)),
    SectionRule("execution_bar", re.compile(r"\[CB_RO\]\s+(extra|bar\[)", re.IGNORECASE)),
    SectionRule("strategy_bar", re.compile(r"\[CB_RO#trend\]\s+(extra|bar\[)", re.IGNORECASE)),
    SectionRule("fills", re.compile(r"(filled|fill|trade_id=|fee=|OrderBook.*Filled|FirmOrderBook.*Filled)", re.IGNORECASE)),
]

# =============================
# UTILITIES
# =============================

TS_6 = re.compile(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{6}\s+\[")

def utc_run_stamp() -> str:
    now = datetime.now(timezone.utc)
    ms = int(now.microsecond / 1000)
    return now.strftime(f"%Y-%m-%dT%H-%M-%S-{ms:03d}Z")

def make_run_id() -> str:
    return utc_run_stamp()

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)

def save_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", errors="ignore")

def write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("", encoding="utf-8")
        return

    fieldnames: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def write_json(obj: Any, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def ensure_under_root(path: Path, root: Path, err: str) -> Path:
    try:
        resolved = path.resolve()
        root_resolved = root.resolve()

        if not str(resolved).startswith(str(root_resolved)):
            raise HTTPException(
                status_code=400,
                detail={
                    "message": err,
                    "received_path": str(resolved),
                    "allowed_root": str(root_resolved),
                },
            )

        return resolved

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={
                "message": err,
                "received_path": str(path),
                "allowed_root": str(root),
                "exception": f"{type(e).__name__}: {e}",
            },
        )

def detect_encoding(path: Path) -> str:
    b = path.read_bytes()[:4096]
    if b.startswith(b"\xff\xfe") or b.startswith(b"\xfe\xff"):
        return "utf-16"
    if b:
        nul_ratio = b.count(b"\x00") / max(1, len(b))
        if nul_ratio > 0.2:
            return "utf-16"
    return "utf-8"

def normalize_xbots_log(src_path: Path, dst_path: Path, *, src_encoding: str) -> None:
    text = src_path.read_text(encoding=src_encoding, errors="replace")
    text = text.replace("\x00", "")
    text = re.sub(r"(?<!\n)(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{6}\s+\[)", r"\n\1", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    dst_path.write_text(text, encoding="utf-8", errors="replace")

def call_parse(fn, lines: List[str]):
    return fn(lines)

def read_lines_from_written_map(written_map: Dict[str, List[str]], *, encoding: str = "utf-8") -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for key, paths in (written_map or {}).items():
        lines: List[str] = []
        for p in (paths or []):
            try:
                with open(p, "r", encoding=encoding, errors="replace") as f:
                    for ln in f:
                        lines.append(ln.rstrip("\n"))
            except FileNotFoundError:
                continue
        out[key] = lines
    return out

def count_lines_from_written_map(written_map: Dict[str, List[str]], *, encoding: str = "utf-8") -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for key, paths in (written_map or {}).items():
        c = 0
        for p in (paths or []):
            try:
                with open(p, "r", encoding=encoding, errors="replace") as f:
                    for _ in f:
                        c += 1
            except FileNotFoundError:
                continue
        counts[key] = c
    return counts

def path_exists_and_not_empty(p: Optional[str]) -> bool:
    if not p:
        return False
    try:
        path = Path(p)
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except Exception:
        return False

def build_context_key(strategy: str, exec_strategy: str, module_strategy: str) -> str:
    return f"{strategy}|{exec_strategy}|{module_strategy}"

def load_param_index() -> Dict[str, Any]:
    if not PARAM_INDEX_PATH.exists():
        return {}
    try:
        with PARAM_INDEX_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_param_index(data: Dict[str, Any]) -> None:
    PARAM_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with PARAM_INDEX_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def validate_required_output_file(path_str: Optional[str], label: str) -> str:
    if not path_str:
        raise HTTPException(status_code=400, detail=f"{label} path is missing in metadata")
    p = Path(path_str)
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail=f"{label} file not found: {path_str}")
    if p.stat().st_size == 0:
        raise HTTPException(status_code=400, detail=f"{label} file is empty: {path_str}")
    return str(p)

def inspect_strategy_params_json(strategy_param_path: Path) -> Dict[str, Any]:
    try:
        with strategy_param_path.open("r", encoding="utf-8") as f:
            param_data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"failed to read strategy_param_path json: {type(e).__name__}: {e}",
        )

    numeric_keys = [
        "epsilon",
        "d2_ewma_lambda",
        "d2_ewma_bullish_threshold",
        "d2_ewma_bearish_threshold",
        "aep_factor",
    ]

    inspected = {}
    bad_types = {}

    for k in numeric_keys:
        v = param_data.get(k)
        inspected[k] = {
            "value": repr(v),
            "type": type(v).__name__,
        }
        if v is not None and not isinstance(v, (int, float)):
            bad_types[k] = {
                "value": repr(v),
                "type": type(v).__name__,
            }

    return {
        "inspected": inspected,
        "bad_types": bad_types,
    }

# =============================
# API MODELS
# =============================

class RunRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    strategy_bar_path: str = Field(..., description="path to strategy_bar csv")
    execution_bar_path: str = Field(..., description="path to execution_bar csv")
    strategy_param_path: str = Field(..., description="path to params json")
    strategy: str = Field(..., description="strategy name, ex: D2Trend")

    exec_strategy: str = Field("EE", description="execution strategy, ex: EE")
    module_strategy: str = Field("strategy_registry", description="module strategy, ex: strategy_registry")
    result_output_path: Optional[str] = Field(
        None,
        description="directory where backtest outputs will be written under BACKTESTING_ROOT",
    )
    run_id: Optional[str] = None

class ProcessRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    file_path: str = Field(..., description="path to raw xBots log under BACKTESTING_ROOT")
    final_root: Optional[str] = Field(None, description="override output root under BACKTESTING_ROOT")

class BacktestPathRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    metadata_path: str = Field(..., description="metadata JSON path returned by /process")
    strategy: str = Field(..., description="strategy name, ex: D2Trend")
    exec_strategy: str = Field(..., description="execution strategy, ex: EE")
    module_strategy: str = Field(..., description="module strategy, ex: strategy_registry")

# =============================
# ENDPOINTS
# =============================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "backtesting_root": str(BACKTESTING_ROOT),
    }

@app.post("/run")
def run_backtest(req: RunRequest = Body(...)):
    run_id = req.run_id or make_run_id()

    strategy_bar_path = ensure_under_root(
        Path(req.strategy_bar_path),
        BACKTESTING_ROOT,
        "strategy_bar_path must be under BACKTESTING_ROOT",
    )
    execution_bar_path = ensure_under_root(
        Path(req.execution_bar_path),
        BACKTESTING_ROOT,
        "execution_bar_path must be under BACKTESTING_ROOT",
    )
    strategy_param_path = ensure_under_root(
        Path(req.strategy_param_path),
        BACKTESTING_ROOT,
        "strategy_param_path must be under BACKTESTING_ROOT",
    )

    if not strategy_bar_path.exists() or not strategy_bar_path.is_file():
        raise HTTPException(status_code=404, detail=f"strategy_bar_path not found: {strategy_bar_path}")
    if strategy_bar_path.stat().st_size == 0:
        raise HTTPException(status_code=400, detail=f"strategy_bar_path is empty: {strategy_bar_path}")

    if not execution_bar_path.exists() or not execution_bar_path.is_file():
        raise HTTPException(status_code=404, detail=f"execution_bar_path not found: {execution_bar_path}")
    if execution_bar_path.stat().st_size == 0:
        raise HTTPException(status_code=400, detail=f"execution_bar_path is empty: {execution_bar_path}")

    if not strategy_param_path.exists() or not strategy_param_path.is_file():
        raise HTTPException(status_code=404, detail=f"strategy_param_path not found: {strategy_param_path}")
    if strategy_param_path.stat().st_size == 0:
        raise HTTPException(status_code=400, detail=f"strategy_param_path is empty: {strategy_param_path}")

    
    if req.result_output_path:
        result_output_path = ensure_under_root(
            Path(req.result_output_path),
            BACKTESTING_ROOT,
            "result_output_path must be under BACKTESTING_ROOT",
        )
    else:
        result_output_path = BACKTESTING_ROOT / "backtest_results" / safe_filename(run_id)

    ensure_dir(result_output_path)

    cmd = [
        "cryptam",
        "backtest",
        str(strategy_bar_path),
        "TradesBboHybridBarData",
        req.strategy,
        "TargetPositionBacktester",
        f"--exec-data-path={execution_bar_path}",
        f"--exec-strategy={req.exec_strategy}",
        f"--strategy-param-path={strategy_param_path}",
        f"--result-output-path={result_output_path}",
        f"--module-strategy={req.module_strategy}",
    ]

    
    env = os.environ.copy()
    env["PYTHONPATH"] = f"/work:{env.get('PYTHONPATH','')}"
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(BACKTESTING_ROOT),
            check=False,
            env=env,
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail="cryptam executable not found in server environment",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"failed to execute backtest: {type(e).__name__}: {e}",
        )

    return {
        "ok": result.returncode == 0,
        "run_id": run_id,
        "returncode": result.returncode,
        "command": cmd,
        "result_output_path": str(result_output_path),
        "stdout": result.stdout,
        "stderr": result.stderr,
         }

@app.get("/runs/{run_id}/summary")
def run_summary(run_id: str):
    return {"run_id": run_id, "status": "unknown", "note": "summary endpoint kept for compatibility"}

@app.post("/backtest-path")
def backtest_path(req: BacktestPathRequest = Body(...)):
    metadata_path = ensure_under_root(
        Path(req.metadata_path),
        BACKTESTING_ROOT,
        "metadata_path must be under BACKTESTING_ROOT",
    )

    if not metadata_path.exists() or not metadata_path.is_file():
        raise HTTPException(status_code=404, detail="metadata_path not found")

    try:
        with metadata_path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed to read metadata: {type(e).__name__}: {e}")

    generated = meta.get("generated") or {}
    split_counts = meta.get("split_counts") or {}

    execution_bar_path_raw = (generated.get("execution_bar") or {}).get("structured")
    strategy_bar_path_raw = (generated.get("strategy_bar") or {}).get("structured")
    current_params_path_raw = (generated.get("params") or {}).get("structured")

    execution_bar_path = validate_required_output_file(execution_bar_path_raw, "execution_bar")
    strategy_bar_path = validate_required_output_file(strategy_bar_path_raw, "strategy_bar")

    context_key = build_context_key(req.strategy, req.exec_strategy, req.module_strategy)

    params_count = split_counts.get("params", 0)
    current_params_is_valid = params_count > 0 and path_exists_and_not_empty(current_params_path_raw)

    index = load_param_index()
    used_fallback = False
    strategy_param_path = None
    fallback_source = None

    if current_params_is_valid:
        strategy_param_path = str(Path(current_params_path_raw))
        index[context_key] = {
            "path": strategy_param_path,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source_metadata": str(metadata_path),
        }
        save_param_index(index)
    else:
        fallback = index.get(context_key)
        if not fallback:
            raise HTTPException(
                status_code=400,
                detail=f"params not found in current metadata and no fallback available for context_key={context_key}",
            )

        fallback_path = fallback.get("path")
        if not path_exists_and_not_empty(fallback_path):
            raise HTTPException(
                status_code=400,
                detail=f"fallback params path is missing or empty for context_key={context_key}: {fallback_path}",
            )

        strategy_param_path = str(Path(fallback_path))
        used_fallback = True
        fallback_source = fallback.get("source_metadata")

    return {
        "ok": True,
        "context_key": context_key,
        "metadata_path": str(metadata_path),
        "used_fallback": used_fallback,
        "fallback_source_metadata": fallback_source,
        "resolved_paths": {
            "execution_bar_path": execution_bar_path,
            "strategy_bar_path": strategy_bar_path,
            "strategy_param_path": strategy_param_path,
        },
        "run_payload": {
            "execution_bar_path": execution_bar_path,
            "strategy_bar_path": strategy_bar_path,
            "strategy_param_path": strategy_param_path,
            "strategy": req.strategy,
            "exec_strategy": req.exec_strategy,
            "module_strategy": req.module_strategy,
        },
    }

@app.post("/process")
def process_log(req: ProcessRequest = Body(...)):
    if req.file_path is None or str(req.file_path).strip() == "":
        raise HTTPException(status_code=400, detail="provide file_path")

    in_path = ensure_under_root(
        Path(req.file_path),
        BACKTESTING_ROOT,
        "file_path must be under BACKTESTING_ROOT",
    )
    if not in_path.exists() or not in_path.is_file():
        raise HTTPException(status_code=404, detail="file_path not found")

    final_root = Path(req.final_root).resolve() if req.final_root else BACKTESTING_ROOT
    final_root = ensure_under_root(final_root, BACKTESTING_ROOT, "final_root must be under BACKTESTING_ROOT")
    ensure_dir(final_root)

    run_stamp = utc_run_stamp()

    try:
        return run_parser_pipeline(input_log=in_path, final_root=final_root, run_stamp=run_stamp)
    except HTTPException:
        raise
    except Exception as e:
        try:
            raw_root = ensure_dir(final_root / SUBFOLDERS["raw"])
            meta_dir = ensure_dir(raw_root / "meta")
            save_text(
                meta_dir / safe_filename(f"_ERROR_{run_stamp}.txt"),
                f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}",
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"ETL failed: {type(e).__name__}: {e}")

def run_parser_pipeline(input_log: Path, final_root: Path, run_stamp: str) -> Dict[str, Any]:
    ensure_dir(final_root)

    raw_root = ensure_dir(final_root / SUBFOLDERS["raw"])
    raw_dir = ensure_dir(raw_root / "raw")
    meta_dir = ensure_dir(raw_root / "meta")
    tmp_dir = ensure_dir(raw_root / "tmp_split")

    raw_copy = raw_dir / safe_filename(f"raw_{run_stamp}.log")
    shutil.copy2(input_log, raw_copy)

    split_tmp_dir = ensure_dir(tmp_dir / safe_filename(f"split_tmp_{run_stamp}"))
    norm_path = split_tmp_dir / safe_filename(f"normalized_{run_stamp}.log")

    src_enc = detect_encoding(raw_copy)
    normalize_xbots_log(raw_copy, norm_path, src_encoding=src_enc)

    splits_written = split_log_bucket(
        file_path=str(norm_path),
        out_dir=str(split_tmp_dir),
        rules=XBOTS_RULES,
        encoding="utf-8",
        errors="replace",
        keep_unmatched=True,
    )

    splits_lines = read_lines_from_written_map(splits_written, encoding="utf-8")

    for k in ALL_BUCKET_KEYS:
        splits_lines.setdefault(k, [])

    split_counts = count_lines_from_written_map(splits_written, encoding="utf-8")
    for k in ALL_BUCKET_KEYS:
        split_counts.setdefault(k, 0)

    split_counts_path = meta_dir / safe_filename(f"split_counts_{run_stamp}.json")
    write_json(split_counts, split_counts_path)

    generated: Dict[str, Dict[str, str]] = {}

    for k in ALL_BUCKET_KEYS:
        folder = SUBFOLDERS.get(k, k)

        base_dir = ensure_dir(final_root / folder)
        logs_dir = ensure_dir(base_dir / "logs")
        csv_dir = ensure_dir(base_dir / "csv")
        json_dir = ensure_dir(base_dir / "json")

        bucket_lines = splits_lines.get(k, [])
        txt = "\n".join(bucket_lines)

        lines_ext = "txt" if k in ("params", "anomalies") else "log"
        lines_path = logs_dir / safe_filename(f"{k}_lines_{run_stamp}.{lines_ext}")
        save_text(lines_path, txt)

        entry: Dict[str, str] = {"lines": str(lines_path)}

        if k in CSV_PARSERS:
            rows = call_parse(CSV_PARSERS[k], bucket_lines)
            csv_path = csv_dir / safe_filename(f"{k}_{run_stamp}.csv")
            write_csv(rows, csv_path)
            entry["structured"] = str(csv_path)

        elif k in JSON_PARSERS:
            obj = call_parse(JSON_PARSERS[k], bucket_lines)
            json_path = json_dir / safe_filename(f"{k}_{run_stamp}.json")
            write_json(obj, json_path)
            entry["structured"] = str(json_path)

        generated[k] = entry

    meta = {
        "status": "done",
        "run_stamp_utc": run_stamp,
        "source_file": str(input_log),
        "final_root": str(final_root),
        "raw_copy": str(raw_copy),
        "normalized_copy": str(norm_path),
        "split_tmp_dir": str(split_tmp_dir),
        "split_counts": split_counts,
        "generated": generated,
        "note": "xBots bucket split + UTF-16 handling + normalization; outputs under <bucket>/{logs,csv,json} and raw_logs/{raw,meta,tmp_split}",
    }

    meta_path = meta_dir / safe_filename(f"_metadata_{run_stamp}.json")
    write_json(meta, meta_path)

    return {
        "status": "done",
        "run_stamp_utc": run_stamp,
        "final_root": str(final_root),
        "raw_copy": str(raw_copy),
        "normalized_copy": str(norm_path),
        "metadata": str(meta_path),
        "split_counts": split_counts,
        "generated": generated,
    }