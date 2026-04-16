# logfilesplit.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class SectionRule:
    name: str
    pattern: re.Pattern


def _safe_stem(text: str) -> str:
    """Make a filename-safe stem."""
    text = text.strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9._-]+", "", text)
    return text or "section"


# Regras default (modo "header"/seção atual). Para xBots, use split_log_bucket com regras próprias.
DEFAULT_RULES: List[SectionRule] = [
    SectionRule("orders", re.compile(r"\borders?\b", re.IGNORECASE)),
    SectionRule("executions", re.compile(r"\bexecutions?\b", re.IGNORECASE)),
    SectionRule("duration_statistics", re.compile(r"\bduration[_\s-]*statistics\b", re.IGNORECASE)),
    SectionRule("grid", re.compile(r"\bgrid\b", re.IGNORECASE)),
    SectionRule("trend", re.compile(r"\btrend\b", re.IGNORECASE)),
    SectionRule("pnl", re.compile(r"\bpnl\b|\bprofit\b|\bloss\b", re.IGNORECASE)),
]


def split_log(
    file_path: str,
    out_dir: str,
    rules: Optional[List[SectionRule]] = None,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    keep_unmatched: bool = True,
) -> Dict[str, List[str]]:
    """
    Split por "seção atual" (quando encontra um header, começa a escrever nessa seção).
    Retorna: section_name -> lista de arquivos escritos (paths em string).
    """
    rules = rules or DEFAULT_RULES

    in_path = Path(file_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Log file not found: {in_path}")

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    handles: Dict[str, object] = {}
    written: Dict[str, List[str]] = {}
    current_section: Optional[str] = None

    def _open_for(section: str) -> object:
        section_stem = _safe_stem(section)
        fp = out_path / f"{section_stem}.log"
        if section not in handles:
            handles[section] = fp.open("w", encoding=encoding, errors=errors)
            written.setdefault(section, []).append(str(fp))
        return handles[section]

    def _match_section(line: str) -> Optional[str]:
        for rule in rules:
            if rule.pattern.search(line):
                return rule.name
        return None

    try:
        with in_path.open("r", encoding=encoding, errors=errors) as f:
            for line in f:
                sec = _match_section(line)
                if sec is not None:
                    current_section = sec

                if current_section is not None:
                    _open_for(current_section).write(line)
                else:
                    if keep_unmatched:
                        _open_for("unmatched").write(line)
    finally:
        for h in handles.values():
            try:
                h.close()
            except Exception:
                pass

    return written


def split_log_bucket(
    file_path: str,
    out_dir: str,
    rules: Optional[List[SectionRule]] = None,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    keep_unmatched: bool = True,
) -> Dict[str, List[str]]:
    """
    Split "bucket" (linha-a-linha):
      - cada linha vai para o PRIMEIRO rule que casar
      - não existe "seção atual"
    Ideal para logs contínuos tipo xBots.

    Retorna: section_name -> lista de arquivos escritos (paths em string).
    """
    rules = rules or DEFAULT_RULES

    in_path = Path(file_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Log file not found: {in_path}")

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    handles: Dict[str, object] = {}
    written: Dict[str, List[str]] = {}

    def _open_for(section: str) -> object:
        section_stem = _safe_stem(section)
        fp = out_path / f"{section_stem}.log"
        if section not in handles:
            handles[section] = fp.open("w", encoding=encoding, errors=errors)
            written.setdefault(section, []).append(str(fp))
        return handles[section]

    def _match_bucket(line: str) -> Optional[str]:
        for rule in rules:
            if rule.pattern.search(line):
                return rule.name
        return None

    try:
        with in_path.open("r", encoding=encoding, errors=errors) as f:
            for line in f:
                bucket = _match_bucket(line)
                if bucket is not None:
                    _open_for(bucket).write(line)
                else:
                    if keep_unmatched:
                        _open_for("unmatched").write(line)
    finally:
        for h in handles.values():
            try:
                h.close()
            except Exception:
                pass

    return written


def quick_rules_from_headers(headers: List[Tuple[str, str]]) -> List[SectionRule]:
    """
    Helper para criar regras a partir de (name, regex_string).

    Ex:
        rules = quick_rules_from_headers([
            ("orders", r"^=+ ORDERS =+$"),
            ("exec",   r"^=+ EXECUTIONS =+$"),
        ])
    """
    out: List[SectionRule] = []
    for name, rx in headers:
        out.append(SectionRule(name=name, pattern=re.compile(rx, re.IGNORECASE)))
    return out
