"""
sitecustomize.py
Automatically loaded by Python when the directory is present in PYTHONPATH.

Purpose:
- Fix invalid cryptam call: pandas.to_csv(..., append=True)
  -> pandas does NOT support append=, so we translate it to mode="a"
- Fix json.dump(...) failures when cryptam tries to serialize
  non-JSON-serializable objects (e.g. TradesBboHybridBarDataRecord)
  -> if default is not provided, we fallback to default=str
"""

# ------------------------------------------------------------------
# Patch pandas.DataFrame.to_csv(append=True) -> mode="a"
# ------------------------------------------------------------------
try:
    import pandas as pd
    from pandas.core.generic import NDFrame

    _original_to_csv = NDFrame.to_csv

    def _to_csv_patched(self, *args, **kwargs):
        # cryptam incorrectly uses append=True, which pandas does not support
        if "append" in kwargs:
            append = kwargs.pop("append")
            if append:
                # append=True means append mode
                kwargs.setdefault("mode", "a")
                # when appending, we usually do not want to duplicate the header
                kwargs.setdefault("header", False)
        return _original_to_csv(self, *args, **kwargs)

    NDFrame.to_csv = _to_csv_patched

except Exception:
    # Fail silently to avoid breaking the process if pandas internals change
    pass


# ------------------------------------------------------------------
# Patch json.dump(...) to handle non-JSON-serializable objects
# ------------------------------------------------------------------
try:
    import json as _json

    _original_dump = _json.dump

    def _dump_patched(obj, fp, *args, **kwargs):
        # If no default serializer is provided, fallback to str(...)
        kwargs.setdefault("default", str)
        return _original_dump(obj, fp, *args, **kwargs)

    _json.dump = _dump_patched

except Exception:
    # Fail silently to avoid breaking json if something unexpected happens
    pass
