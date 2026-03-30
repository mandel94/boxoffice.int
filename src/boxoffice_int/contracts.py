"""Data contract loader and DataFrame validator — backed by Pandera.

Implements contract validation at domain boundaries, following Data Mesh
principles: each data product is responsible for ensuring its output
conforms to its published contract before crossing domain boundaries.

Usage
-----
    from boxoffice_int.contracts import load_contract, validate

    contract = load_contract("box-office-raw-daily")
    validate(df, contract)          # raises ContractViolationError on failure

    # Optionally get the raw pandera schema for custom checks:
    from boxoffice_int.contracts import build_pandera_schema
    schema = build_pandera_schema(contract)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pandera.pandas as pa
import yaml

LOG = logging.getLogger(__name__)

# Resolved at import time so it works regardless of the working directory.
CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"

# Maps contract field types to the pandas dtype used for coercion.
# Shared by cast_to_contract and build_pandera_schema.
_DTYPE_CAST: dict[str, str] = {
    "string": "object",
    "date": "datetime64[ns]",
    "integer": "Int64",
    "float": "Float64",
}


class ContractViolationError(Exception):
    """Raised when a DataFrame violates its data contract."""


def load_contract(contract_id: str) -> dict[str, Any]:
    """Load a contract YAML by its ``metadata.id`` or filename stem.

    Parameters
    ----------
    contract_id:
        Either the value of ``metadata.id`` inside the YAML, or the
        filename stem (e.g. ``"box_office_raw_daily"``).

    Raises
    ------
    FileNotFoundError
        If no matching contract is found in :data:`CONTRACTS_DIR`.
    """
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        with path.open(encoding="utf-8") as fh:
            contract = yaml.safe_load(fh)
        if not isinstance(contract, dict):
            continue
        if contract.get("metadata", {}).get("id") == contract_id:
            return contract
        if path.stem == contract_id or path.name.split(".")[0] == contract_id:
            return contract
    raise FileNotFoundError(
        f"Contract '{contract_id}' not found in {CONTRACTS_DIR}. "
        f"Available: {[p.stem for p in CONTRACTS_DIR.glob('*.yaml')]}"
    )


def build_pandera_schema(contract: dict[str, Any]) -> pa.DataFrameSchema:
    """Build a :class:`pandera.DataFrameSchema` from a contract YAML dict.

    The resulting schema:

    * **Coerces** each column to its declared dtype (``coerce=True`` per
      column), so calling :func:`cast_to_contract` beforehand is optional.
    * Marks columns as **required** when they are non-nullable *or* listed
      in ``quality.completeness.required_fields``.
    * Enforces ``minimum`` / ``maximum`` constraints via
      :func:`pandera.Check`.
    * Uses ``strict=False`` so extra columns not in the contract are ignored.

    Parameters
    ----------
    contract:
        A contract dict as returned by :func:`load_contract`.
    """
    schema_def = contract.get("schema", {})
    fields: list[dict[str, Any]] = schema_def.get("fields", [])
    required_by_quality: list[str] = (
        contract.get("quality", {})
        .get("completeness", {})
        .get("required_fields", [])
    )

    columns: dict[str, pa.Column] = {}
    for field in fields:
        name: str = field["name"]
        field_type: str = field.get("type", "string")
        nullable: bool = field.get("nullable", True)
        constraints_cfg: dict[str, Any] = field.get("constraints", {})

        required: bool = (not nullable) or (name in required_by_quality)
        dtype: str | None = _DTYPE_CAST.get(field_type)

        checks: list[pa.Check] = []
        if "minimum" in constraints_cfg:
            checks.append(pa.Check.ge(constraints_cfg["minimum"]))
        if "maximum" in constraints_cfg:
            checks.append(pa.Check.le(constraints_cfg["maximum"]))
        if "min_length" in constraints_cfg:
            checks.append(pa.Check.str_length(min_value=constraints_cfg["min_length"]))
        if "max_length" in constraints_cfg:
            checks.append(pa.Check.str_length(max_value=constraints_cfg["max_length"]))
        if "pattern" in constraints_cfg:
            checks.append(pa.Check.str_matches(constraints_cfg["pattern"]))
        if "allowed_values" in constraints_cfg:
            checks.append(pa.Check.isin(constraints_cfg["allowed_values"]))
        if constraints_cfg.get("unique"):
            checks.append(pa.Check(lambda s: s.dropna().is_unique, error="values are not unique"))

        _known = {"minimum", "maximum", "min_length", "max_length", "pattern", "allowed_values", "unique"}
        for unknown_key in set(constraints_cfg) - _known:
            LOG.warning("[build_pandera_schema] Field '%s': unknown constraint '%s' — ignored", name, unknown_key)

        columns[name] = pa.Column(
            dtype=dtype,
            checks=checks or None,
            nullable=nullable,
            required=required,
            coerce=True,
        )

    return pa.DataFrameSchema(columns=columns, strict=False)


def validate(df: pd.DataFrame, contract: dict[str, Any]) -> None:
    """Validate *df* against the schema defined in *contract* using Pandera.

    Delegates all checks to a :class:`pandera.DataFrameSchema` built from the
    contract via :func:`build_pandera_schema`.  Validation runs in *lazy* mode
    so that every violation is collected before raising.

    Parameters
    ----------
    df:
        The DataFrame to validate.
    contract:
        A contract dict as returned by :func:`load_contract`.

    Raises
    ------
    ContractViolationError
        If one or more violations are found, with the full Pandera failure
        report embedded in the message.
    """
    contract_id: str = contract.get("metadata", {}).get("id", "<unknown>")
    schema = build_pandera_schema(contract)

    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        n = len(exc.failure_cases)
        details = exc.failure_cases.to_string(index=False)
        raise ContractViolationError(
            f"Contract '{contract_id}' violated ({n} failure(s)):\n{details}"
        ) from exc
    except pa.errors.SchemaError as exc:
        raise ContractViolationError(
            f"Contract '{contract_id}' violated:\n  • {exc}"
        ) from exc

    LOG.info(
        "[%s] DataFrame passed contract validation (%d rows, %d columns)",
        contract_id,
        len(df),
        len(df.columns),
    )


def cast_to_contract(df: pd.DataFrame, contract: dict[str, Any]) -> pd.DataFrame:
    """Return a copy of *df* with each column cast to its declared contract type.

    Uses nullable pandas extension dtypes (``Int64``, ``Float64``) so that
    columns containing ``NaN`` are cast cleanly without raising.  Null
    enforcement is left to :func:`validate`.

    Columns absent from the DataFrame are skipped silently.

    Parameters
    ----------
    df:
        The DataFrame to cast.
    contract:
        A contract dict as returned by :func:`load_contract`.
    """
    schema = contract.get("schema", {})
    fields: list[dict[str, Any]] = schema.get("fields", [])
    contract_id: str = contract.get("metadata", {}).get("id", "<unknown>")

    df = df.copy()
    for field in fields:
        name: str = field["name"]
        if name not in df.columns:
            continue
        field_type: str = field.get("type", "string")
        target: str | None = _DTYPE_CAST.get(field_type)
        if target is None:
            continue
        try:
            if target == "datetime64[ns]":
                df[name] = pd.to_datetime(df[name], errors="coerce")
            else:
                df[name] = df[name].astype(target)
        except (ValueError, TypeError) as exc:
            LOG.warning(
                "[%s] Could not cast column '%s' to %s: %s",
                contract_id,
                name,
                target,
                exc,
            )
    return df
