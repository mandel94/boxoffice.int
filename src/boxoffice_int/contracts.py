"""Data contract loader and DataFrame validator.

Implements contract validation at domain boundaries, following Data Mesh
principles: each data product is responsible for ensuring its output
conforms to its published contract before crossing domain boundaries.

Usage
-----
    from boxoffice_int.contracts import load_contract, validate

    contract = load_contract("box-office-raw-daily")
    validate(df, contract)          # raises ContractViolationError on failure
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

LOG = logging.getLogger(__name__)

# Resolved at import time so it works regardless of the working directory.
CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"

# Map contract field types to acceptable pandas dtype strings.
_DTYPE_COMPAT: dict[str, tuple[str, ...]] = {
    "string": ("object", "string"),
    "date": ("object", "datetime64[ns]", "datetime64[us]", "datetime64[s]"),
    "integer": ("int64", "int32", "int16", "int8", "Int64", "Int32"),
    "float": ("float64", "float32", "Float64", "Float32"),
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
        if path.stem == contract_id:
            return contract
    raise FileNotFoundError(
        f"Contract '{contract_id}' not found in {CONTRACTS_DIR}. "
        f"Available: {[p.stem for p in CONTRACTS_DIR.glob('*.yaml')]}"
    )


def validate(df: pd.DataFrame, contract: dict[str, Any]) -> None:
    """Validate *df* against the schema defined in *contract*.

    Checks performed for each declared field:

    1. **Presence** — column must exist (unless ``nullable: true`` and not
       listed in ``quality.completeness.required_fields``).
    2. **Nullability** — non-nullable columns must have zero null values.
    3. **Type** — dtype is compared to the allowed set for the declared type
       (soft check: logs a warning instead of raising, because pandas may
       legitimately represent dates as ``object``).
    4. **Constraints** — ``minimum`` and ``maximum`` are enforced on non-null
       values.

    Parameters
    ----------
    df:
        The DataFrame to validate.
    contract:
        A contract dict as returned by :func:`load_contract`.

    Raises
    ------
    ContractViolationError
        If one or more hard violations are found, with all violations listed.
    """
    schema = contract.get("schema", {})
    fields: list[dict[str, Any]] = schema.get("fields", [])
    contract_id: str = contract.get("metadata", {}).get("id", "<unknown>")
    required_by_quality: list[str] = (
        contract.get("quality", {})
        .get("completeness", {})
        .get("required_fields", [])
    )

    violations: list[str] = []

    for field in fields:
        name: str = field["name"]
        expected_type: str = field.get("type", "string")
        nullable: bool = field.get("nullable", True)
        constraints: dict[str, Any] = field.get("constraints", {})

        # ── 1. Column presence ────────────────────────────────────────────
        if name not in df.columns:
            if not nullable or name in required_by_quality:
                violations.append(f"Missing required column: '{name}'")
            else:
                LOG.warning(
                    "[%s] Optional column '%s' absent — skipping checks",
                    contract_id,
                    name,
                )
            continue

        series = df[name]

        # ── 2. Nullability ────────────────────────────────────────────────
        null_count = int(series.isna().sum())
        if not nullable and null_count > 0:
            violations.append(
                f"Column '{name}' is non-nullable but has {null_count} null value(s)"
            )

        # ── 3. Type (soft — warn only) ────────────────────────────────────
        actual_dtype = str(series.dtype)
        allowed_dtypes = _DTYPE_COMPAT.get(expected_type)
        if allowed_dtypes and actual_dtype not in allowed_dtypes:
            LOG.warning(
                "[%s] Column '%s': expected type '%s' (dtypes %s), got '%s'",
                contract_id,
                name,
                expected_type,
                allowed_dtypes,
                actual_dtype,
            )

        # ── 4. Constraints ────────────────────────────────────────────────
        non_null = series.dropna()
        if "minimum" in constraints:
            below = int((non_null < constraints["minimum"]).sum())
            if below:
                violations.append(
                    f"Column '{name}': {below} value(s) below minimum"
                    f" ({constraints['minimum']})"
                )
        if "maximum" in constraints:
            above = int((non_null > constraints["maximum"]).sum())
            if above:
                violations.append(
                    f"Column '{name}': {above} value(s) above maximum"
                    f" ({constraints['maximum']})"
                )

    if violations:
        bullet_list = "\n".join(f"  • {v}" for v in violations)
        raise ContractViolationError(
            f"Contract '{contract_id}' violated ({len(violations)} issue(s)):\n"
            + bullet_list
        )

    LOG.info(
        "[%s] DataFrame passed contract validation (%d rows, %d columns)",
        contract_id,
        len(df),
        len(df.columns),
    )
