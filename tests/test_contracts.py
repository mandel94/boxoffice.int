"""Tests for boxoffice_int.contracts — load_contract, build_pandera_schema, validate, cast_to_contract."""
import logging

import pandas as pd
import pandera as pa
import pytest

from boxoffice_int.contracts import (
    ContractViolationError,
    build_pandera_schema,
    cast_to_contract,
    load_contract,
    validate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _contract(fields: list[dict], required_fields: list[str] | None = None) -> dict:
    return {
        "metadata": {"id": "test-contract"},
        "schema": {"fields": fields},
        "quality": {"completeness": {"required_fields": required_fields or []}},
    }


def _valid_raw_df() -> pd.DataFrame:
    return pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01"] * 3),
        "rank": pd.array([1, 2, 3], dtype="Int64"),
        "title": ["Film A", "Film B", "Film C"],
        "gross_eur": pd.array([100_000, 80_000, 60_000], dtype="Int64"),
        "admissions": pd.array([5_000, 4_000, 3_000], dtype="Int64"),
        "cinemas": pd.array([200, 180, 150], dtype="Int64"),
        "avg_per_cinema_eur": pd.array([500, 444, 400], dtype="Int64"),
        "total_gross_eur": pd.array([100_000, 80_000, 60_000], dtype="Int64"),
    })


# ---------------------------------------------------------------------------
# load_contract
# ---------------------------------------------------------------------------

class TestLoadContract:
    def test_load_by_metadata_id(self):
        c = load_contract("box-office-raw-daily")
        assert c["metadata"]["id"] == "box-office-raw-daily"

    def test_load_by_stem(self):
        c = load_contract("box_office_raw_daily")
        assert c["metadata"]["id"] == "box-office-raw-daily"

    def test_load_film_metadata(self):
        c = load_contract("film-metadata")
        assert c["metadata"]["id"] == "film-metadata"

    def test_load_market_analytics(self):
        c = load_contract("market-analytics-kpi-daily")
        assert c["metadata"]["id"] == "market-analytics-kpi-daily"

    def test_unknown_contract_raises(self):
        with pytest.raises(FileNotFoundError, match="not-found-contract"):
            load_contract("not-found-contract")


# ---------------------------------------------------------------------------
# build_pandera_schema
# ---------------------------------------------------------------------------

class TestBuildPanderaSchema:
    def test_declared_columns_present(self):
        contract = _contract([
            {"name": "date", "type": "date", "nullable": False},
            {"name": "rank", "type": "integer", "nullable": False},
        ])
        schema = build_pandera_schema(contract)
        assert "date" in schema.columns
        assert "rank" in schema.columns

    def test_extra_columns_allowed(self):
        contract = _contract([{"name": "gross_eur", "type": "integer", "nullable": False}])
        schema = build_pandera_schema(contract)
        df = pd.DataFrame({"gross_eur": pd.array([100], dtype="Int64"), "extra": ["x"]})
        schema.validate(df)  # strict=False — must not raise

    def test_minimum_constraint_enforced(self):
        contract = _contract([
            {"name": "rank", "type": "integer", "nullable": False, "constraints": {"minimum": 1}}
        ])
        schema = build_pandera_schema(contract)
        df = pd.DataFrame({"rank": pd.array([0], dtype="Int64")})
        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df)

    def test_maximum_constraint_enforced(self):
        contract = _contract([
            {"name": "rank", "type": "integer", "nullable": False, "constraints": {"maximum": 10}}
        ])
        schema = build_pandera_schema(contract)
        df = pd.DataFrame({"rank": pd.array([11], dtype="Int64")})
        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df)

    def test_allowed_values_constraint_enforced(self):
        contract = _contract([
            {"name": "lang", "type": "string", "nullable": True,
             "constraints": {"allowed_values": ["it", "en"]}}
        ])
        schema = build_pandera_schema(contract)
        df = pd.DataFrame({"lang": ["fr"]})
        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df)

    def test_unknown_constraint_logs_warning(self, caplog):
        contract = _contract([
            {"name": "col", "type": "string", "nullable": True,
             "constraints": {"not_a_real_constraint": True}}
        ])
        with caplog.at_level(logging.WARNING, logger="boxoffice_int.contracts"):
            build_pandera_schema(contract)
        assert "not_a_real_constraint" in caplog.text


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

class TestValidate:
    def test_valid_dataframe_passes(self):
        contract = load_contract("box-office-raw-daily")
        validate(_valid_raw_df(), contract)  # must not raise

    def test_negative_gross_raises(self):
        df = _valid_raw_df()
        df["gross_eur"] = pd.array([-1, 80_000, 60_000], dtype="Int64")
        with pytest.raises(ContractViolationError):
            validate(df, load_contract("box-office-raw-daily"))

    def test_rank_above_max_raises(self):
        df = _valid_raw_df()
        df["rank"] = pd.array([11, 2, 3], dtype="Int64")
        with pytest.raises(ContractViolationError):
            validate(df, load_contract("box-office-raw-daily"))

    def test_rank_below_min_raises(self):
        df = _valid_raw_df()
        df["rank"] = pd.array([0, 2, 3], dtype="Int64")
        with pytest.raises(ContractViolationError):
            validate(df, load_contract("box-office-raw-daily"))

    def test_missing_required_column_raises(self):
        df = _valid_raw_df().drop(columns=["gross_eur"])
        with pytest.raises(ContractViolationError):
            validate(df, load_contract("box-office-raw-daily"))

    def test_error_message_contains_contract_id(self):
        df = _valid_raw_df()
        df["gross_eur"] = pd.array([-1, 80_000, 60_000], dtype="Int64")
        with pytest.raises(ContractViolationError, match="box-office-raw-daily"):
            validate(df, load_contract("box-office-raw-daily"))


# ---------------------------------------------------------------------------
# cast_to_contract
# ---------------------------------------------------------------------------

class TestCastToContract:
    def test_integer_column_cast(self):
        contract = _contract([{"name": "gross_eur", "type": "integer", "nullable": False}])
        df = pd.DataFrame({"gross_eur": ["1000", "2000"]})
        result = cast_to_contract(df, contract)
        assert str(result["gross_eur"].dtype) == "Int64"

    def test_date_column_cast(self):
        contract = _contract([{"name": "date", "type": "date", "nullable": False}])
        df = pd.DataFrame({"date": ["2026-01-01", "2026-01-02"]})
        result = cast_to_contract(df, contract)
        assert str(result["date"].dtype) == "datetime64[ns]"

    def test_float_column_cast(self):
        contract = _contract([{"name": "price", "type": "float", "nullable": True}])
        df = pd.DataFrame({"price": ["9.50", "12.00"]})
        result = cast_to_contract(df, contract)
        assert str(result["price"].dtype) == "Float64"

    def test_missing_column_skipped_silently(self):
        contract = _contract([{"name": "rank", "type": "integer", "nullable": False}])
        df = pd.DataFrame({"title": ["Film A"]})
        result = cast_to_contract(df, contract)
        assert "rank" not in result.columns

    def test_original_df_not_mutated(self):
        contract = _contract([{"name": "gross_eur", "type": "integer", "nullable": False}])
        df = pd.DataFrame({"gross_eur": ["1000"]})
        original_dtype = df["gross_eur"].dtype
        cast_to_contract(df, contract)
        assert df["gross_eur"].dtype == original_dtype
