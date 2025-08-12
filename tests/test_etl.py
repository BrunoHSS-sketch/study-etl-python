# tests/test_etl.py
import importlib
from datetime import datetime
import polars as pl


def test_locale_file_executa(monkeypatch, tmp_path):
    etl = importlib.import_module("etl")

    df = pl.DataFrame({
        "xc_case_id": [1],
        "courier_shift": ["night"],
        "ev_delivery_started_at": [datetime(2025, 8, 12, 8, 0)],
        "courier_name": ["Carlos Lima"],
    }).with_columns(
        pl.col("ev_delivery_started_at").cast(pl.Datetime)
    )

    df3 = pl.DataFrame({
        "xc_case_id": [1],
        "xc_customer_state": ["SP"],
        "evc_order_received_at": [datetime(2025, 8, 12, 7, 30)],
    }).with_columns(
        pl.col("evc_order_received_at").cast(pl.Datetime)
    )

    etl.df = df
    etl.df3 = df3

    etl.excel_in = "fake_in.xlsx"

    calls = []
    monkeypatch.setattr(etl.logger, "info",    lambda msg, *a, **k: calls.append(("info", str(msg))))
    monkeypatch.setattr(etl.logger, "success", lambda msg, *a, **k: calls.append(("success", str(msg))))
    monkeypatch.setattr(etl, "generate_xls",   lambda df_out, p: calls.append(("write", p)))

    etl.locale_file()

def test_return_type_log(monkeypatch):
    import etl
    etl.df  = pl.DataFrame({"a": [1]})
    etl.df3 = pl.DataFrame({"b": [1]})

    seen = []
    monkeypatch.setattr(etl.logger, "info", lambda msg, *a, **k: seen.append(str(msg)))

    etl.return_type()
    assert any("Type of file is" in m for m in seen)


def test_transform_and_write_excel(tmp_path):
    import etl

    df = pl.DataFrame({
        "xc_case_id": [1, 2, 3],
        "ev_delivery_started_at": [
            datetime(2025, 8, 12, 8, 45),
            datetime(2025, 8, 12, 10, 0),
            datetime(2025, 8, 12, 11, 15),
        ],
        "courier_name": ["Carlos Lima", "Pedro Augusto", "Carlos Lima"],
        "courier_shift": ["night", "morning", "night"],
    }).with_columns(
        pl.col("ev_delivery_started_at").cast(pl.Datetime)
    )

    df3 = pl.DataFrame({
        "xc_case_id": [1, 2, 3],
        "xc_customer_state": ["SP", "RJ", "SC"],
        "evc_order_received_at": [
            datetime(2025, 8, 12, 8, 0),
            datetime(2025, 8, 12, 9, 40),
            datetime(2025, 8, 12, 10, 30),
        ],
    }).with_columns(
        pl.col("evc_order_received_at").cast(pl.Datetime)
    )

    out = etl.transform(df, df3)
    out_path = tmp_path / "out.xlsx"
    etl.generate_xls(out, str(out_path))
    assert out_path.exists() and out_path.stat().st_size > 0
