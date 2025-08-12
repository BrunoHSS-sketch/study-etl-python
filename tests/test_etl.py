import importlib
import polars as pl
import warnings
from polars.exceptions import MapWithoutReturnDtypeWarning

warnings.filterwarnings("ignore", category=MapWithoutReturnDtypeWarning)

def test_transform_and_write_excel(tmp_path, monkeypatch):
    df = pl.DataFrame({
        "xc_case_id": [1, 2, 3],
        "ev_delivery_started_at": [
            pl.datetime(2025, 8, 12, 8, 45),
            pl.datetime(2025, 8, 12, 10,  0),
            pl.datetime(2025, 8, 12, 11, 15),
        ],
        "ev_delivery_completed_at": [
            pl.datetime(2025, 8, 12, 9,  0),
            pl.datetime(2025, 8, 12, 10, 15),
            pl.datetime(2025, 8, 12, 11, 45),
        ],
        "evs_delivery_process": [
            pl.datetime(2025, 8, 12, 8, 50),
            pl.datetime(2025, 8, 12, 9, 30),
            pl.datetime(2025, 8, 12, 11,  0),
        ],
        "evc_delivery_process": [
            pl.datetime(2025, 8, 12, 8, 55),
            pl.datetime(2025, 8, 12, 9, 45),
            pl.datetime(2025, 8, 12, 11, 10),
        ],
        "customer_rating": [3, 5, 4],
        "courier_name": ["Carlos Lima", "Carlos Lima", "Carlos Lima"],
        "courier_shift": ["night", "morning", "night"],
    })

    df3 = pl.DataFrame({
        "xc_case_id": [1, 2, 3],
        "xc_customer_state": ["SP", "RJ", "SC"],
        "evc_order_received_at": [
            pl.datetime(2025, 8, 12, 8, 0),
            pl.datetime(2025, 8, 12, 9, 40),
            pl.datetime(2025, 8, 12, 10, 30),
        ],
    })

    excel_in  = tmp_path / "df.xlsx"
    excel_in2 = tmp_path / "df3.xlsx"
    out_path  = tmp_path / "out.xlsx"
    log_path  = tmp_path / "etl.log"

    df.write_excel(workbook=str(excel_in), worksheet="Sheet1")
    df3.write_excel(workbook=str(excel_in2), worksheet="Sheet1")

    monkeypatch.setenv("LOG_PATH",            str(log_path))
    monkeypatch.setenv("EXCEL_PATH",          str(excel_in))
    monkeypatch.setenv("EXCEL_PATH2",         str(excel_in2))
    monkeypatch.setenv("EXCEL_WRITE_PATH",    str(out_path))

    etl = importlib.import_module("etl")
    etl.df = etl.df.with_columns(
        pl.col("ev_delivery_started_at").cast(pl.Datetime, strict=False)
    )
    etl.df3 = etl.df3.with_columns(
        pl.col("evc_order_received_at").cast(pl.Datetime, strict=False)
    )

    out = etl.transform(etl.df)

    etl.generate_xls(out, str(out_path))

    hhmm = out.select("duration_hhmm").to_series().to_list()
