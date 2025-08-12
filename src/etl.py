import os
import polars as pl
from dotenv import load_dotenv
from loguru import logger

load_dotenv(r"C:\Users\Bruno Henrique\MyProjects\estudo-etl\.env")

log_path = os.getenv("LOG_PATH")
excel_in = os.getenv("EXCEL_PATH")
excel_in2 = os.getenv("EXCEL_PATH2")
excel_out = os.getenv("EXCEL_WRITE_PATH")

logger.add(
    log_path,
    rotation="500 KB",
    retention="5 days",
    encoding="utf-8",
    format="{time} {level} {message}",
    level="INFO",
)

df = pl.read_excel(excel_in)
df3 = pl.read_excel(excel_in2)

def locale_file():
    logger.info(f"File located at {excel_in}")
    logger.info(f"df cols: {df.columns}")
    logger.info(f"df3 cols: {df3.columns}")
    logger.success(f"File found with {len(df)} rows and {len(df.columns)} columns")
    return_type()
    df2 = transform(df)
    generate_xls(df2, excel_out)

def return_type():
    schema = df.schema
    print(schema)
    logger.info(f"Type of file is {schema}")

def transform(input_df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Transforming rows")
    df2 = (
        input_df
        .with_columns(
            pl.when(
                pl.col("courier_shift") == pl.lit("night")
            )
              .then(
                pl.lit("yes")
            )
              .otherwise(
                pl.lit("no")
            )
              .alias("night_shift_allowance")
        )
    )
    logger.success(f"DataFrame modified with {len(df2)} rows and {len(df2.columns)} columns")

    df2 = (
        input_df
        .join(
            df3.select(["xc_case_id", "xc_customer_state", "evc_order_received_at"]),
            on="xc_case_id",
            how="left"
        )
        .rename({"xc_customer_state": "state_works"})
        .with_columns(
            (pl.col("ev_delivery_started_at") - pl.col("evc_order_received_at"))
            .alias("duration")
        )
        .with_columns(
            (pl.col("duration").abs().dt.total_nanoseconds().cast(pl.Time).dt.strftime("%H:%M").alias("duration_hhmm"))
        )
        .drop("duration")
        .filter(pl.col("courier_name") == "Carlos Lima")
    )

    return df2

def generate_xls(df_out: pl.DataFrame, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_out.write_excel(workbook=out_path, worksheet="Sheet1")
    logger.success(f"Generated new XLSX at {out_path}")