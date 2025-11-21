#!/usr/bin/env python3
"""
Unified ingestion script for Week 2.
- Scans /data mounted folder for files (recursive)
- Detects file type by extension, loads into pandas, does light cleaning,
  and writes to Postgres staging tables via SQLAlchemy.
- CSVs are read in chunks.
- Logs ingestion attempts into ingestion_log table.
"""

import os
import re
import sys
import time
import logging
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import sqlalchemy
from sqlalchemy import text

# CONFIG via env vars (change as needed)
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")
DATA_DIR = os.getenv("DATA_DIR", "/data")
CSV_CHUNK_SIZE = int(os.getenv("CSV_CHUNK_SIZE", "20000"))  # rows per chunk for CSVs

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
logging.info(f"Connecting to DB: {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}")
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

# useful helpers
def sanitize_table_name(path: Path) -> str:
    """
    Create table name based on relative path: stg_<folder>_<filename_noext>
    non-alphanum -> underscore, all lower-case.
    """
    rel = path.relative_to(Path(DATA_DIR))
    parts = list(rel.parts)
    # if file at root, parts may be just the file — include parent folder if exists
    name = "_".join(parts)
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    return f"stg_{name}"

def drop_unnamed(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [c for c in df.columns if re.match(r"^Unnamed", str(c))]
    if cols_to_drop:
        logging.debug(f"Dropping columns {cols_to_drop}")
        return df.drop(columns=cols_to_drop)
    return df

def normalize_discount(col: pd.Series) -> pd.Series:
    # remove non-digit characters, guard empty -> NaN, convert to float percent (0-100)
    if col.dtype == object or col.dtype.name == "string":
        s = col.astype(str).str.lower().str.replace("%", "")
        s = s.str.replace("percent", "").str.replace("pct", "").str.replace("%%", "", regex=False)
        # extract digits
        s = s.str.extract(r"([0-9]+(?:\.[0-9]+)?)")[0]
        return pd.to_numeric(s, errors="coerce")
    else:
        return pd.to_numeric(col, errors="coerce")

def normalize_quantity(col: pd.Series) -> pd.Series:
    # extract first integer from textual quantities like '4pcs', '5piece', '6PC'
    s = col.astype(str).str.extract(r"([0-9]+)")[0]
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def normalize_estimated_arrival(col: pd.Series) -> pd.Series:
    """
    Converts formats like '10days', '3 days', '11d', '5day' → integer number of days.
    Returns Int64 with nullable support.
    """
    s = col.astype(str)

    # Extract the number before 'day' or 'days'
    extracted = s.str.extract(r"(\d+)\s*day", expand=False)

    return pd.to_numeric(extracted, errors="coerce").astype("Int64")

def light_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_unnamed(df)

    for c in df.columns:
        lc = c.lower()

        # discount column
        if "discount" in lc:
            df[c] = normalize_discount(df[c])

        # quantity column
        if "quantity" in lc:
            df[c] = normalize_quantity(df[c])

        # estimated arrival column
        if "estimated" in lc and "arrival" in lc:
            logging.info("Normalizing estimated arrival column: %s", c)
            df[c] = normalize_estimated_arrival(df[c])

        # any date-like column
        if re.search(r"(date|transaction_date|creation_date)", lc):
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce")
            except:
                pass

    return df

def write_chunk_to_db(df: pd.DataFrame, table_name: str):
    # attempt to append, create table if not exists
    if df.empty:
        logging.info("Empty chunk, skipping write.")
        return
    try:
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=5000)
    except Exception as e:
        logging.exception("Failed to write to DB for table %s: %s", table_name, e)
        raise

def log_ingestion(file_path: str, table_name: str, rows: int, status: str, message: str = None):
    # create ingestion_log table if not exists and insert a row
    create_stmt = text("""
    CREATE TABLE IF NOT EXISTS ingestion_log (
      id SERIAL PRIMARY KEY,
      file_path TEXT,
      table_name TEXT,
      rows_ingested BIGINT,
      status TEXT,
      message TEXT,
      ts TIMESTAMPTZ DEFAULT now()
    );
    """)
    with engine.begin() as conn:
        conn.execute(create_stmt)
        insert_stmt = text("INSERT INTO ingestion_log (file_path, table_name, rows_ingested, status, message) VALUES (:file_path, :table_name, :rows, :status, :message)")
        conn.execute(insert_stmt, {"file_path": file_path, "table_name": table_name, "rows": rows, "status": status, "message": message})

def detect_delimiter(path: Path):
    """
    Detect the dominant delimiter in the first few KB of the file.
    Handles comma, tab, semicolon, pipe.
    Falls back to comma if unclear.
    """
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            sample = f.read(4096)
    except Exception:
        return ","

    counts = {
        ",": sample.count(","),
        "\t": sample.count("\t"),
        ";": sample.count(";"),
        "|": sample.count("|")
    }

    # pick the delimiter with the highest occurrences
    sep = max(counts, key=counts.get)

    # if no delimiter at all, default to comma
    if counts[sep] == 0:
        return ","

    # special case: CSV almost always uses comma, 
    # unless tab clearly dominates
        
    return sep

def process_csv(path: Path, table_name: str):
    """
    Load CSV or TSV (tab-separated) with automatic delimiter detection.
    Handles quoting issues, BOM, and pandas fallback.
    """

    logging.info(f"Processing CSV/TSV: {path}")
    total_rows = 0

    # 1. Detect delimiter
    sep = detect_delimiter(path)
    logging.info(f"Detected delimiter for {path.name}: {repr(sep)}")

    try:
        # 2. Read in chunks with correct delimiter
        for chunk in pd.read_csv(
            path,
            chunksize=CSV_CHUNK_SIZE,
            sep=sep,
            engine="python",
            encoding="utf-8-sig",
            quotechar='"',
            doublequote=True,
            on_bad_lines="skip"   # pandas 2.x compatible
        ):
            logging.debug(f"Preview for {table_name}:\n{chunk.head()}")

            # Clean + load
            chunk = light_clean(chunk)
            write_chunk_to_db(chunk, table_name)
            total_rows += len(chunk)

        log_ingestion(str(path), table_name, total_rows, "success", None)
        logging.info(f"Finished ingesting {path.name} → {total_rows} rows")

    except Exception as e:
        logging.exception(f"Error processing CSV file: {path}")
        log_ingestion(str(path), table_name, total_rows, "failed", str(e))

def process_parquet(path: Path, table_name: str):
    logging.info("Processing parquet: %s", path)
    try:
        df = pd.read_parquet(path)
        df = light_clean(df)
        write_chunk_to_db(df, table_name)
        log_ingestion(str(path), table_name, len(df), "success", None)
        logging.info("Finished parquet %s -> %d rows", path, len(df))
    except Exception as e:
        logging.exception("Error processing parquet %s", path)
        log_ingestion(str(path), table_name, 0, "failed", str(e))

def process_json(path: Path, table_name: str):
    logging.info("Processing json: %s", path)
    try:
        df = pd.read_json(path, lines=False)
        df = light_clean(df)
        write_chunk_to_db(df, table_name)
        log_ingestion(str(path), table_name, len(df), "success", None)
        logging.info("Finished json %s -> %d rows", path, len(df))
    except Exception as e:
        logging.exception("Error processing json %s", path)
        log_ingestion(str(path), table_name, 0, "failed", str(e))

def process_excel(path: Path, table_name: str):
    logging.info("Processing excel: %s", path)
    try:
        df = pd.read_excel(path)
        df = light_clean(df)
        write_chunk_to_db(df, table_name)
        log_ingestion(str(path), table_name, len(df), "success", None)
        logging.info("Finished excel %s -> %d rows", path, len(df))
    except Exception as e:
        logging.exception("Error processing excel %s", path)
        log_ingestion(str(path), table_name, 0, "failed", str(e))

def process_pickle(path: Path, table_name: str):
    logging.info("Processing pickle: %s", path)
    try:
        df = pd.read_pickle(path)
        df = light_clean(df)
        write_chunk_to_db(df, table_name)
        log_ingestion(str(path), table_name, len(df), "success", None)
        logging.info("Finished pickle %s -> %d rows", path, len(df))
    except Exception as e:
        logging.exception("Error processing pickle %s", path)
        log_ingestion(str(path), table_name, 0, "failed", str(e))

def process_html(path: Path, table_name: str):
    logging.info("Processing html: %s", path)
    try:
        dfs = pd.read_html(path)
        # if multiple tables, load them with suffix
        total = 0
        for i, df in enumerate(dfs):
            df = light_clean(df)
            tn = table_name if i == 0 else f"{table_name}_tbl{i}"
            write_chunk_to_db(df, tn)
            total += len(df)
        log_ingestion(str(path), table_name, total, "success", None)
        logging.info("Finished html %s -> %d rows across %d tables", path, total, len(dfs))
    except Exception as e:
        logging.exception("Error processing html %s", path)
        log_ingestion(str(path), table_name, 0, "failed", str(e))

def process_file(path: Path):
    ext = path.suffix.lower()
    table_name = sanitize_table_name(path)
    logging.info("Processing %s as %s", path, ext)

    if ext in [".csv"]:
        process_csv(path, table_name)
    elif ext in [".parquet", ".pq"]:
        process_parquet(path, table_name)
    elif ext in [".json"]:
        process_json(path, table_name)
    elif ext in [".xlsx", ".xls"]:
        process_excel(path, table_name)
    elif ext in [".pkl", ".pickle"]:
        process_pickle(path, table_name)
    elif ext in [".html", ".htm"]:
        process_html(path, table_name)
    else:
        logging.warning("Unsupported file type %s for file %s", ext, path)
        log_ingestion(str(path), table_name, 0, "skipped", f"Unsupported ext {ext}")

def main():
    data_root = Path(DATA_DIR)
    if not data_root.exists():
        logging.error("DATA_DIR does not exist: %s", DATA_DIR)
        sys.exit(1)

    # walk files
    file_count = 0
    for p in data_root.rglob("*"):
        if p.is_file():
            file_count += 1
            process_file(p)

    logging.info("Processed %d files from %s", file_count, DATA_DIR)

if __name__ == "__main__":
    main()
