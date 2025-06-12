from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")
from prefect import flow, task, get_run_logger
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
import shutil
import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
EMAIL_TO = os.getenv("EMAIL_TO", "").split(",")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

@task
def load_full_dataset(filepath: str) -> pd.DataFrame:
    logger = get_run_logger()
    df = pd.read_csv(filepath, parse_dates=["Date"])
    logger.info(f"Loaded dataset with {len(df)} rows.")
    return df

@task
def get_simulated_today_row(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    real_today = datetime.today()
    simulated_date = pd.to_datetime(f"2024-{real_today.month:02d}-{real_today.day:02d}")
    today_row = df[df["Date"] == simulated_date]

    if today_row.empty:
        logger.warning(f"No data for simulated date: {simulated_date.date()}")
    else:
        logger.info(f"Retrieved {len(today_row)} row(s) for {simulated_date.date()}")
    return today_row

@task
def insert_into_sqlite(row_df: pd.DataFrame):
    logger = get_run_logger()
    if row_df.empty:
        logger.warning("No row to insert into database.")
        return

    sim_date = row_df["Date"].iloc[0].strftime("%Y-%m-%d")
    db_path = f"Data/processed/stocks_data_{sim_date}.db"
    conn = sqlite3.connect(db_path)

    try:
        try:
            existing_df = pd.read_sql_query("SELECT * FROM stock_data", conn)
        except:
            existing_df = pd.DataFrame()

        combined = pd.concat([existing_df, row_df], ignore_index=True)
        deduped = combined.drop_duplicates(subset=["Date", "Ticker"]).copy()
        deduped["Date"] = deduped["Date"].astype(str)
        deduped.to_sql("stock_data", conn, if_exists="replace", index=False)

        logger.info(f"Inserted {len(deduped)} deduplicated row(s) into {db_path}")
    except Exception as e:
        logger.error(f"DB Insert failed: {e}")
    finally:
        conn.close()

@task
def save_to_csv(row_df: pd.DataFrame):
    logger = get_run_logger()
    if row_df.empty:
        logger.warning("No data to save as CSV.")
        return

    sim_date = row_df["Date"].iloc[0].strftime("%Y-%m-%d")
    csv_path = Path(f"Data/processed/stocks_data_{sim_date}.csv")

    try:
        files = sorted(Path("Data/processed").glob("stocks_data_*.csv"))
        if files:
            latest = pd.read_csv(files[-1], parse_dates=["Date"])
            updated = pd.concat([latest, row_df], ignore_index=True).drop_duplicates()
        else:
            updated = row_df
    except Exception:
        updated = row_df

    updated = updated[updated["Date"] <= pd.to_datetime(sim_date)]
    updated.to_csv(csv_path, index=False)
    logger.info(f"Saved CSV to {csv_path} with {len(updated)} total row(s)")

@task
def copy_latest_csv():
    logger = get_run_logger()
    sim_date = datetime.today().strftime("2024-%m-%d")
    source = Path(f"Data/processed/stocks_data_{sim_date}.csv")
    dest = Path("Data/processed/stocks_latest.csv")

    if source.exists():
        shutil.copyfile(source, dest)
        logger.info(f"Copied latest CSV to stocks_latest.csv ({source.name} → stocks_latest.csv)")
    else:
        logger.warning(f"Source CSV file not found: {source}")

@task
def verify_inserted_data(row_df: pd.DataFrame):
    logger = get_run_logger()
    if row_df.empty:
        logger.info("Nothing to verify — no data for simulated date.")
        return

    sim_date = row_df["Date"].iloc[0].strftime("%Y-%m-%d 00:00:00")
    db_path = f"Data/processed/stocks_data_{row_df['Date'].iloc[0].strftime('%Y-%m-%d')}.db"

    conn = sqlite3.connect(db_path)
    query = """
    SELECT * FROM stock_data
    WHERE Date = ?
    ORDER BY Ticker
    """
    df = pd.read_sql_query(query, conn, params=(sim_date,))
    conn.close()

    logger.info(f"Found {len(df)} row(s) for {sim_date} in DB:")
    if not df.empty:
        logger.info(df.to_string(index=False))

@flow(name="Daily Stock Data Loader")
def daily_data_flow():
    try:
        df = load_full_dataset("Data/processed/all_stocks_cleaned.csv")
        today_row = get_simulated_today_row(df)
        insert_into_sqlite(today_row)
        save_to_csv(today_row)
        copy_latest_csv()
        verify_inserted_data(today_row)

        # Final step — optional .db file for Power BI
        if not today_row.empty:
            sim_date = today_row["Date"].iloc[0].strftime("%Y-%m-%d")
            source_db = f"Data/processed/stocks_data_{sim_date}.db"
            dest_db = "Data/processed/latest_stocks_data.db"

            if Path(source_db).exists():
                shutil.copyfile(source_db, dest_db)
                print("Updated Power BI database: latest_stocks_data.db")
            else:
                print(f"Could not find {source_db}, skipping final .db copy.")

        send_status_email(success=True)

    except Exception as e:
        print(f"ETL run failed: {e}")
        send_status_email(success=False, error=str(e))

def send_status_email(success=True, error=None):
    subject = "ETL Success" if success else "ETL Failure"
    body = "ETL pipeline ran successfully." if success else f"ETL failed with error:\n{error}"
    message = MIMEText(body)
    message["Subject"] = subject
    message["From"] = EMAIL_FROM
    message["To"] = ", ".join(EMAIL_TO)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_FROM, EMAIL_TO, message.as_string())
    except Exception as e:
        print(f"Failed to send status email: {e}")

if __name__ == "__main__":
    daily_data_flow()
