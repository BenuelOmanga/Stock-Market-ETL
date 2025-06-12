import os
import shutil
import sqlite3
import pandas as pd
from datetime import datetime
from prefect import flow, task
from prefect_email import EmailServerCredentials, email_send_message
from dotenv import load_dotenv


# Load environment variables
load_dotenv(dotenv_path=".env")

# === CONSTANTS === #
TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_DB_PATH = f"Data/processed/stocks_data_{TODAY}.db"
LATEST_DB_PATH = "Data/processed/latest_stocks_data.db"
TO_ADDRESSES = os.getenv("EMAIL_TO", "").split(",")

# === TASKS === #
@task
def load_latest_data():
    if not os.path.exists(DAILY_DB_PATH):
        print(f"❌ Database for today not found: {DAILY_DB_PATH}")
        print("👉 Using most recent backup instead.")

        folder = os.path.dirname(DAILY_DB_PATH)
        db_files = [f for f in os.listdir(folder) if f.startswith("stocks_data_") and f.endswith(".db")]
        if not db_files:
            raise FileNotFoundError("No backup .db files found in processed folder.")
        db_files.sort(reverse=True)
        fallback_path = os.path.join(folder, db_files[0])
        shutil.copyfile(fallback_path, LATEST_DB_PATH)
        print(f"✅ Copied fallback DB: {fallback_path} → latest_stocks_data.db")
    else:
        shutil.copyfile(DAILY_DB_PATH, LATEST_DB_PATH)
        print(f"✅ Copied today's DB to latest_stocks_data.db")

    conn = sqlite3.connect(LATEST_DB_PATH)
    df = pd.read_sql_query("SELECT * FROM stock_data", conn)
    conn.close()

    print(f"✅ Loaded {len(df)} row(s) from {LATEST_DB_PATH}")
    return df

@task
def detect_anomalies(df: pd.DataFrame) -> str:
    anomalies = df[df["Volatility_30"] > 3.0]
    if anomalies.empty:
        print("✅ No anomalies found today.")
        return ""
    return "\n".join(
        f"{row['Ticker']} volatility high: {row['Volatility_30']:.2f}%" for _, row in anomalies.iterrows()
    )

@task
def send_email_report(alert_message: str):
    if not alert_message:
        print("ℹ️ No alert email sent.")
        return

    email_block = EmailServerCredentials.load("gmail-notifier")

    import asyncio
    asyncio.run(email_send_message(
        email_server_credentials=email_block,
        subject=f"Daily Stock Volatility Alerts for {TODAY}",
        msg=alert_message,
        email_to=TO_ADDRESSES,
    ))
    print("📧 Alert email sent to recipients.")

# === FLOW === #
@flow(name="Stock Anomaly Alert Flow")
def anomaly_alert_flow():
    df = load_latest_data()
    alert_message = detect_anomalies(df)
    send_email_report(alert_message)

if __name__ == "__main__":
    anomaly_alert_flow()
