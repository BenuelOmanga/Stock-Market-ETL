# Stock Market Analytics & Alerting System (2020–2024)

This project delivers a real-time stock market analytics pipeline that simulates daily stock data ingestion, tracks key financial indicators, and triggers anomaly alerts using Prefect and email notifications, all visualized through an interactive Power BI dashboard and a Streamlit ML prediction app.


## Why This Project?

With growing volatility in global stock markets, investors and analysts need:
- Timely insights on price movements
- Automated detection of unusual activity
- Forecasting tools for informed decision-making

We built this pipeline to replicate a real-world, end-to-end stock intelligence system — ideal for decision-makers, analysts, or retail investors seeking live insights without manually tracking the market.


## Dashboard Goals

The Power BI dashboard serves as the main interface for insights and visual analytics. It is built to display:

- **Latest Closing Price** – Shows the most recent value of a stock for a selected date.
- **Daily % Price Change** – Measures short-term momentum, ideal for spotting sharp price shifts.
- **RSI (14-Day Momentum)** – Identifies overbought or oversold conditions; RSI > 70 means overbought, < 30 means oversold.
- **30-Day Price Volatility** – Indicates how drastically the stock price fluctuates, helping in risk evaluation.
- **Stock Price Trend with SMA-30 and SMA-200** – Moving averages smooth out trends for better long-term analysis.
- **Trading Volume vs 20-Day Moving Average** – Helps detect unusual market behavior, like volume surges during price drops.

All metrics are interactive by stock ticker (AAPL, AMZN, GOOGL, META, MSFT) and time range (2020–2024).


## Anomaly Monitoring

This system tracks specific thresholds or abnormal patterns and sends alerts when:

- RSI exceeds safe limits (e.g., > 80 or < 20)
- Volatility spikes beyond average ranges
- Daily % price change is unusually high or negative
- ETL pipeline fails or is interrupted

These events trigger automated emails to stakeholders for rapid response and visibility.

## Email Notifications

The ETL pipeline now includes a fully working email notification system.

- Emails are sent after each ETL run **every 30 minutes** to indicate success or failure.
- The email includes a subject line (`ETL Success` or `ETL Failure`) and a brief message.
- If the ETL fails, the error details are included in the email body.

### Email Configuration

Email credentials and recipient list are stored in a `.env` file for security. This file is not tracked in Git. Here's how to set it up: 

#### env
EMAIL_FROM=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_TO=recipient1@example.com,recipient2@example.com


## Data Source & Ingestion

- **Source**: Yahoo Finance historical data
- **Period Covered**: January 2020 to December 2024
- **Stocks**: AAPL, AMZN, GOOGL, META, MSFT
- **Storage**: Cleaned data saved in SQLite
- **Simulation**: In 2025, the pipeline simulates new daily rows from 2024 data for testing alerts and ML


## Tools & Technologies Used

### Data Cleaning & Storage
- `pandas` and `numpy` for preprocessing
- `sqlite3` to load data into a relational format

### ETL & Automation
- `Prefect` (v3.4.4) used to orchestrate:
  - Daily data simulation
  - ETL pipeline execution
  - Alert flows (no Airflow used in this project)

### Alerting System
- `smtplib` + `python-dotenv` for email integration
- `.env` file used to securely store Gmail credentials and recipient emails

### Machine Learning
- `Ridge Regression` used to predict next-day closing price
- Model training done in Jupyter Notebook
- Model saved as `ridge_model.pkl` and deployed via Streamlit

### Visualization
- `Power BI` for the main dashboard
- Live connection to SQLite for dynamic visuals and updates

