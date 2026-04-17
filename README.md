# SK Forecast

SK Forecast is a forecasting and business intelligence dashboard designed to turn scattered operational and commercial data into actionable business decisions.

It was built for real e-commerce operations, with a focus on consolidating marketplace, financial, and marketing data into one system that supports forecasting, cash flow planning, and performance tracking.

## What it does

SK Forecast centralizes data from multiple sources and transforms it into a structured decision-support tool.

The dashboard currently supports:

- sales forecasting
- units forecasting
- cash flow projection
- ROAS tracking
- marketplace data ingestion
- bank balance monitoring
- acquisition expense tracking
- manual and automated data update flows

## Main Features

### 1. Sales and Units Forecasting
Forecasts commercial performance using historical sales and units data.

Supports:
- daily view
- weekly view
- monthly view

Outputs:
- forecast
- min scenario
- max scenario
- interactive charts with forecast bands

### 2. Cash Flow Projection
Projects future balance using:

- forecasted sales
- already planned expenses
- latest registered bank balance

This allows the system to estimate future liquidity under different scenarios.

### 3. ROAS Tracking
Measures return on ad spend by comparing weekly sales against weekly acquisition expense across channels such as:

- Amazon
- Mercado Libre
- Facebook
- Tiktok
- Google
- UGC & collaborations
- Others

### 4. Marketplace Data Ingestion
The dashboard supports different ingestion flows depending on source:

- **Mercado Libre**
  - API-based update flow
  - token refresh handling
  - normalization of product titles
  - conversion into sales and units structures

- **Amazon**
  - TXT file upload
  - multi-file support
  - SKU normalization
  - conversion into sales and units structures

### 5. Financial Data Management
Supports upload and maintenance of:

- expenses
- bank balances
- acquisition expense
- marketplace-derived sales and units

### 6. Environment Selector
The app includes two access modes:

- **Shinny Skin**
  - private environment
  - password-protected
  - uses real operational data

- **Test**
  - public demo environment
  - uses sample CSV data
  - safe to share without exposing private information

This allows the dashboard to function both as an internal business tool and as a public portfolio/demo product.

---

## Why it was built

Many businesses manage critical decisions with fragmented spreadsheets, marketplace exports, and disconnected financial records.

SK Forecast was created to solve that problem by offering:

- cleaner data structure
- centralized visualization
- forward-looking forecasts
- faster financial interpretation
- a reusable operational dashboard for business decision-making

It is not just a reporting tool. It is a forecasting system built around actual business workflows.

---

## Tech Stack

- **Python**
- **Streamlit**
- **Pandas**
- **SQLAlchemy**
- **Prophet**
- **PostgreSQL / SQLite**
- **dotenv**
- **Plotly**

---

## Project Structure

```bash
sk_forecast/
├── main.py
├── database.py
├── models.py
├── settings.py
├── requirements.txt
├── README.md
├── sample_data/
│   ├── example_sales.csv
│   ├── example_units.csv
│   ├── example_expenses.csv
│   ├── example_banks.csv
│   └── example_acquisition_expense.csv
└── services/
    ├── acquisition_expense_service.py
    ├── amazon_upload_service.py
    ├── banks_service.py
    ├── expenses_service.py
    ├── forecast_prepare_service.py
    ├── forecast_run_service.py
    ├── forecast_service.py
    ├── ml_update_service.py
    ├── roas_service.py
    ├── sales_service.py
    ├── test_data_service.py
    └── units_service.py