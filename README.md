# SK Forecast

SK Forecast is a data-driven forecasting dashboard built to clean, structure, and analyze operational datasets for business decision-making.

The project is designed to transform raw commercial and financial data into actionable forecasts, with a practical focus on:

- sales forecasting
- cash flow projection
- ROAS tracking
- weekly and monthly business monitoring
- marketplace data consolidation

## Purpose

This project helps convert fragmented business data from marketplaces, expenses, bank balances, and acquisition costs into a centralized forecasting system that can be accessed through an interactive dashboard.

Its goal is not only to visualize historical performance, but also to support forward-looking decisions through structured forecasting models.

## Main Features

- **Data cleaning and transformation**
  - prepares raw datasets from different sources for analysis
  - standardizes marketplace and product-level data
  - converts operational files into database-ready structures

- **Sales forecasting**
  - forecasts sales using historical transactional data
  - supports daily, weekly, and monthly aggregation

- **Cash flow projection**
  - combines projected sales, planned expenses, and current balances
  - estimates future cash position under different forecast scenarios

- **ROAS tracking**
  - calculates return on ad spend from weekly acquisition expense data
  - compares sales performance against marketing investment

- **Marketplace uploads**
  - supports structured uploads from multiple data sources
  - includes automated and manual update flows for marketplaces such as Mercado Libre and Amazon

## Tech Stack

- **Python**
- **Streamlit**
- **Pandas**
- **SQLAlchemy**
- **Prophet**
- **PostgreSQL / SQLite**
- **dotenv for environment variable management**

## Use Cases

SK Forecast is especially useful for:

- e-commerce businesses
- marketplace sellers
- operators managing multi-channel sales
- teams that need quick financial visibility
- founders who want forecasting tools without relying on spreadsheets alone

## Project Structure

```bash
sk_forecast/
├── main.py
├── database.py
├── models.py
├── settings.py
├── requirements.txt
├── README.md
└── services/
    ├── amazon_upload_service.py
    ├── acquisition_expense_service.py
    ├── banks_service.py
    ├── expenses_service.py
    ├── forecast_prepare_service.py
    ├── forecast_run_service.py
    ├── forecast_service.py
    ├── ml_update_service.py
    ├── roas_service.py
    ├── sales_service.py
    └── units_service.py
