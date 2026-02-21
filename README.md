# Lissen User Journey Analysis

This project analyzes the user onboarding funnel for the Lissen app using data from BigQuery and visualizes the results with Streamlit.

## Features
- Data loading from BigQuery
- Onboarding funnel visualization
- Breakdown of user progression and drop-off

### `user_journey_v2.sql` Logic adjustments

The onboarding analysis uses `user_journey_v2.sql` query, modified from `user_journey` view. The query no longer filters exclusively for sessions that have visited an `/access/%` page.
```sql
    -- AND event_name LIKE '/access/%'
```

## Directory Structure
```
lissen-user-journey/
├── README.md
├── pyproject.toml
├── Makefile
├── onboarding/
│   ├── streamlit_app.py
│   ├── data_loader.py
│   ├── user_journey_v2.sql
│   └── config.yaml
└── uv.lock
```

## Usage

- Install dependencies: `uv sync`
- Run the Streamlit app: `make run`
