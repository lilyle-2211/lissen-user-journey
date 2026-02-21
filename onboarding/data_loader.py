"""
Data loading from BigQuery.
"""

import pandas as pd
import yaml
from pathlib import Path
from google.cloud import bigquery


def load_data_from_bigquery(use_view_query: bool = False) -> pd.DataFrame:
    """
    Load data from BigQuery using config.yaml.

    Args:
        use_view_query: If True, execute the SQL from user_journey_v2.sql.
                       If False, query the existing table directly.

    Returns:
        DataFrame with player journey
    """
    # Load config from yaml file
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Get BigQuery settings from config
    project_id = config["bigquery"]["project_id"]

    client = bigquery.Client(project=project_id)

    if use_view_query:
        # Read and execute SQL query from user_journey_v2.sql
        sql_path = Path(__file__).parent / "user_journey_v2.sql"
        with open(sql_path, "r") as f:
            query = f.read()
        print("Executing SQL query from user_journey_v2.sql...")
    else:
        # Query existing table directly
        dataset_id = config["bigquery"]["dataset_id"]
        table_id = config["bigquery"]["table_id"]
        full_table = f"`{project_id}.{dataset_id}.{table_id}`"
        query = f"""
        SELECT *
        FROM {full_table}
        """

    df = client.query(query).to_dataframe()
    return df


if __name__ == "__main__":
    df = load_data_from_bigquery(use_view_query=True)
    print(f"Loaded {len(df)} rows")
    print(df.head())
