from pathlib import Path

import pandas as pd


def read(table: str, limit: int = 1_000_000_000, schema: str = "input"):
    df = next(
        pd.read_sql_table(
            table,
            "postgresql://db3_user:abcdef123@proxinea-db3.postgres.database.azure.com:5432/db3",
            schema=schema,
            chunksize=limit,
        )
    )
    return df


path = Path(__file__).parents[1]


def save():
    tables = ["account_info", "interactions", "product_holdings"]

    for table in tables:
        df = read(table)
        df.to_csv(path / f"data/{table}.csv", index=False)


def load():
    tables = ["account_info", "interactions", "product_holdings"]
    res = {}
    for table in tables:
        df = pd.read_csv(path / f"data/{table}.csv")
        res[table] = df
    return res
