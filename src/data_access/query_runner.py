from sqlalchemy import create_engine, text
import pandas as pd

def run_query(db_url, query):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df
