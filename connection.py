import os 
import pandas as pd 
from sqlalchemy import create_engine 
from dotenv import load_dotenv


load_dotenv() 
engine = create_engine(os.getenv("DB_URL"), pool_pre_ping=True) 
df = pd.read_sql("SELECT COUNT(*) AS products FROM retail.products", engine) 
print(df) 
print("Connected!")