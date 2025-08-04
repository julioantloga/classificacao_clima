import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
#DATABASE_URL = os.environ.get("postgres://u7v0qvouq99ed7:pac5c0429a529fce61761c3776da333dd681668f2bde519e87027063fb0c6d880@ca8lne8pi75f88.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d2i1sotad13i9")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL não está definida!")

engine = create_engine(DATABASE_URL)