import os
from sqlalchemy import create_engine

DATABASE_URL = os.environ.get("ca8lne8pi75f88.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com")
engine = create_engine(DATABASE_URL)
