import os
from sqlalchemy import create_engine

#from dotenv import load_dotenv
#load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
BASE_URL = os.environ.get("BASE_URL")

# Corrige o prefixo do Heroku se necessário
if not DATABASE_URL:
    raise ValueError("DATABASE_URL não está definida!")

if DATABASE_URL.startswith("postgres://"):
   DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise ValueError("DATABASE_URL não está definida!")

engine = create_engine(DATABASE_URL)