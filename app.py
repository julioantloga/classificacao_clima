from flask import Flask, jsonify
from db_config import engine
import pandas as pd

app = Flask(__name__)

@app.route("/comentarios")
def listar_comentarios():
    df = pd.read_sql("SELECT * FROM comentarios_pesquisa", con=engine)
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    app.run()
