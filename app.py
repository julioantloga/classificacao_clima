from flask import Flask, jsonify, request
from db_config import engine
import pandas as pd

app = Flask(__name__)

@app.route("/tenants")
def listar_tenants():
    df = pd.read_sql("SELECT * FROM tenant", con=engine)
    return jsonify(df.to_dict(orient="records"))

@app.route("/pesquisas")
def listar_pesquisas():
    df = pd.read_sql("SELECT * FROM pesquisa", con=engine)
    return jsonify(df.to_dict(orient="records"))

@app.route("/comentarios")
def listar_comentarios():
    query = "SELECT * FROM comentarios_pesquisa"
    filtros = []

    tema = request.args.get("tema")
    intencao = request.args.get("intencao")

    if tema:
        filtros.append(f"tema ILIKE '%{tema}%'")
    if intencao:
        filtros.append(f"intencao ILIKE '%{intencao}%'")

    if filtros:
        query += " WHERE " + " AND ".join(filtros)

    df = pd.read_sql(query, con=engine)
    return jsonify(df.to_dict(orient="records"))

@app.route("/comentarios/<int:pesquisa_id>")
def listar_comentarios_por_pesquisa(pesquisa_id):
    df = pd.read_sql(
        "SELECT * FROM comentarios_pesquisa WHERE pesquisa_id = %s",
        con=engine,
        params=(pesquisa_id,)
    )
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    app.run()
