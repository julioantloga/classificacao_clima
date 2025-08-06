from flask import Flask, jsonify, request
from db_config import engine
import pandas as pd
import os
import requests

app = Flask(__name__)

# Executes INSERT into the database
def insert_with_pandas(table_name, data_dict):
    df = pd.DataFrame([data_dict])
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

# Add Tenant
@app.route('/adiciona_tenant/')
def call_to_action():
    url = "https://classificacao-clima-production.up.railway.app/tenant/"
    payload = {"tenant_name": "Nome da empresa"}
    headers = {"Content-Type": "application/json"}

    response = request.post(url, json=payload, headers=headers)
    print(response.json())

# Add Tenant
@app.route('/tenant/', methods=['POST'])
def create_tenant():
    data = request.get_json()
    if not data.get("tenant_name"):
        return jsonify({"error": "tenant_name é obrigatório"}), 400

    insert_with_pandas("tenant", {"tenant_name": data["tenant_name"]})
    return jsonify({"message": "Tenant criado com sucesso"}), 201


@app.route("/pesquisas/")
def listar_pesquisas():
    df = pd.read_sql("SELECT * FROM pesquisa", con=engine)
    return jsonify(df.to_dict(orient="records"))

@app.route("/comentarios/")
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

@app.route("/comentarios/<int:pesquisa_id>/")
def listar_comentarios_por_pesquisa(pesquisa_id):
    df = pd.read_sql(
        "SELECT * FROM comentarios_pesquisa WHERE pesquisa_id = %s",
        con=engine,
        params=(pesquisa_id,)
    )
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)