from flask import Flask, jsonify, request, send_from_directory
from db_config import engine, BASE_URL
import pandas as pd
import os
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return send_from_directory('static', 'index.html')

@app.route('/classifica_comentarios', methods=['POST'])
def classifica_comentarios():
    if 'campanha' not in request.files or 'pessoas' not in request.files:
        return jsonify({"error": "Arquivos não enviados"}), 400

    campanha_file = request.files['campanha']
    pessoas_file = request.files['pessoas']

    try:
        # Carrega os DataFrames dos arquivos CSV
        df_campanha = pd.read_csv(campanha_file)
        df_pessoas = pd.read_csv(pessoas_file)

        # Aqui entra a lógica de classificação real:
        # Exemplo: classificar_comentarios(df_campanha, df_pessoas)
        # Para fins de protótipo, apenas um print:
        print("Campanha:", df_campanha.head())
        print("Pessoas:", df_pessoas.head())

        return jsonify({"message": "Classificação concluída com sucesso!"})
    except Exception as e:
        return jsonify({"error": f"Erro ao processar arquivos: {str(e)}"}), 500


""""
# Executes INSERT into the database
def insert_with_pandas(table_name, data_dict):
    df = pd.DataFrame([data_dict])
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    
# Add Tenant
@app.route('/adiciona_tenant/')
def call_to_action():
    url = f"{BASE_URL}/tenant/"
    payload = {"tenant_name": "Nome da empresa"}
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    return jsonify(response.json()), response.status_code

# Add Tenant
@app.route('/tenant/', methods=['POST'])
def create_tenant():
    data = request.get_json()
    if not data.get("tenant_name"):
        return jsonify({"error": "tenant_name é obrigatório"}), 400

    insert_with_pandas("tenant", {"tenant_name": data["tenant_name"]})
    return jsonify({"message": "Tenant criado com sucesso"}), 201
"""