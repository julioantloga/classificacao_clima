from flask import Flask, jsonify, request
from db_config import engine, BASE_URL
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