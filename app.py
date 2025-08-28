# app.py
from flask import Flask, request, render_template, send_from_directory, jsonify
from sqlalchemy import text
import pandas as pd
import os
from db_config import engine

from service.classification_service import data_preprocessing
from service.survey_repository import insert_survey

from service.areas_service import create_organizational_chart
from service.areas_repository import insert_areas


app = Flask(__name__, static_folder="static", template_folder="templates")

def read_csv_flex(file_storage):
    """Leitura robusta do CSV vindo de request.files['...']"""
    file_storage.stream.seek(0)
    try:
        return pd.read_csv(file_storage, sep=None, engine="python", encoding="utf-8", on_bad_lines="skip")
    except UnicodeDecodeError:
        file_storage.stream.seek(0)
        return pd.read_csv(file_storage, sep=None, engine="python", encoding="latin-1", on_bad_lines="skip")

@app.route("/")
def home():
    return send_from_directory("static", "index.html")

@app.route("/classifica_comentarios", methods=["POST"])
def classifica_comentarios_route():
    if "campanha" not in request.files or "pessoas" not in request.files:
        return jsonify({"error": "Arquivos 'campanha' e 'pessoas' são obrigatórios."}), 400

    campanha_file = request.files["campanha"]
    pessoas_file  = request.files["pessoas"]
    instancia_areas_file  = request.files["instancia_areas"]
    hierarquia_areas_file  = request.files["hierarquia_areas"]

    try:
        # Ler CSVs
        df_campanha = read_csv_flex(campanha_file)
        df_person     = read_csv_flex(pessoas_file)
        df_instancia_areas     = read_csv_flex(instancia_areas_file)
        df_hierarquia_areas     = read_csv_flex(hierarquia_areas_file)

        # Campos da seção "Configuração"
        config = {
            "nome_pesquisa": request.form.get("nome_pesquisa"),            
            "org_top": request.form.get("org_top", type=int),
            "org_bottom": request.form.get("org_bottom", type=int),
        }

        survey_id = insert_survey(config.get("nome_pesquisa"))

        # Limpa e organiza os dados da campanha antes de rodarmos as classificações
        df_processed = data_preprocessing(df_campanha, df_person, survey_id)

        # Salva as áreas e hierarquia na base de dados 
        df_areas = create_organizational_chart(df_instancia_areas, df_hierarquia_areas, survey_id)
        areas_update = insert_areas(df_areas)
        print(f"Inseridas {areas_update} linhas na tabela area.")

        # Monta tabela HTML (limitando para não pesar a página)
        shown = 200
        display_df = df_areas.head(shown).copy()
        table_html = display_df.to_html(
            classes="striped highlight responsive-table",
            index=False,
            border=0
        )

        return render_template(
            "resultados.html",                 # <-- usa 'resultados.html' (plural)
            message="Classificação concluída com sucesso!",
            meta=config,
            table_html=table_html,
            row_count=len(df_processed),
            shown_rows=len(display_df),
        ), 200

    except ValueError as ve:
        return render_template(
            "resultados.html",
            message=f"Erro de validação: {ve}",
            meta={},
            table_html="",
            row_count=0,
            shown_rows=0,
        ), 400

    except Exception as e:
        return render_template(
            "resultados.html",
            message=f"Erro ao processar: {e}",
            meta={},
            table_html="",
            row_count=0,
            shown_rows=0,
        ), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
