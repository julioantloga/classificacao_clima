from sqlalchemy import text
import pandas as pd
import os
from db_config import engine

#OK
def get_survey_config(survey_id):

    with engine.begin() as conn:
        
        df = pd.read_sql(
            text("""
                SELECT *
                FROM config_empresa
                WHERE survey_id = :sid
                LIMIT 1
            """),
            conn, params={"sid": survey_id}
        )

    if not df.empty:
        config = df.loc[0].to_dict()
    else:
        config = {
        "sobre_empresa": "",
        "valores": "",
        "politicas": "",
        "canais_comunicacao": "",
        "armazenamento_info": "",
        "acoes_rh": "",
        "metricas": "",
        "survey_id": survey_id
    }
        
    return config 

#OK
def update_survey_config(data):
    print("ENTROU NO UPDATE")
    with engine.begin() as conn:
        # Remove registros anteriores da mesma survey
        x = conn.execute(
            text("DELETE FROM config_empresa WHERE survey_id = :sid"),
            {"sid": data["survey_id"]}
        )

        # Insere novo registro
        x = conn.execute(
            text("""
                INSERT INTO config_empresa (
                    sobre_empresa,
                    valores,
                    politicas,
                    canais_comunicacao,
                    armazenamento_info,
                    acoes_rh,
                    metricas,
                    survey_id
                ) VALUES (
                    :sobre_empresa,
                    :valores,
                    :politicas,
                    :canais_comunicacao,
                    :armazenamento_info,
                    :acoes_rh,
                    :metricas,
                    :survey_id
                )
            """),
            data
        )
    
    return data["survey_id"]

