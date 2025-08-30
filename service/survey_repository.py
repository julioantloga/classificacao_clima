from sqlalchemy import text
from db_config import engine

def insert_survey(survey_name: str) -> int | None:
    """
    Insere um registro na tabela 'survey' com o campo survey_name.
    Retorna o id/survey_id inserido (se disponível) ou None.
    """
    if not survey_name or not survey_name.strip():
        raise ValueError("O nome da pesquisa (survey_name) é obrigatório.")

    # Usamos RETURNING * para funcionar com PK chamada 'id' OU 'survey_id'
    query = text("""
        INSERT INTO survey (survey_name)
        VALUES (:survey_name)
        RETURNING *;
    """)

    with engine.begin() as conn:
        row = conn.execute(query, {"survey_name": survey_name.strip()}).mappings().first()

    if not row:
        return None

    # Tenta localizar o nome da PK mais comum
    return row.get("survey_id")