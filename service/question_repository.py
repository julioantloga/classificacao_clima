from typing import Dict, List
from sqlalchemy import text
from db_config import engine

def get_existing_questions(survey_id: int) -> Dict[str, int]:
    sql = text("""
        SELECT question_name, question_id
        FROM question
        WHERE question_survey_id = :sid
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"sid": survey_id}).fetchall()
    return {r[0]: r[1] for r in rows}

def insert_questions(survey_id: int, names: List[str]) -> Dict[str, int]:
    """
    Garante perguntas para o survey_id.
    Retorna {question_name -> question_id}.
    """
    names = [n for n in {str(n).strip() for n in names} if n]
    if not names:
        return {}

    existing = get_existing_questions(survey_id)
    to_insert = [n for n in names if n not in existing]

    if to_insert:
        sql_ins = text("""
            INSERT INTO question (question_name, question_survey_id)
            VALUES (:name, :sid)
            RETURNING question_id, question_name
        """)
        with engine.begin() as conn:
            for n in to_insert:  # <- uma por vez (RETURNING seguro)
                row = conn.execute(sql_ins, {"name": n, "sid": survey_id}).fetchone()
                if row:
                    qid, qname = row[0], row[1]
                    existing[qname] = qid

    return existing
