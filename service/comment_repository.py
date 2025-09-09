from typing import Dict, List
from sqlalchemy import text
from db_config import engine

def employee_lookup_map(survey_id: int) -> Dict[str, dict]:
    """
    Retorna {email_lower -> {employee_id, employee_area_id, employee_manager_id}}.
    Útil para cruzar area_id/gestor_id que vêm na df_final.
    """
    sql = text("""
        SELECT lower(employee_email) AS email,
               employee_id,
               employee_area_id,
               employee_manager_id
        FROM employee
        WHERE employee_survey_id = :sid
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"sid": survey_id}).mappings().all()
    out = {}
    
    for r in rows:
        out[r["email"]] = {
            "employee_id": r["employee_id"],
            "employee_area_id": r["employee_area_id"],
            "employee_manager_id": r["employee_manager_id"],
        }
    return out

def insert_comments(rows: List[dict]) -> int:
    """
    rows: [{comment, comment_employee_id, comment_question_id}]
    Retorna quantidade inserida.
    """
    if not rows:
        return 0
    sql = text("""
        INSERT INTO comment (comment, comment_employee_id, comment_question_id)
        VALUES (:comment, :comment_employee_id, :comment_question_id)
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)
