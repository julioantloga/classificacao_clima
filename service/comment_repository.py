from typing import Dict, List
from sqlalchemy import text
from db_config import engine
import pandas as pd

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
        INSERT INTO comment (comment, comment_employee_id, comment_question_id, comment_survey_id, comment_area_id)
        VALUES (:comment, :comment_employee_id, :comment_question_id, :comment_survey_id, :comment_area_id)
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)

#------------------------------
# NOVO 
#-----------------------------

def insert_themes(rows: List[dict]) -> int:
    """
    Inicia a base de temas da pesquisa:
    """
    if not rows:
        return 0

    sql = text("""
        INSERT INTO theme_ranking (
            area_id, theme_name, score, dissatisfied_score, survey_id
        ) VALUES (
            :area_id, :theme_name, :score, :dissatisfied_score, :survey_id
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)

#OK
def get_comment_perceptions_search(survey_id: int, area_id: int, intention: str, theme: str):
    
    params = {"sid": survey_id}

    # 1) Buscar comentários da pesquisa
    q_comments = """
        SELECT 
          c.comment_id,
          c.comment,
          c.comment_area_id,                   
          q.question_name
        FROM comment c
        JOIN question q ON q.question_id = c.comment_question_id
        WHERE q.question_survey_id = :sid
    """

    if area_id != 0:
        q_comments += " AND c.comment_area_id = :aid"
        params["aid"] = area_id
    
    q_comments = text(q_comments)
    
    # 2) Percepções para os comentários retornados
    query_str = """
        SELECT 
          p.perception_id,
          p.perception_comment_id,
          p.perception_comment_clipping,
          p.perception_theme,
          p.perception_intension
        FROM perception p
        WHERE p.perception_comment_id IN :ids
    """
   
    if intention != "all":
        query_str += " AND p.perception_intension = :intent"
        params["intent"] = intention
    
    if theme != "all":
        query_str += " AND p.perception_theme = :theme"
        params["theme"] = theme

    q_perceptions = text(query_str)

    with engine.begin() as conn:
       
        comments = conn.execute(q_comments, params).mappings().all()

        if not comments:
            return []

        # ids dos comentários
        comment_ids = tuple([c["comment_id"] for c in comments])
        
        # SQLAlchemy precisa de tupla para IN
        percs = conn.execute(q_perceptions, {**params, "ids": comment_ids}).mappings().all()
    
    # Agrupa percepções por comment_id
    percs_by_comment = {}
    for p in percs:
        percs_by_comment.setdefault(p["perception_comment_id"], []).append({
            "perception_id": p["perception_id"],
            "perception_comment_clipping": p["perception_comment_clipping"],
            "perception_theme": p["perception_theme"],
            "perception_intension": p["perception_intension"],
        })

    # Monta rows
    rows = []
    for c in comments:
        perceptions = percs_by_comment.get(c["comment_id"], [])
        if perceptions:  # só adiciona se não for vazio
            rows.append({
                "comment_id": c["comment_id"],
                "comment": c["comment"],
                "question_name": c["question_name"],
                "perceptions": perceptions
            })

    return rows