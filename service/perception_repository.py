
from typing import Dict, List, Tuple
from sqlalchemy import text
from db_config import engine

def fetch_employee_comments_grouped(survey_id: int) -> Dict[str, List[dict]]:
    """
    Retorna { email -> [ {comment_id, question, comment, email, area_id, gestor_id} ... ] }
    Somente comentários do survey informado.
    """
    sql = text("""
        SELECT
          lower(e.employee_email)             AS email,
          e.employee_area_id                  AS area_id,
          e.employee_manager_id               AS gestor_id,
          q.question_name                     AS question,
          c.comment_id,
          c.comment                           AS comment
        FROM employee e
        JOIN comment c
          ON c.comment_employee_id = e.employee_id
        JOIN question q
          ON q.question_id = c.comment_question_id
        WHERE e.employee_survey_id = :sid
          AND q.question_survey_id = :sid
        ORDER BY email, c.comment_id
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"sid": survey_id}).mappings().all()

    grouped: Dict[str, List[dict]] = {}
    for r in rows:
        grouped.setdefault(r["email"], []).append({
            "comment_id": r["comment_id"],
            "question":   r["question"],
            "comment":    r["comment"],
            "email":      r["email"],
            "area_id":    r["area_id"],
            "gestor_id":  r["gestor_id"],
        })
    return grouped

def insert_perceptions(rows: List[dict]) -> int:
    """
    rows: [{perception_comment_id, perception_comment_clipping, perception_theme, perception_intension}]
    """
    if not rows:
        return 0
    sql = text("""
      INSERT INTO perception
        (perception_comment_id, perception_comment_clipping, perception_theme, perception_intension)
      VALUES
        (:perception_comment_id, :perception_comment_clipping, :perception_theme, :perception_intension)
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)

def delete_perceptions_for_survey(survey_id: int) -> int:
    """
    (Opcional) Remove percepções associadas a comentários do survey.
    Útil para reprocessar.
    """
    sql = text("""
      DELETE FROM perception p
      USING comment c
      JOIN question q ON q.question_id = c.comment_question_id
      WHERE p.perception_comment_id = c.comment_id
        AND q.question_survey_id = :sid
    """)
    # Em PostgreSQL puro, reescreva sem JOIN no DELETE:
    sql = text("""
      DELETE FROM perception p
      USING comment c, question q
      WHERE p.perception_comment_id = c.comment_id
        AND q.question_id = c.comment_question_id
        AND q.question_survey_id = :sid
    """)
    with engine.begin() as conn:
        res = conn.execute(sql, {"sid": survey_id})
        return res.rowcount or 0
