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

def list_surveys():
    
    query = text("SELECT survey_id, survey_name FROM survey ORDER BY survey_id DESC")
    
    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()
    
    return [{"id": r["survey_id"], "name": r["survey_name"]} for r in rows]

def get_survey(survey_id: int):
    q = text("SELECT survey_id, survey_name FROM survey WHERE survey_id = :id")
    with engine.begin() as conn:
        row = conn.execute(q, {"id": survey_id}).mappings().first()
    if not row:
        return None
    return {"id": row["survey_id"], "name": row["survey_name"]}

def get_survey_general_data(survey_id: int):
    q = text("SELECT survey_id, survey_name FROM survey WHERE survey_id = :id")
    with engine.begin() as conn:
        row = conn.execute(q, {"id": survey_id}).mappings().first()
    if not row:
        return None
    return {"id": row["survey_id"], "name": row["survey_name"]}

def get_comments_with_perceptions(survey_id: int):
    # 1) Comentários da pesquisa (join question -> comment)
    q_comments = text("""
        SELECT 
          c.comment_id,
          c.comment,
          q.question_name
        FROM comment c
        JOIN question q ON q.question_id = c.comment_question_id
        WHERE q.question_survey_id = :sid
        ORDER BY c.comment_id DESC
        LIMIT 1000
    """)
    # 2) Percepções para os comentários retornados
    q_perceptions = text("""
        SELECT 
          p.perception_id,
          p.perception_comment_id,
          p.perception_comment_clipping,
          p.perception_theme,
          p.perception_intension
        FROM perception p
        WHERE p.perception_comment_id IN :ids
        ORDER BY p.perception_id
    """)

    with engine.begin() as conn:
        comments = conn.execute(q_comments, {"sid": survey_id}).mappings().all()

        if not comments:
            return []

        comment_ids = tuple([c["comment_id"] for c in comments])
        # SQLAlchemy precisa de tupla para IN
        percs = conn.execute(q_perceptions, {"ids": comment_ids}).mappings().all()

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
        rows.append({
            "comment_id": c["comment_id"],
            "comment": c["comment"],
            "question_name": c["question_name"],
            "perceptions": percs_by_comment.get(c["comment_id"], [])
        })
    return rows


def list_areas_with_non_null_score(survey_id: int) -> list[int]:

    sql = text("""
        SELECT DISTINCT a.area_id AS area_id, a.area_name AS area_name
        FROM comment  c
        JOIN employee e ON e.employee_id    = c.comment_employee_id
        JOIN area     a ON a.area_id        = e.employee_area_id
        WHERE e.employee_survey_id = :sid
          AND a.area_score IS NOT NULL
        ORDER BY a.area_name
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"sid": survey_id}).mappings().all()

    return [{"area_id": int(r["area_id"]), "area_name": (r["area_name"])} for r in rows]



def list_perception_themes_for_survey(survey_id: int) -> list[str]:

    sql = text("""
        SELECT DISTINCT p.perception_theme AS theme
        FROM perception p
        JOIN comment    c ON c.comment_id        = p.perception_comment_id
        JOIN employee   e ON e.employee_id       = c.comment_employee_id
        WHERE e.employee_survey_id = :sid
          AND p.perception_theme IS NOT NULL
          AND TRIM(p.perception_theme) <> ''
        ORDER BY 1
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"sid": survey_id}).mappings().all()
    return [r["theme"] for r in rows if r["theme"] is not None]

def get_area_review_plan(area_id: int, survey_id: int):

    sql = text("""
        SELECT 
            area_survey_id,
            area_review,
            area_plan
        FROM area
        WHERE area_id = :aid AND area_survey_id = :sid
        LIMIT 1
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {"aid": area_id, "sid": survey_id}).mappings().first()

    return row  
