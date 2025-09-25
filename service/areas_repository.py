from typing import List
import pandas as pd
from sqlalchemy import text
from db_config import engine
import json

REQUIRED_COLS = {"area_id", "area_name", "area_parent", "area_survey_id"}

def insert_areas (df_areas: pd.DataFrame) -> int:
    """
    Insere todas as linhas de df_areas na tabela 'area'.
    As colunas do DF devem ser: area_id, area_name, area_parent, area_survey_id.
    Retorna o número de linhas inseridas.
    """
    if df_areas is None or df_areas.empty:
        return 0

    missing = REQUIRED_COLS - set(df_areas.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes no DF: {', '.join(sorted(missing))}")

    with engine.begin() as conn:
        df_areas.to_sql(
            "area",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

    return len(df_areas)

# ============================================================
# Consultas e updates
# ============================================================

def fetch_survey_areas(survey_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT area_id, area_parent, area_name, area_survey_id, area_level
                FROM area
                WHERE area_survey_id = :sid
            """),
            conn, params={"sid": survey_id}
        )

def fetch_survey_employees(survey_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT employee_id, employee_area_id, employee_survey_id
                FROM employee
                WHERE employee_survey_id = :sid
            """),
            conn, params={"sid": survey_id}
        )

def fetch_survey_comments(survey_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT c.comment_id, c.comment_employee_id, c.comment_area_id, c.comment_survey_id
                FROM comment c
                JOIN question q ON q.question_id = c.comment_question_id
                WHERE q.question_survey_id = :sid
            """),
            conn, params={"sid": survey_id}
        )

def fetch_survey_perceptions(survey_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT
                    p.perception_id,
                    p.perception_comment_id,
                    p.perception_intension,
                    p.perception_theme,
                    p.perception_comment_clipping,
                    p.perception_area_id
                FROM perception p
                JOIN comment c ON c.comment_id = p.perception_comment_id
                JOIN question q ON q.question_id = c.comment_question_id
                WHERE q.question_survey_id = :sid
            """),
            conn, params={"sid": survey_id}
        )

def update_area_metrics_bulk(survey_id: int, df_metrics: pd.DataFrame, chunk_size: int = 1000) -> int:
    
    print ('entrou no update de métricas \n')

    """
    Atualiza métricas nas linhas de 'area' presentes em df_metrics.
    Colunas aceitas no DF (todas opcionais, exceto area_id):
      - area_id
      - area_employee_number
      - area_comments_number
      - area_criticism_number
      - area_suggestions_number
      - area_recognition_number
      - area_response_rate
      - area_score
      - area_intents
      - area_level

    Retorna o total de linhas atualizadas.
    """

    df = df_metrics.copy()
    df["area_id"] = pd.to_numeric(df["area_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["area_id"]).copy()

    # Lista base de colunas suportadas
    base_cols = [
        "area_employee_number",
        "area_comments_number",
        "area_criticism_number",
        "area_suggestions_number",
        "area_recognition_number",
        "area_response_rate",
        "area_score",
        "area_intents",
    ]

    # Converção de tipos numéricos (quando existirem)
    num_int_cols = [
        "area_employee_number",
        "area_comments_number",
        "area_criticism_number",
        "area_suggestions_number",
        "area_recognition_number",
    ]
    for c in num_int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    if "area_response_rate" in df.columns:
        df["area_response_rate"] = pd.to_numeric(df["area_response_rate"], errors="coerce").fillna(0.0).astype(float)
    if "area_score" in df.columns:
        df["area_score"] = pd.to_numeric(df["area_score"], errors="coerce").fillna(0.0).astype(float)

    # area_intents: aceita dict -> json str; str vazia -> NULL
    if "area_intents" in df.columns:
        def _to_jsonb(v):
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                return json.dumps(v, ensure_ascii=False)
            s = str(v).strip()
            return s if s not in ("", "None", "null") else None
        df["area_intents"] = df["area_intents"].apply(_to_jsonb)

    # Monta SQL dinamicamente (incluindo area_level se existir)
    set_clauses = []
    if "area_employee_number" in df.columns:    set_clauses.append("area_employee_number = :area_employee_number")
    if "area_comments_number" in df.columns:    set_clauses.append("area_comments_number = :area_comments_number")
    if "area_criticism_number" in df.columns:   set_clauses.append("area_criticism_number = :area_criticism_number")
    if "area_suggestions_number" in df.columns: set_clauses.append("area_suggestions_number = :area_suggestions_number")
    if "area_recognition_number" in df.columns: set_clauses.append("area_recognition_number = :area_recognition_number")
    if "area_response_rate" in df.columns:      set_clauses.append("area_response_rate = :area_response_rate")
    if "area_score" in df.columns:              set_clauses.append("area_score = :area_score")
    if "area_intents" in df.columns:            set_clauses.append("area_intents = CAST(:area_intents AS jsonb)")

    if not set_clauses:
        # nada para atualizar além do where → evita SQL inválido
        return 0

    sql = text(f"""
        UPDATE area
           SET {", ".join(set_clauses)}
         WHERE area_survey_id = :sid
           AND area_id = :area_id
    """)

    # Prepara payload apenas com colunas que o SQL usa + sid + area_id
    used_cols = {"area_id"} | {c.split(" = ")[0] for c in set_clauses}
    records = []
    for _, r in df.iterrows():
        rec = {k: r.get(k) for k in used_cols if k in df.columns or k == "area_id"}
        rec["sid"] = survey_id
        records.append(rec)

    total = 0
    with engine.begin() as conn:
        # chunk para lotes grandes
        for i in range(0, len(records), chunk_size):
            batch = records[i:i+chunk_size]
            res = conn.execute(sql, batch)
            # dependendo do driver, rowcount em executemany retorna -1; soma defensiva:
            if hasattr(res, "rowcount") and res.rowcount and res.rowcount > 0:
                total += res.rowcount
            else:
                # assume que atualizou todos do batch
                total += len(batch)
    return total

def fetch_survey_areas_with_intents(survey_id: int) -> pd.DataFrame:
    """
    Retorna as áreas do survey com seus JSONs de intenções (se houver).
    Colunas: area_id, area_parent, area_name, area_survey_id, area_intents, area_review
    """
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT area_id, area_parent, area_name, area_survey_id, area_intents, area_review
                FROM area
                WHERE area_survey_id = :sid
                  AND area_intents IS NOT NULL
            """),
            conn, params={"sid": survey_id}
        )

def update_area_reviews_bulk(survey_id: int, df_reviews: pd.DataFrame) -> int:
    """
    Atualiza o campo area_review das áreas presentes em df_reviews.
    Espera colunas: [area_id, area_review]
    """
    if df_reviews is None or df_reviews.empty:
        return 0

    sql = text("""
        UPDATE area
        SET area_review = :area_review
        WHERE area_survey_id = :sid
          AND area_id = :area_id
    """)

    payload = df_reviews[["area_id", "area_review"]].to_dict(orient="records")
    for row in payload:
        row["sid"] = survey_id

    with engine.begin() as conn:
        conn.execute(sql, payload)

    return len(payload)

def ensure_general_area(survey_id: int) -> None:
    """
    Garante a existência da área 'Geral' (area_id=0, area_parent=0).
    """
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM area WHERE area_survey_id = :sid AND area_id = 0"),
            {"sid": survey_id}
        ).first()
        if not exists:
            conn.execute(
                text("""
                    INSERT INTO area (area_id, area_name, area_parent, area_survey_id)
                    VALUES (0, 'Geral', 0, :sid)
                """),
                {"sid": survey_id}
            )

def update_area_0_all_fields(
    survey_id: int,
    area_employee_number: int,
    area_comments_number: int,
    area_criticism_number: int,
    area_suggestions_number: int,
    area_recognition_number: int,
    area_response_rate: float,
    area_score: float,
    area_intents: str,  # JSON (str)
    area_review: str,
) -> int:
    """
    Atualiza todas as colunas de métricas + intents + review para a área_id=0 do survey.
    Retorna 1 se atualizou, 0 caso contrário.
    """
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                UPDATE area
                SET
                  area_employee_number    = :area_employee_number,
                  area_comments_number    = :area_comments_number,
                  area_criticism_number   = :area_criticism_number,
                  area_suggestions_number = :area_suggestions_number,
                  area_recognition_number = :area_recognition_number,
                  area_response_rate      = :area_response_rate,
                  area_score              = :area_score,
                  area_intents            = CAST(:area_intents AS jsonb),
                  area_review             = :area_review
                WHERE area_survey_id = :sid
                  AND area_id = 0
            """),
            {
                "sid": survey_id,
                "area_employee_number": area_employee_number,
                "area_comments_number": area_comments_number,
                "area_criticism_number": area_criticism_number,
                "area_suggestions_number": area_suggestions_number,
                "area_recognition_number": area_recognition_number,
                "area_response_rate": area_response_rate,
                "area_score": area_score,
                "area_intents": area_intents,
                "area_review": area_review
            }
        )
        return res.rowcount or 0

def fetch_area_intents(survey_id: int, area_id: int = 0) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT area_intents
                  FROM area
                 WHERE area_survey_id = :sid
                   AND area_id = :aid
            """),
            {"sid": survey_id, "aid": area_id}
        ).first()
    return row[0] if row else None

def update_area_review(survey_id: int, area_id: int, review_text: str) -> int:

    with engine.begin() as conn:
        res = conn.execute(
            text("""
                UPDATE area
                   SET area_review = :review
                 WHERE area_survey_id = :sid AND area_id = :aid
            """),
            {"sid": survey_id, "aid": area_id, "review": review_text}
        )
    return res.rowcount

def fetch_area_metrics_for_ids(survey_id: int, ids: List[int]) -> pd.DataFrame:
    """
    Busca métricas das áreas (por IDs) dentro de um survey.
    Retorna colunas:
      area_id, area_employee_number, area_comments_number,
      area_criticism_number, area_suggestions_number, area_recognition_number,
      area_response_rate, area_score
    """
    if not ids:
        return pd.DataFrame(columns=[
            "area_id","area_employee_number","area_comments_number",
            "area_criticism_number","area_suggestions_number","area_recognition_number",
            "area_response_rate","area_score"
        ])
    with engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                  area_id,
                  COALESCE(area_employee_number,0)    AS area_employee_number,
                  COALESCE(area_comments_number,0)    AS area_comments_number,
                  COALESCE(area_criticism_number,0)   AS area_criticism_number,
                  COALESCE(area_suggestions_number,0) AS area_suggestions_number,
                  COALESCE(area_recognition_number,0) AS area_recognition_number,
                  COALESCE(area_response_rate,0.0)    AS area_response_rate,
                  COALESCE(area_score,0.0)            AS area_score
                FROM area
               WHERE area_survey_id = :sid
                 AND area_id = ANY(:ids)
            """),
            conn,
            params={"sid": survey_id, "ids": ids},
        )
    return df

def update_area_0_metrics(
    survey_id: int,
    area_employee_number: int,
    area_comments_number: int,
    area_criticism_number: int,
    area_suggestions_number: int,
    area_recognition_number: int,
    area_response_rate: float,
    area_score: float,
) -> int:
    """
    Atualiza SOMENTE as métricas da área_id=0 (Geral),
    sem alterar area_intents e area_review.
    """
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                UPDATE area
                   SET area_employee_number    = :emp,
                       area_comments_number    = :com,
                       area_criticism_number   = :cri,
                       area_suggestions_number = :sug,
                       area_recognition_number = :rec,
                       area_response_rate      = :rr,
                       area_score              = :score
                 WHERE area_survey_id = :sid
                   AND area_id = 0
            """),
            {
                "sid": survey_id,
                "emp": int(area_employee_number),
                "com": int(area_comments_number),
                "cri": int(area_criticism_number),
                "sug": int(area_suggestions_number),
                "rec": int(area_recognition_number),
                "rr": float(area_response_rate),
                "score": float(area_score),
            }
        )
    return res.rowcount or 0

def save_area_plan(area_id: int, plan_text: str):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE area SET area_plan = :plan_text WHERE area_id = :aid
        """), {"plan_text": plan_text, "aid": area_id})

####################
########################

def get_area_perceptions(survey_id):
    """ 
    """
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT *
                FROM perception
                WHERE perception_survey_id = :sid 
            """),
            conn, params={"sid": survey_id}
        )

def get_themes_score (survey_id):
    """
    """
    with engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT *
                FROM theme_ranking
                WHERE survey_id = :sid
            """),
            conn, params={"sid": survey_id}
        ) 
       
def update_theme_ranking_scores(survey_id, scores_by_area):
    with engine.begin() as conn:
        for area_id, area_scores in scores_by_area.items():
            direct_scores = area_scores.get("direto", {})
            total_scores = area_scores.get("total", {})

            for theme_name, direct_score in direct_scores.items():
                total_score = total_scores.get(theme_name, 0)

                result = conn.execute(
                    text("""
                        UPDATE theme_ranking
                        SET 
                            direct_comment_score = :dcs,
                            comment_score = :cs
                        WHERE 
                            area_id = :aid 
                            AND survey_id = :sid 
                            AND theme_name = :t
                    """),
                    {
                        "dcs": direct_score,
                        "cs": total_score,
                        "aid": area_id,
                        "sid": survey_id,
                        "t": theme_name
                    }
                )

                # Se nenhum registro foi atualizado, ignora e segue
                if result.rowcount == 0:
                    continue

    return True

