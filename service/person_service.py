import pandas as pd

def person_preprocessing(
    df_person: pd.DataFrame,
    df_person_areas: pd.DataFrame,
    survey_id: int
) -> pd.DataFrame:
    """
    Monta df_employee a partir de df_person e df_person_areas.

    Regras:
      - Apenas pessoas com área ATIVA entram (end_date nulo em df_person_areas).
      - Se houver múltiplas áreas ativas, escolhe a com maior start_date.

    Saída:
      employee_id, employee_email, employee_name,
      employee_manager_id, employee_area_id, employee_survey_id
    """

    # ----------- df_person: seleciona e normaliza -----------
    required_person = {"id", "email", "nome", "sobrenome", "id_gestor"}
    missing_p = required_person - set(df_person.columns)
    if missing_p:
        raise ValueError(f"df_person sem colunas obrigatórias: {', '.join(sorted(missing_p))}")

    p = df_person.copy()

    p["employee_id"] = pd.to_numeric(p["id"], errors="coerce").astype("Int64")
    p["employee_manager_id"] = pd.to_numeric(p["id_gestor"], errors="coerce").astype("Int64")
    p["employee_manager_id"] = p["employee_manager_id"].fillna(0).astype("Int64")

    p["employee_email"] = (
        p["email"].astype(str).str.strip().str.lower().replace({"": pd.NA})
    )

    first = p["nome"].fillna("").astype(str).str.strip()
    last  = p["sobrenome"].fillna("").astype(str).str.strip()
    full  = (first + " " + last).str.replace(r"\s+", " ", regex=True).str.strip()
    p["employee_name"] = full.where(full.ne(""), pd.NA)

    p = p[["employee_id", "employee_email", "employee_name", "employee_manager_id"]]

    # ----------- df_person_areas: apenas áreas ATIVAS -----------
    required_areas = {"person", "area", "start_date", "end_date"}
    missing_a = required_areas - set(df_person_areas.columns)
    if missing_a:
        raise ValueError(f"df_person_areas sem colunas obrigatórias: {', '.join(sorted(missing_a))}")

    a = df_person_areas.copy()
    a["employee_id"] = pd.to_numeric(a["person"], errors="coerce").astype("Int64")
    a["employee_area_id"] = pd.to_numeric(a["area"], errors="coerce").astype("Int64")
    a["start_date"] = pd.to_datetime(a["start_date"], errors="coerce", utc=False)
    a["end_date"]   = pd.to_datetime(a["end_date"],   errors="coerce", utc=False)

    # >>> Somente ATIVOS (end_date nulo)
    active = a[a["end_date"].isna()].copy()

    # Se não houver nenhum ativo, retorna DF vazio com as colunas esperadas
    if active.empty:
        cols = ["employee_id", "employee_email", "employee_name",
                "employee_manager_id", "employee_area_id", "employee_survey_id"]
        return pd.DataFrame(columns=cols)

    # Entre os ativos, pegar o de maior start_date por pessoa
    active_sorted = active.sort_values(["employee_id", "start_date"], ascending=[True, True])
    best_active_area = (
        active_sorted.groupby("employee_id", as_index=False)
        .tail(1)[["employee_id", "employee_area_id"]]
    )

    # ----------- merge final (INNER JOIN para excluir quem não tem área ativa) -----------
    df_employee = p.merge(best_active_area, on="employee_id", how="inner")

    # survey_id para todas as linhas
    df_employee["employee_survey_id"] = survey_id
    df_employee["employee_survey_id"] = pd.to_numeric(df_employee["employee_survey_id"], errors="coerce").astype("Int64")


    # Ordena colunas conforme solicitado
    df_employee = df_employee[
        ["employee_id", "employee_email", "employee_name",
         "employee_manager_id", "employee_area_id", "employee_survey_id"]
    ]

    # Dedup por employee_id (mantém a primeira ocorrência)
    df_employee = df_employee.drop_duplicates(subset=["employee_id"], keep="first").reset_index(drop=True)

    return df_employee
