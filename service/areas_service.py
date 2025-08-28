import pandas as pd

def create_organizational_chart(
    df_instancia_areas: pd.DataFrame,
    df_hierarquia_areas: pd.DataFrame,
    area_survey_id: int | None = None,
) -> pd.DataFrame:
    """
    Retorna DataFrame com: area_id, area_name, area_parent, area_survey_id.

    Join:
      LEFT JOIN df_instancia_areas (id -> name)
                com df_hierarquia_areas (area -> parent)
      Chave: instancia.id == hierarquia.area (como string, sem espaços).

    Se a área não existir em df_hierarquia_areas, area_parent será nulo.
    'area_survey_id' é replicado para todas as linhas do resultado.
    """
    # Validação mínima
    req_inst = {"id", "name"}
    req_hier = {"area", "parent"}
    if not req_inst.issubset(df_instancia_areas.columns):
        faltam = ", ".join(sorted(req_inst - set(df_instancia_areas.columns)))
        raise ValueError(f"df_instancia_areas sem colunas obrigatórias: {faltam}")
    if not req_hier.issubset(df_hierarquia_areas.columns):
        faltam = ", ".join(sorted(req_hier - set(df_hierarquia_areas.columns)))
        raise ValueError(f"df_hierarquia_areas sem colunas obrigatórias: {faltam}")

    # Prepara chaves (string + trim)
    left = df_instancia_areas.loc[:, ["id", "name"]].copy()
    left["_key"] = left["id"].astype(str).str.strip()

    right = df_hierarquia_areas.loc[:, ["area", "parent"]].copy()
    right["_key"] = right["area"].astype(str).str.strip()
    right = right.drop_duplicates(subset="_key", keep="first")

    merged = left.merge(
        right.loc[:, ["_key", "parent"]],
        on="_key",
        how="left",
        validate="one_to_one",
    )

    out = pd.DataFrame({
        "area_id":     merged["id"],
        "area_name":   merged["name"],
        "area_parent": merged["parent"],
    })

    # Normaliza nulos de parent
    if out["area_parent"].dtype == object:
        out["area_parent"] = out["area_parent"].apply(
            lambda x: pd.NA if (x is None or str(x).strip() == "") else x
        )

    # Adiciona a nova coluna area_survey_id (nulo-seguro)
    out["area_survey_id"] = pd.Series(area_survey_id, index=out.index)
    # Se quiser garantir inteiro com NA, converte:
    try:
        out["area_survey_id"] = pd.to_numeric(out["area_survey_id"], errors="coerce").astype("Int64")
    except Exception:
        # Se não for numérico, mantém como object
        pass

    return out
