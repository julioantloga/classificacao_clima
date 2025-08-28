# service/areas_repository.py
import pandas as pd
from sqlalchemy import text
from db_config import engine

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

    # Ajustes de tipo comuns (opcional)
    # - area_parent pode ser numérico ou string no seu CSV; se quiser forçar Int64:
    # df_areas["area_parent"] = pd.to_numeric(df_areas["area_parent"], errors="coerce").astype("Int64")

    # Inserção em lote (append) – usa transação
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
