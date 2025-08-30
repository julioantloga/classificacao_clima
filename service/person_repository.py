# service/person_repository.py
import pandas as pd
from db_config import engine

REQUIRED_COLS = {
    "employee_id",
    "employee_email",
    "employee_name",
    "employee_manager_id",
    "employee_area_id",
    "employee_survey_id",
}

def insert_person(df_employee: pd.DataFrame) -> int:
    """
    Insere todas as linhas de df_employee na tabela 'employee'.
    As colunas do DF devem ser exatamente:
      employee_id, employee_email, employee_name,
      employee_manager_id, employee_area_id, employee_survey_id

    Retorna o número de linhas inseridas.
    """
    if df_employee is None or df_employee.empty:
        return 0

    missing = REQUIRED_COLS - set(df_employee.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes em df_employee: {', '.join(sorted(missing))}")

    # Ajustes de tipos comuns (opcionais)
    df = df_employee.copy()
    for col in ("employee_id", "employee_manager_id", "employee_area_id", "employee_survey_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Inserção em lote (append) dentro de transação
    with engine.begin() as conn:
        df.to_sql(
            "employee",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    return len(df)
