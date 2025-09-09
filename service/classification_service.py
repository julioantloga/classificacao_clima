import pandas as pd
import re
from typing import Dict
from service.question_repository import insert_questions
from service.comment_repository import employee_lookup_map, insert_comments

def data_preprocessing(df, df_person, survey_id):
    # Informar o id das perguntas abertas e perguntas que contém comentário
    perguntas_abertas = [165,166,167,168,169,170,171,172,175,176,178,179,181,182,183,184,185,187,188,191,192,193,195,196,200,203,204,241,242,243]
    nova_planilha = []
    colunas = df.columns.tolist()
    i = 0

    while i < len(colunas):
        coluna_atual = colunas[i]

        match = re.match(r'^(\d+)-ID-Categoria$', coluna_atual)
        if match:
            numero_pergunta = int(match.group(1))

            if (not perguntas_abertas or numero_pergunta in perguntas_abertas) and i + 1 < len(colunas):

                # Definição dos nomes das colunas
                coluna_categoria = coluna_atual
                coluna_pergunta = colunas[i + 1]

                # Verificar se existe a coluna de comentário
                coluna_comentario = None
                if (i + 2) < len(colunas):
                    proxima_coluna = colunas[i + 2]
                    if proxima_coluna.startswith(f"{numero_pergunta}-Comentario"):
                        coluna_comentario = proxima_coluna

                pergunta_texto = re.sub(r'^\d+-', '', coluna_pergunta.strip())

                for _, row in df.iterrows():
                    email = row['Email']
                    resposta = row[coluna_pergunta]
                    categoria = row[coluna_categoria]

                    # Remove o número e hífen no início da categoria
                    categoria = re.sub(r'^\d+-', '', str(categoria).strip())

                    # Verifica se existe comentário
                    if coluna_comentario:
                        comentario = row.get(coluna_comentario, None)
                        comentario_valido = pd.notna(comentario) and str(comentario).strip() != ""

                        if comentario_valido:
                            resposta = comentario  # Usa o comentário
                        else:
                            continue  # comentário vazio → ignora
                    else:
                        if pd.isna(resposta) or str(resposta).strip() == "":
                            continue

                    nova_planilha.append({
                        'email': email,
                        'categoria': categoria,
                        'pergunta': pergunta_texto,
                        'resposta': resposta,
                        'survey_id': survey_id
                    })

                # Pular para a próxima pergunta
                i += 2
                if coluna_comentario:
                    i += 1
            else:
                i += 1
        else:
            i += 1

    # ---------- DataFrame base ----------
    df_resultado = pd.DataFrame(nova_planilha)

    # Limpa emails internos mindsight
    if not df_resultado.empty:
        df_resultado = df_resultado[~df_resultado["email"].astype(str).str.contains("@mindsight.com.br", na=False)]

    # ---------- Padronização de colunas no df_person ----------
    area_candidates   = ['area_id', 'id_area', 'área', 'area']
    gestor_candidates = ['gestor_id', 'id_gestor', 'gestor', 'manager_id']

    def pick_col(candidates, df_):
        for c in candidates:
            if c in df_.columns:
                return c
        return None

    area_col_in_person   = pick_col(area_candidates, df_person)
    gestor_col_in_person = pick_col(gestor_candidates, df_person)

    if 'email' not in df_person.columns:
        raise ValueError("df_person precisa conter a coluna 'email'.")

    merge_cols = ['email']
    if area_col_in_person:   merge_cols.append(area_col_in_person)
    if gestor_col_in_person: merge_cols.append(gestor_col_in_person)

    df_merge = df_person[merge_cols].copy()

    # Converte para Int64
    if area_col_in_person:
        df_merge['area_id'] = pd.to_numeric(df_merge[area_col_in_person], errors='coerce').astype('Int64')
        df_merge.drop(columns=[area_col_in_person], inplace=True, errors='ignore')
    else:
        df_merge['area_id'] = pd.Series(dtype='Int64')

    if gestor_col_in_person:
        df_merge['gestor_id'] = pd.to_numeric(df_merge[gestor_col_in_person], errors='coerce').astype('Int64')
        df_merge.drop(columns=[gestor_col_in_person], inplace=True, errors='ignore')
    else:
        df_merge['gestor_id'] = pd.Series(dtype='Int64')

    # Merge final
    df_final = pd.merge(df_resultado, df_merge, on='email', how='left')

    # Ordena colunas para consistência
    col_order = ['email', 'categoria', 'pergunta', 'resposta', 'survey_id', 'area_id', 'gestor_id']
    for c in col_order:
        if c not in df_final.columns:
            df_final[c] = pd.NA
    df_final = df_final[col_order]

    return df_final


def persist_questions_and_comments(df_final: pd.DataFrame) -> Dict[str, int]:
    """
    Espera colunas: ['email','categoria','pergunta','resposta','survey_id','area_id','gestor_id'].
    Fluxo:
      1) Garante perguntas (question) por survey_id.
      2) Resolve employee por email e cruza area_id/gestor_id da df_final.
      3) Insere comentários (comment).
    Retorna contadores: questions, comments, skipped_no_employee, skipped_no_question,
                       area_mismatch, gestor_mismatch.
    """
    required = {"email","pergunta","resposta","survey_id","area_id","gestor_id"}
    missing = required - set(df_final.columns)
    if missing:
        raise ValueError(f"df_final sem colunas obrigatórias: {', '.join(sorted(missing))}")

    stats = {
        "questions": 0,
        "comments": 0,
        "skipped_no_employee": 0,
        "skipped_no_question": 0,
        "area_mismatch": 0,
        "gestor_mismatch": 0,
    }

    # processa por survey_id (caso haja múltiplos no DF)
    for survey_id, df_s in df_final.groupby("survey_id"):
        sid = int(survey_id)

        # 1) perguntas (garante/insere e devolve {name -> id})
        question_names = (
            df_s["pergunta"]
            .dropna().astype(str).str.strip()
            .unique().tolist()
        )
        qmap = insert_questions(sid, question_names)
        stats["questions"] += len(qmap)

        # 2) mapa completo de employees por email (inclui area/gestor atuais no banco)
        emap = employee_lookup_map(sid)

        # 3) montar payload de comments
        payload = []
        for _, r in df_s.iterrows():
            email = (str(r["email"]).strip().lower() if pd.notna(r["email"]) else "")
            qname = (str(r["pergunta"]).strip() if pd.notna(r["pergunta"]) else "")
            text_comment = (str(r["resposta"]).strip() if pd.notna(r["resposta"]) else "")

            if not text_comment:
                continue

            emp = emap.get(email)
            if not emp:
                stats["skipped_no_employee"] += 1
                continue

            qid = qmap.get(qname)
            if not qid:
                stats["skipped_no_question"] += 1
                continue

            # ---- cruzamento area/gestor com o cadastro atual ----
            df_area   = None if pd.isna(r["area_id"])   else int(r["area_id"])
            df_gestor = None if pd.isna(r["gestor_id"]) else int(r["gestor_id"])

            # compara e contabiliza divergências (apenas estatística; não bloqueia)
            if df_area is not None and emp.get("employee_area_id") is not None:
                if int(emp["employee_area_id"]) != df_area:
                    stats["area_mismatch"] += 1
            if df_gestor is not None and emp.get("employee_manager_id") is not None:
                if int(emp["employee_manager_id"]) != df_gestor:
                    stats["gestor_mismatch"] += 1

            payload.append({
                "comment": text_comment,
                "comment_employee_id": int(emp["employee_id"]),
                "comment_question_id": int(qid),
            })

        inserted = insert_comments(payload)
        stats["comments"] += inserted

    return stats