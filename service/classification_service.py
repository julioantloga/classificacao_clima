import pandas as pd
import re
from typing import Dict, List
from service.question_repository import insert_questions
from service.comment_repository import employee_lookup_map, insert_comments
from .openai_client import get_openai_client
from db_config import engine
from sqlalchemy import text

def define_category_themes (categorias, temas):
    
    client = get_openai_client()
    model: str = "gpt-4o"
    temperature: float = 0.0

    prompt_user = f"""
Você é um especialista em pesquisa de cilma organizacional e precisa vincluar categorias da pesquisa a um tema:

Categorias:
"{categorias}"

Temas disponíveis:
{temas}

#Dicas
- Categorias com "NPS" normalmente são relacionadas ao tema "Engajamento e Motivação"
- Categorias que citam "Satisfação Geral" no Trabalho normalmente são relacionadas ao tema "Engajamento e Motivação"

Output - traga somente a lista das categorias com o tema escolhido para ela:
<categoria>: <tema>
<categoria>: <tema>
<categoria>: <tema>
<categoria>: <tema>...
"""
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt_user}],
        )
        tema = resp.choices[0].message.content.strip()
        
        linhas = tema.strip().split("\n")

        dados = []
        for linha in linhas:
            linha = linha.strip()      

            # separa categoria e tema
            if ":" in linha:
                categoria, tema = linha.split(":", 1)
                dados.append({
                    "categoria": categoria.strip(),
                    "tema": tema.strip()
                })

        df = pd.DataFrame(dados)
        print (df)
        return df
    
    except Exception as e:
        print(f"Erro ao classificar pergunta: {categorias}\n{e}")
        return "Erro na classificação"

def save_themes_score(df_themes, survey_id):
    df_to_insert = df_themes[[
        "area_id",
        "tema",
        "nota",
        "nota diretos",
        "porcentagem insatisfeitos",
        "porcentagem insatisfeitos diretos",
    ]].rename(columns={
        "tema": "theme_name",
        "nota": "score",
        "nota diretos": "direct_score",
        "porcentagem insatisfeitos": "dissatisfied_score",
        "porcentagem insatisfeitos diretos":"direct_dissatisfied_score",
    })

    df_to_insert["survey_id"] = survey_id
    df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)
    df_to_insert = df_to_insert.dropna(subset=["score"])

    df_to_insert["dissatisfied_score"] = df_to_insert["dissatisfied_score"].fillna(0)
    df_to_insert["direct_dissatisfied_score"] = df_to_insert["direct_dissatisfied_score"].fillna(0)

    rows: List[dict] = df_to_insert.to_dict(orient="records")

    sql = text("""
        INSERT INTO theme_ranking (
            area_id, theme_name, score, direct_score, dissatisfied_score, direct_dissatisfied_score, survey_id
        ) VALUES (
            :area_id, :theme_name, :score, :direct_score, :dissatisfied_score, :direct_dissatisfied_score, :survey_id
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return True

def define_questions (df_campanha, survey_id, perguntas_abertas):
    """Objetivo: Extrair questões e categorias da pesquisa"""
    colunas = df_campanha.columns.tolist()
    i = 0
    df_qustions = pd.DataFrame(columns=["id", "name", "categoria","survey_id"])

    while i < len(colunas):
        coluna_atual = colunas[i]
        match = re.match(r'^(\d+)-ID-Categoria$', coluna_atual)
        if match:
            numero_pergunta = int(match.group(1)) #numero da pergunta
            categoria = match.group(2) #nome da categoria

            #verifica se é pergunta fechada e se a coluna da pergunta existe.
            #if (not perguntas_abertas or numero_pergunta not in perguntas_abertas) and i + 1 < len(colunas):
            
                
    return True

def data_preprocessing(df_campanha, survey_id, df_employee):
    
    #perguntas_abertas = [1,18]
    perguntas_abertas = [165,166,167,168,169,170,171,172,175,176,178,179,181,182,183,184,185,187,188,191,192,193,195,196,200,203,204,241,242,243]
    nova_planilha = []
    colunas = df_campanha.columns.tolist()
    # Valores indesejados para comentários
    INVALID_COMMENTS = {".", "..", "...", "-", "--", "---", "NULL", "null", "NaN", "nan", "na", "N/A", "n/a", "N/a","N/D", "N/d", "n/d", "nd", "ND", "Nenhuma", "Nehuma.", "Não aplicável", "Não aplicável.", "Não tenho", "Não tenho.", "Nada a declarar", "Nada a declarar.", "Sem considerações", "Sem considerações.", "Sem comentários"}
    
    i = 0
    while i < len(colunas):

        coluna_atual = colunas[i]

        match = re.match(r'^(\d+)-ID-Categoria$', coluna_atual)
        if match:
            numero_pergunta = int(match.group(1))

            if (not perguntas_abertas or numero_pergunta in perguntas_abertas) and i + 1 < len(colunas):
                coluna_categoria = coluna_atual
                coluna_pergunta = colunas[i + 1]

                coluna_comentario = None
                if (i + 2) < len(colunas):
                    proxima_coluna = colunas[i + 2]
                    if proxima_coluna.startswith(f"{numero_pergunta}-Comentario"):
                        coluna_comentario = proxima_coluna

                pergunta_texto = re.sub(r'^\d+-', '', coluna_pergunta.strip())

                for _, row in df_campanha.iterrows():
                    email = row['Email']
                    resposta = row[coluna_pergunta]
                    categoria = row[coluna_categoria]
                    categoria = re.sub(r'^\d+-', '', str(categoria).strip())

                    if coluna_comentario:
                        comentario = row.get(coluna_comentario, None)
                        comentario_valido = pd.notna(comentario) and str(comentario).strip() != ""

                        if comentario_valido:
                            resposta = comentario
                        else:
                            continue
                    else:
                        if pd.isna(resposta) or str(resposta).strip() == "":
                            continue

                    # --- Filtro para remover comentários/respostas indesejadas ---
                    resposta = str(resposta).strip()
                    if resposta in INVALID_COMMENTS:
                        continue

                    nova_planilha.append({
                        'email': email,
                        'categoria': categoria,
                        'pergunta': pergunta_texto,
                        'resposta': resposta,
                        'survey_id': survey_id
                    })

                i += 2
                if coluna_comentario:
                    i += 1
            else:
                i += 1
        else:
            i += 1

    df_resultado = pd.DataFrame(nova_planilha) 
    
    # Limpa emails internos mindsight
    #if not df_resultado.empty:
        #df_resultado = df_resultado[~df_resultado["email"].astype(str).str.contains("@mindsight.com.br", na=False)]

    # Merge com df_employee
    df_employee_filtered = df_employee[df_employee["employee_survey_id"] == survey_id].copy()
    df_employee_filtered.rename(columns={
        "employee_email": "email",
        "employee_area_id": "area_id",
        "employee_manager_id": "gestor_id"
    }, inplace=True)
      
    df_final = pd.merge(df_resultado, df_employee_filtered[["email", "area_id", "gestor_id"]], on="email", how="left")
    
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
    df_final["area_id"] = pd.to_numeric(df_final["area_id"], errors="coerce")
    df_final["gestor_id"] = pd.to_numeric(df_final["gestor_id"], errors="coerce")
    df_final["survey_id"] = pd.to_numeric(df_final["survey_id"], errors="coerce")

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
            aid = int(r["area_id"]) if pd.notna(r["area_id"]) else None

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
                "comment_survey_id": int(sid),
                "comment_area_id": aid
            })

        inserted = insert_comments(payload)
        stats["comments"] += inserted

    return stats