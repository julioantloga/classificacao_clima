# service/general_review.py
from __future__ import annotations
import json
from typing import Dict, List, Tuple
import pandas as pd
from sqlalchemy import text
from typing import Union
import re

from db_config import engine

from .areas_repository import (
    fetch_survey_areas,
    fetch_survey_areas_with_intents,
    fetch_area_intents,
    update_area_review
)

from .openai_client import get_openai_client
from .areas_service import _compute_area_levels  # já existe

# ----------------------------------------------------------
# Util: garantir a existência da área "Geral" (area_id = 0)
# ----------------------------------------------------------
def ensure_general_area_exists(survey_id: int) -> None:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT 1
                FROM area
                WHERE area_survey_id = :sid AND area_id = 0
            """),
            {"sid": survey_id}
        ).first()
        if row:
            return
        conn.execute(
            text("""
                INSERT INTO area (area_id, area_name, area_parent, area_survey_id)
                VALUES (0, 'Geral', 0, :sid)
            """),
            {"sid": survey_id}
        )

# ----------------------------------------------------------
# Util: buscar métricas reais na tabela area (comentários, score etc.)
# ----------------------------------------------------------
def _fetch_area_metrics_for_ids(survey_id: int, ids: List[int]) -> pd.DataFrame:
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
                  area_employee_number, area_comments_number,
                  area_criticism_number, area_suggestions_number, area_recognition_number,
                  area_response_rate, area_score
                FROM area
                WHERE area_survey_id = :sid
                  AND area_id = ANY(:ids)
            """),
            conn,
            params={"sid": survey_id, "ids": ids},
        )
    # garante colunas se vierem nulas
    for c in ["area_employee_number","area_comments_number","area_criticism_number",
              "area_suggestions_number","area_recognition_number","area_response_rate","area_score"]:
        if c not in df.columns:
            df[c] = 0
    return df

# ----------------------------------------------------------
# Monta o JSON da área geral + métricas para update
# Regra: se houver 1 única área no nível topo => copia métricas dessa área.
#        caso contrário => soma/compila das áreas de topo.
# Além disso, inclui 'resumos_filhas' (apenas áreas de topo).
# ----------------------------------------------------------
def build_general_json_and_metrics(
    survey_id: int,
    min_level: int = 0
) -> Tuple[Dict, Dict]:
    # Áreas + níveis
    df_areas = fetch_survey_areas(survey_id)  # [area_id, area_parent, area_name, area_survey_id]
    if df_areas.empty:
        return {}, {}

    levels = _compute_area_levels(df_areas)
    # ignora a "Geral" (id 0) na detecção de topo
    df_top = df_areas[df_areas["area_id"] != 0].copy()
    df_top["level"] = df_top["area_id"].map(levels).fillna(0).astype(int)
    top_level = int(min_level)
    top_ids = df_top.loc[df_top["level"] == top_level, "area_id"].astype(int).tolist()

    # pega intents e reviews das áreas (inclui col area_review)
    df_ai = fetch_survey_areas_with_intents(survey_id)
    df_ai = df_ai[df_ai["area_id"] != 0]  # evita confundir com a Geral
    df_ai = df_ai.merge(df_areas[["area_id","area_name"]], on="area_id", how="left")

    # filtra só top_ids
    df_top_ai = df_ai[df_ai["area_id"].isin(top_ids)].copy()

    # parseia cada area_intents (pode ser None/str/dict)
    def parse_json(x):
        if x is None:
            return {}
        if isinstance(x, dict):
            return x
        if isinstance(x, str):
            x = x.strip()
            if x == "" or x == "{}":
                return {}
            try:
                return json.loads(x)
            except Exception:
                # se não for JSON válido, ignora
                return {}
        return {}

    df_top_ai["intents_obj"] = df_top_ai["area_intents"].apply(parse_json)

    # áreas com payload válido
    valid_rows = df_top_ai[df_top_ai["intents_obj"].apply(lambda o: isinstance(o, dict) and len(o) > 0)].copy()
    # resumos das filhas (apenas topo)
    resumos_filhas = {}
    for _, r in df_top_ai.iterrows():
        name = r.get("area_name") or f"Área {r['area_id']}"
        review = (r.get("area_review") or "").strip() 
        if review:
            resumos_filhas[name] = review

    # Se nenhuma top tem intents válidas: retorna JSON mínimo + métricas zeradas
    if valid_rows.empty:
        general_json = {
            "metricas": {
                "funcionarios": 0, "respondentes": 0, "adesao": 0.0,
                "criticas": 0, "sugestoes": 0, "reconhecimentos": 0, "neutros": 0,
                "peso_area": 0.0, "score_area": 0.0
            },
            "temas_citados": {},
            "recortes": {},
            "resumos_filhas": resumos_filhas
        }
        metrics_row = {
            "area_employee_number": 0, "area_comments_number": 0,
            "area_criticism_number": 0, "area_suggestions_number": 0, "area_recognition_number": 0,
            "area_response_rate": 0.0, "area_score": 0.0
        }
        return general_json, metrics_row

    # 1) Se só existe 1 área de topo com intents válidas → copia métricas e json dela
    #    (mas ainda adiciona resumos_filhas das demais, se houver)
    if len(valid_rows) == 1:
        row = valid_rows.iloc[0]
        obj = row["intents_obj"]
        general_json = dict(obj)  # cópia
        general_json["resumos_filhas"] = resumos_filhas

        # Métricas: copiar da área (tabela area)
        top_metrics = _fetch_area_metrics_for_ids(survey_id, [int(row["area_id"])])
        if top_metrics.empty:
            metrics_row = {
                "area_employee_number": int(obj.get("metricas", {}).get("funcionarios", 0)),
                "area_comments_number": 0,
                "area_criticism_number": int(obj.get("metricas", {}).get("criticas", 0)),
                "area_suggestions_number": int(obj.get("metricas", {}).get("sugestoes", 0)),
                "area_recognition_number": int(obj.get("metricas", {}).get("reconhecimentos", 0)),
                "area_response_rate": float(obj.get("metricas", {}).get("adesao", 0.0)) / 100.0,
                "area_score": float(obj.get("metricas", {}).get("score_area", 0.0)),
            }
        else:
            r0 = top_metrics.iloc[0].fillna(0)
            metrics_row = {
                "area_employee_number": int(r0["area_employee_number"]),
                "area_comments_number": int(r0["area_comments_number"]),
                "area_criticism_number": int(r0["area_criticism_number"]),
                "area_suggestions_number": int(r0["area_suggestions_number"]),
                "area_recognition_number": int(r0["area_recognition_number"]),
                "area_response_rate": float(r0["area_response_rate"]),
                "area_score": float(r0["area_score"]),
            }
        return general_json, metrics_row

    # 2) Caso haja várias áreas de topo:
    #    somamos métricas e unimos temas/recortes
    #    (comments_number virá da tabela area)
    funcionarios = respondentes = criticas = sugestoes = reconhecimentos = neutros = 0
    temas_citados: Dict[str, Dict[str, int]] = {}
    recortes: Dict[str, Dict[str, List[str]]] = {}

    # acumular a partir do JSON de cada área válida
    for _, r in valid_rows.iterrows():
        obj = r["intents_obj"]
        met = obj.get("metricas", {})
        funcionarios    += int(met.get("funcionarios", 0))
        respondentes    += int(met.get("respondentes", 0))
        criticas        += int(met.get("criticas", 0))
        sugestoes       += int(met.get("sugestoes", 0))
        reconhecimentos += int(met.get("reconhecimentos", 0))
        neutros         += int(met.get("neutros", 0))

        # temas
        for tema, cc in (obj.get("temas_citados") or {}).items():
            dst = temas_citados.setdefault(tema, {"criticas":0,"sugestoes":0,"reconhecimentos":0,"neutros":0,"total":0})
            dst["criticas"]        += int(cc.get("criticas", 0))
            dst["sugestoes"]       += int(cc.get("sugestoes", 0))
            dst["reconhecimentos"] += int(cc.get("reconhecimentos", 0))
            dst["neutros"]         += int(cc.get("neutros", 0))
            dst["total"]           += int(cc.get("total", 0))

        # recortes
        for tema, intents_map in (obj.get("recortes") or {}).items():
            bucket = recortes.setdefault(tema, {"critica":[], "sugestao":[], "reconhecimento":[], "neutro":[]})
            for intent in ["critica","sugestao","reconhecimento","neutro"]:
                bucket[intent].extend(intents_map.get(intent, []))

    # taxa de adesão consolidada
    adesao = round((respondentes / funcionarios * 100.0), 2) if funcionarios > 0 else 0.0

    # soma das métricas vindas da TABELA area (para comments_number e score somado)
    tops_metrics = _fetch_area_metrics_for_ids(survey_id, valid_rows["area_id"].astype(int).tolist()).fillna(0)
    area_comments_number = int(tops_metrics["area_comments_number"].sum()) if not tops_metrics.empty else 0
    area_score_sum       = float(tops_metrics["area_score"].sum()) if not tops_metrics.empty else 0.0
    # response rate geral como respondentes/funcionarios
    area_response_rate = (respondentes / funcionarios) if funcionarios > 0 else 0.0

    general_json = {
        "metricas": {
            "funcionarios": funcionarios,
            "respondentes": respondentes,
            "adesao": adesao,
            "criticas": criticas,
            "sugestoes": sugestoes,
            "reconhecimentos": reconhecimentos,
            "neutros": neutros,
            "peso_area": 1.0,            # Geral representa o todo (pode deixar 1.0)
            "score_area": round(area_score_sum, 6)
        },
        "temas_citados": temas_citados,
        "recortes": recortes,
        "resumos_filhas": resumos_filhas
    }

    metrics_row = {
        "area_employee_number": funcionarios,
        "area_comments_number": area_comments_number,
        "area_criticism_number": criticas,
        "area_suggestions_number": sugestoes,
        "area_recognition_number": reconhecimentos,
        "area_response_rate": area_response_rate,
        "area_score": area_score_sum,
    }
    return general_json, metrics_row

# ----------------------------------------------------------
# Gera e salva o Resumo Executivo Geral (OpenAI)
# ----------------------------------------------------------
def generate_and_save_general_review(
    survey_id: int,
    model: str = "gpt-4o",
    temperature: float = 0.0,
    max_chars: int = 20000
) -> Union[str, bool]:
    """
    Gera o resumo da área geral (area_id = 0) com base exclusivamente no campo area_intents.
    Retorna o texto gerado (str) ou False se a área 0 não existir / não tiver dados.
    """
    SQL_SELECT_INTENTS = """
        SELECT area_intents
        FROM area
        WHERE area_survey_id = :sid AND area_id = 0
    """
    SQL_UPDATE_REVIEW = """
        UPDATE area
        SET area_review = :rev
        WHERE area_survey_id = :sid AND area_id = 0
    """

    # Busca o valor da primeira (e única) coluna
    with engine.begin() as conn:
        area_intents_value = conn.execute(
            text(SQL_SELECT_INTENTS), {"sid": survey_id}
        ).scalar_one_or_none()

    # Sem área 0 ou sem JSON válido
    if area_intents_value is None:
        return False

    # Normaliza para string JSON bonita
    if isinstance(area_intents_value, dict):
        intents_json_str = json.dumps(area_intents_value, ensure_ascii=False, indent=2)
    elif isinstance(area_intents_value, (str, bytes)):
        if isinstance(area_intents_value, bytes):
            area_intents_value = area_intents_value.decode("utf-8", errors="ignore")
        area_intents_value = area_intents_value.strip()
        if not area_intents_value:
            return False
        try:
            parsed = json.loads(area_intents_value)
            intents_json_str = json.dumps(parsed, ensure_ascii=False, indent=2)
        except json.JSONDecodeError:
            # Se vier string já “formatada”/não-JSON, usa como está
            intents_json_str = area_intents_value
    else:
        return False

    if max_chars and len(intents_json_str) > max_chars:
        intents_json_str = intents_json_str[:max_chars]

    prompt = _build_general_review_prompt(intents_json_str)

    client = get_openai_client()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
    )
    content = resp.choices[0].message.content.strip()

    with engine.begin() as conn:
        conn.execute(text(SQL_UPDATE_REVIEW), {"sid": survey_id, "rev": content})

    return content

REVIEW_EXAMPLE = f"""
<div class="show_review">
<div><p>Os temas que demosntram maior insatisfação são Liderança e Gestão, Carga de Trabalho e Cultura e Valores Organizacionais, com relatos recorrentes 
sobre distanciamento da liderança, pressão por resultados e desalinhamento cultural.</p>
</div>

<div class='review_item'>Oportunidades:</div>
<div>
<ul>
<li>Percepções negativas sobre a liderança, incluindo falta de preparo, contradições, cobrança excessiva e ausência de apoio.</li>
<li>Carga de trabalho considerada excessiva e metas desconectadas do contexto da área.</li>
<li>Percepção de benefício alimentação abaixo do mercado.</li>
</ul>
</div>

<div class='review_item'>Destaques:</p>
<divp>
<ul>
<li>Orgulho pela cultura da empresa e identificação com o produto e as pessoas.</li>
<li>Colaboração entre colegas e ambiente de equipe coeso.</li>
</ul>
</div>
</div>
"""

def _build_general_review_prompt(json_geral: dict | str) -> str:
    if not isinstance(json_geral, str):
        json_geral = json.dumps(json_geral, ensure_ascii=False, indent=2)

    return f"""
Você é um especialista em análise de dados quantitativos de pesquisa de engajamento. Sua tarefa é analisar dados estruturados em JSON que representam o resultado de todas as áreas da classificação de comentários de uma pesquisa de clima organizacional. 

Seu objetivo é gerar um resumo executivo geral da empresa com base nos dados (Json) fornecidos das principais áreas da empresa.

O resumo deve:
- Analisar os comentários dos funcionários das principais áreas e sugerir *oportunidades de melhoria* (comentários de críticas e sugestões) e *destaques* (comentários de reconhecimentos). 
- Apresentar as sugestões em formato de bulet point, onde cada linha deve representar uma oportunidade de melhoria ou reconhecimento.
- Concentrar as oportunidades de melhoria e reconhecimento em até 3 bulet points. Se fizer sentido, resuma 2 ou mais bulet points em apenas 1.
- Utilizar as palavras mais encontradas nos recortes.
- Ser técnico. É um resumo geral que será apresentado para o CEO.
- Não dividir o resumo por áreas e não citar o nome das áreas.
- Utilizar uma linguagem objetiva e não alarmante. Evite termos como 'muito', 'fortemente', 'extremamente', 'severa' e 'urgente'.
- Evite repetir dados. Concentre-se na interpretação dos resumos das áreas e suas implicações práticas.
- Não de sugestões de solução, foque no problema.

O output deve ser em html e ter o seguinte formato:
<div id="show_review">
<div><p><Análise enxuta dos dados></p></div>
<div id="review_item">Oportunidades:</div> <div><ul><lista das oportunidades de melhoria></ul></div>
<div id="review_item">Destaques:</div> <div><ul><lista dos pontos de reconhecimentos><ul></div>
</div>

Não utilize cercas Markdown de início/fim no output. Ex: ```html ... ```

Dados de entrada, estruturados em JSON, que representam a percepção de todos os colaboradores da empresa encontradas nos comentários da pesquisa de clima organizacional:
{json_geral}

Exemplo de Resposta:
{REVIEW_EXAMPLE}
"""

def generate_general_area_review(
    survey_id: int,
    model: str = "gpt-4o",
    temperature: float = 0.0,
    overwrite: bool = False
) -> bool:
    """
    Usa o JSON da área 0 para gerar o 'Resumo Executivo' via OpenAI e grava em area_review.
    Retorna True se gravou algo novo.
    """
    # pega o JSON da área geral (assumindo compute_general_area_metrics_and_json já rodou)
    s = fetch_area_intents(survey_id, area_id=0)
    if not s or str(s).strip() in ("", "{}", "[]"):
        # nada para resumir
        return False

    # se não for overwrite e já existir review, não gera
    if not overwrite:
        from sqlalchemy import text
        with engine.begin() as conn:
            row = conn.execute(
                text("""
                    SELECT area_review FROM area
                     WHERE area_survey_id = :sid AND area_id = 0
                """),
                {"sid": survey_id}
            ).first()
        if row and str(row[0] or "").strip():
            return False

    client = get_openai_client()
    prompt = _build_general_review_prompt(s)
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        content = resp.choices[0].message.content.strip()

        update_area_review(survey_id, 0, content)
        return True
    except Exception as e:
        update_area_review(survey_id, 0, f"[ERRO AO GERAR RESUMO GERAL: {e}]")
        return False
    
def generate_and_save_general_plan(
    survey_id: int,
    model: str = "gpt-4o",
    temperature: float = 0.0,
    max_chars: int = 20000
) -> Union[str, bool]:
    """
    Gera o plano geral (area_id = 0) com base no area_intents (área 0) e nos reviews das demais áreas.
    Retorna o texto gerado (str) ou False se a área 0 não existir / não tiver dados.
    """
    objetivos = "Melhorar a qualidade de vida dos colaboradores"
    restricoes = "Nenhuma restrição"

    SQL_SELECT_INTENTS = """
        SELECT area_intents
        FROM area
        WHERE area_survey_id = :sid AND area_id = 0
    """
    # pegamos nome + review das áreas != 0
    SQL_SELECT_REVIEWS = """
        SELECT area_name, area_review
        FROM area
        WHERE area_survey_id = :sid AND area_id != 0
    """
    SQL_UPDATE_PLAN = """
        UPDATE area
        SET area_plan = :rev
        WHERE area_survey_id = :sid AND area_id = 0
    """

    # ---- Intents da área 0
    with engine.begin() as conn:
        area_intents_value = conn.execute(
            text(SQL_SELECT_INTENTS), {"sid": survey_id}
        ).scalar_one_or_none()

    if area_intents_value is None:
        return False

    if isinstance(area_intents_value, dict):
        intents_json_str = json.dumps(area_intents_value, ensure_ascii=False, indent=2)
    elif isinstance(area_intents_value, (str, bytes)):
        if isinstance(area_intents_value, bytes):
            area_intents_value = area_intents_value.decode("utf-8", errors="ignore")
        area_intents_value = area_intents_value.strip()
        if not area_intents_value:
            return False
        try:
            parsed = json.loads(area_intents_value)
            intents_json_str = json.dumps(parsed, ensure_ascii=False, indent=2)
        except json.JSONDecodeError:
            intents_json_str = area_intents_value  # já é string utilizável
    else:
        return False

    if max_chars and len(intents_json_str) > max_chars:
        intents_json_str = intents_json_str[:max_chars]

    # ---- Monta areas_review (nome da área + review, separados por divisores)
    with engine.begin() as conn:
        rows = conn.execute(text(SQL_SELECT_REVIEWS), {"sid": survey_id}).mappings().fetchall()

    parts = []
    for r in rows:
        name = (r.get("area_name") or "").strip()
        review = r.get("area_review")
        if review is None:
            continue
        if isinstance(review, bytes):
            review = review.decode("utf-8", errors="ignore")
        review = str(review).strip()
        if not review:
            continue

        # bloco: título (nome da área) + linha divisória + review
        parts.append(f"{name}\n---\n{review}")

    areas_review = "\n\n===== ===== =====\n\n".join(parts) if parts else ""

    # (opcional) limitar tamanho do bloco de reviews
    if max_chars and len(areas_review) > max_chars:
        areas_review = areas_review[:max_chars]

    # ---- Prompt e chamada ao modelo
    prompt = _build_general_plan_prompt(intents_json_str, areas_review, objetivos, restricoes)

    client = get_openai_client()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
    )
    content = resp.choices[0].message.content.strip()
    match = re.search(r'<div id="show_review">.*?</div>\s*$', content, flags=re.DOTALL)
    if match:
            content = match.group(0)

    # ---- Persistência do plano na área 0
    with engine.begin() as conn:
        conn.execute(text(SQL_UPDATE_PLAN), {"sid": survey_id, "rev": content})

    return content

def _build_general_plan_prompt(json_geral: dict | str, areas_review, objetivos, restricoes) -> str:
    if not isinstance(json_geral, str):
        json_geral = json.dumps(json_geral, ensure_ascii=False, indent=2)

    REVIEW_EXAMPLE = f"""
<div class="show_review">
<div class='review_item'>Resumo:</div>
<div><p>As ações propostas estão direcionadas em resolvr problemas relacionados à insatisfação são Liderança e Gestão, Carga de Trabalho e Cultura e Valores Organizacionais, com relatos recorrentes 
sobre distanciamento da liderança, pressão por resultados e desalinhamento cultural.</p>
</div>

<div class='review_item'>Plano de ação:</div>
<div>
<ul>
<li>Aumentar o valor do vale alimentação para um valor competitivo no mercado.</li>
<li>Rodar uma pesquisa para ter o diagnóstico de Burnout da empresa</li>
</ul>
</div>

</div>
    """    

    return f"""
#Objetivo    
Você é um especialista em cultura, engajamento e clima organizacional.
Sua tarefa é elaborar um plano de ação geral para apresentar ao CEO da empresa com base nos dados da pesquisa de clima organizacional.

#instruções
- Leia atentamente aos recortes, indicadores da área geral e resumos das áreas. As ações sugeridas devem **responder diretamente às situações relatadas**
- Priorize sugerir soluções para os temas com maior volume de críticas e sugestões.
- Concentrar as ações em até 3 bulet points.
- Evite sugestões genéricas como “melhorar a comunicação” ou “valorizar os colaboradores”. Seja técnico e específico.
- Caso uma lista de restrições seja fornecida, **Não inclua ações consideradas inviáveis**.
- Caso uma lista de objetivos seja fornecida, priorize as ações de maior impacto nos objetivos da organização.
- Utilizar uma linguagem objetiva e não alarmante. Evite termos como 'muito', 'fortemente', 'extremamente', 'severa' e 'urgente'.
- Evite repetir dados.

#Output
O output sem em html e ter o seguinte formato:

<div class="show_review">
<div><p><Análise do plano de ação proposto></p></div>
<div class="review_item">Plano de ação:</div> <div><ul><lista de ações></ul></div>
</div>

Não utilize cercas Markdown de início/fim no output. Ex: ```html ... ```

#Dados de entrada: 

Objetivos da organização:
{objetivos}

Ações que devem ser evitadas:
{restricoes}

Indicadores da área geral e recorte de todos os comentários organizados por temas e intenção:
{json_geral}

Resumo das oportunidades de melhoria e reconhecimentos de todas as áreas:
{areas_review}

Exemplo de saída:
{REVIEW_EXAMPLE}
"""

def get_ranking_area(survey_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                  *
                FROM area
                WHERE area_survey_id = :sid
                AND area_score IS NOT NULL
                AND area_id != 0
                ORDER BY area_score
            """),
            conn,
            params={"sid": survey_id}
        )

    # Apenas reordena e filtra as colunas, se elas existirem
    expected_columns = [
        "area_id", "area_name",
        "area_criticism_number", "area_suggestions_number", "area_recognition_number",
        "area_response_rate", "area_score"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    return df

def get_ranking_general_themes(survey_id: int) -> pd.DataFrame:
    """
    Retorna o ranking de temas com score baseado em críticas e sugestões:
    crítica = 2 pontos, sugestão = 1 ponto

    Args:
        survey_id (int): ID do survey
    Returns:
        pd.DataFrame: colunas ['tema', 'qtd', 'score']
    """
    with engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                    perception_theme AS tema,
                    COUNT(*) AS qtd,
                    SUM(
                        CASE 
                            WHEN perception_intension = 'Crítica' THEN 2
                            WHEN perception_intension = 'Sugestão' THEN 1
                            ELSE 0
                        END
                    ) AS score
                FROM perception
                WHERE perception_survey_id = :sid
                  AND perception_intension IN ('Crítica', 'Sugestão')
                GROUP BY perception_theme
                ORDER BY score DESC
            """),
            conn,
            params={"sid": survey_id}
        )
    
    return df
