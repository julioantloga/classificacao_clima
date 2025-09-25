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
    SQL_SELECT_REVIEWS = """
        SELECT area_name, area_review, area_intents
        FROM area
        WHERE area_survey_id = :sid AND area_parent = 0
    """
    SQL_UPDATE_REVIEW = """
        UPDATE area
        SET area_review = :rev
        WHERE area_survey_id = :sid AND area_id = 0
    """
    
    #Busca áreas filhas da área Geral e adiciona em um dicionário
    with engine.begin() as conn:
        df = pd.read_sql(text(SQL_SELECT_REVIEWS), conn, params={"sid": survey_id})

    # 2. Filtrar e sobrescrever area_intents com apenas as chaves desejadas
    def clean_intents(raw_json):
        try:
            intents = json.loads(raw_json)
        except (TypeError, json.JSONDecodeError):
            return {}

        return {
            "metricas": intents.get("metricas", {}),
            "ranking_temas_criticados": intents.get("ranking_temas_criticados", []),
            "ranking_temas_reconhecidos": intents.get("ranking_temas_reconhecidos", [])
        }

    df["area_intents"] = df["area_intents"].apply(clean_intents)

    blocos = []
    for _, row in df.iterrows():
        area_name = row["area_name"]
        area_review = (row["area_review"] or "").strip()
        area_intents = json.dumps(row["area_intents"], ensure_ascii=False, indent=2)

        bloco = (
            f"#Resumo {area_name}:\n"
            f"métricas da área: {area_intents}\n"
            f"texto do resumo: {area_review}\n---"
        )
        blocos.append(bloco)

    # 4. Consolidar tudo
    area_reviews_text = "\n".join(blocos)

    # Adiciona resumos das áreas filhas no JSON da área geral
    # try:
    #     intents_dict = json.loads(intents_json_str)
    # except json.JSONDecodeError:
    #     return False
    
    # intents_dict["resumo_areas"] = area_reviews_dict
    # intents_json_str = json.dumps(intents_dict, ensure_ascii=False, indent=2)

    prompt = _build_general_review_prompt(area_reviews_text)

    client = get_openai_client()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
    )
    content = resp.choices[0].message.content.strip()

    prompt_ajust = _build_general_ajust_review_prompt(content)
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt_ajust}],
        temperature=temperature,
    )
    content_ajust = resp.choices[0].message.content.strip()   

    with engine.begin() as conn:
        conn.execute(text(SQL_UPDATE_REVIEW), {"sid": survey_id, "rev": content_ajust})

    return content_ajust

def _build_general_review_prompt(area_reviews_text):

    REVIEW_EXAMPLE = f"""
Na análise geral, podemos perceber uma maior insatisfação referente à Carga de Trabalho e Comunicação Interna, com relatos e sugestões de melhorias sobre pressão por resultados e comunicação descentralizada. 
E as áreas que demandam mais atenção são área x, y e z. Por outro lado, percebse reconhecimento referente à Cultura.

Oportunidades:
Percepções negativas sobre a liderança, incluindo falta de preparo para lidar com problemas difíceis e contradições.
Percepção de Carga de trabalho excessiva e metas desconectadas da realidade.
Percepção de benefício alimentação abaixo do mercado.

Destaques:
Percepção de pertencimento e orgulho pela cultura da empresa e identificação com os produtos e as pessoas.
Percepção positiva sobre a colaboração entre colegas de trabalho.
Novo escritório percebido como algo que motivou os colaboradores.
"""

    return f"""
Você é um especialista em pesquisa de clima organizacional.
Seu objetivo é criar um resumo executivo com oportunidades de melhoria e pontos de reconhecimento, a partir da análise prévia de cada área da empresa.

Crie o resumo:
- Criar um resumo executivo, enxuto, a partir do resumo das áreas da empresa listados abaixo.
- Evidencie as áreas da empresa com menor satisfação e engajamento, ou seja, aquelas com menor score_area. Não cite o termo "score_area" no resumo.
- Apresente até 3 *oportunidades de melhoria* com base nas oportunidades mais cidatas.
- Apresente até 3 *reconhecimentos* com base nos reconhecimentos mas citados.
- Apresentar as oportunidades e reconhecimentos em formato de bulet point, onde cada linha deve representar uma oportunidade de melhoria ou reconhecimento.
- Evitar dar sugestões de solução, foque em trazer o que pode ser melhorado.

O output deve ter o seguinte formato:

<Análise enxuta dos dados>
Oportunidades:<lista das oportunidades de melhoria>
Destaques: <lista dos pontos de reconhecimentos>

RESUMO DAS ÁRES DA EMPRESA:
{area_reviews_text}

Exemplo de Resposta:
{REVIEW_EXAMPLE}
"""

def _build_general_ajust_review_prompt(general_review):

    REVIEW_EXAMPLE = f"""
<div class="show_review">
<div><p>Na análise geral, podemos perceber uma maior insatisfação referente à Carga de Trabalho e Comunicação Interna, com relatos e sugestões de melhorias sobre pressão por resultados e comunicação descentralizada. 
E as áreas que demandam mais atenção são área x, y e z. Por outro lado, percebse reconhecimento referente à Cultura.</p>
</div>

<div class='review_item'>Oportunidades:</div>
<div>
<ul>
<li>Percepções negativas sobre a liderança, incluindo falta de preparo para lidar com problemas difíceis e contradições.</li>
<li>Percepção de Carga de trabalho excessiva e metas desconectadas da realidade.</li>
<li>Percepção de benefício alimentação abaixo do mercado.</li>
</ul>
</div>

<div class='review_item'>Destaques:</p>
<divp>
<ul>
<li>Percepção de pertencimento e orgulho pela cultura da empresa e identificação com os produtos e as pessoas.</li>
<li>Percepção positiva sobre a colaboração entre colegas de trabalho.</li>
</ul>
</div>
</div>"""

    return f"""
#Objetivo    
Refinar, Normalizar e aprimorar o resumo geral da empresa.

Resumo geral:
{general_review}

#Gere um novo resumo a partir dessas instruções:
- Remova incoerências e contradições entre oportunidades e reconhecimentos.
-- por exemplo: Evite dizer que "a liderança enfrenta críticas por falta de apoio" em oportunidades e "A liderança é reconhecida por sua capacidade de gestão e apoio" em reconhecimentos.
-- outro exemplo: Citar temas críticos na análise que não estão no ranking de temas críticos.
- Elimine termos sensasionalistas como: 'muito', 'fortemente', 'extremamente', 'severa', 'incrível', 'urgente'... e troque por termos mais neutros.

O output deve ser em html e ter o seguinte formato:
<div id="show_review">
<div><p><Análise enxuta dos dados></p></div>
<div id="review_item">Oportunidades:</div> <div><ul><lista das oportunidades de melhoria></ul></div>
<div id="review_item">Destaques:</div> <div><ul><lista dos pontos de reconhecimentos><ul></div>
</div>

Não utilize cercas Markdown de início/fim no output. Ex: ```html ... ```

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
                ORDER BY area_score ASC
                LIMIT 3 
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
                 LIMIT 3
            """),
            conn,
            params={"sid": survey_id}
        )
    
    return df

def save_general_ranking(ranking):

    df_to_insert = ranking[[
        "theme_name",
        "nota_geral",
        "nota_direta",
        "survey_id",
        "ranking",
        "ranking_direta"
    ]].rename(columns={
        "theme_name": "theme_name",
        "nota_geral": "score",
        "nota_direta": "direct_score",
        "survey_id" : "survey_id",
        "ranking":"ranking",
        "ranking_direta": "direct_ranking" 
    })

    df_to_insert["area_id"] = 0
    df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)
    rows: List[dict] = df_to_insert.to_dict(orient="records")

    sql = text("""
        INSERT INTO theme_ranking (
            theme_name, score, direct_score, survey_id, ranking, direct_ranking, area_id
        ) VALUES (
            :theme_name, :score, :direct_score, :survey_id, :ranking, :direct_ranking, :area_id
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

def get_critical_theme_ranking(survey_id):
    """

    """
    with engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                    *
                FROM theme_ranking
                WHERE survey_id = :sid
                  AND area_id = 0
                ORDER BY ranking
                LIMIT 3 
            """),
            conn,
            params={"sid": survey_id}
        )
  
    return df


def get_general_action_plan(survey_id):
    """

    """
    with engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                    *
                FROM action_plan
                WHERE action_plan_survey_id = :sid
                  ORDER BY id 
            """),
            conn,
            params={"sid": survey_id}
        )
    
    return df
    
def get_comment_clippings_for_critical_themes(survey_id):
    """
    Retorna um DataFrame com os 3 temas mais críticos e os respectivos recortes de comentários (texto + intenção).
    """
    # Passo 1: obter os 3 temas mais críticos
    critical_themes_df = get_critical_theme_ranking(survey_id)
    critical_theme_names = tuple(critical_themes_df["theme_name"].tolist())
    print (critical_theme_names)

    # Passo 2: buscar os comentários relacionados aos temas críticos
    with engine.begin() as conn:
        comments_df = pd.read_sql(
            text("""
                SELECT
                    perception_theme,
                    perception_comment_clipping,
                    perception_intension
                FROM perception
                WHERE perception_survey_id = :sid
                  AND perception_theme IN :themes
            """),
            conn,
            params={
                "sid": survey_id,
                "themes": critical_theme_names
            }
        )

    # Passo 3: organizar os recortes por tema
    grouped = comments_df.groupby("perception_theme").apply(
        lambda g: [
            {
                "comment": row["perception_comment_clipping"],
                "intension": row["perception_intension"]
            } for _, row in g.iterrows()
        ]
    ).reset_index(name="recortes")

    # Passo 4: combinar com o ranking
    final_df = pd.merge(
        critical_themes_df,
        grouped,
        how="left",
        left_on="theme_name",
        right_on="perception_theme"
    ).drop(columns=["perception_theme"])

    return final_df

def _build_prompt_for_theme(theme_name, recortes, predefined_plans_json):
    """
    Gera o prompt para um tema específico com base nos recortes e planos pré-definidos.
    """
    prompt = f"""
#Objetivo
Você é um especialista em gestão organizacional e o seu objetivo é sugerir um plano de ação para a gestão de RH da empresa.

Abaixo estão informações coletadas de uma pesquisa de clima sobre o tema "{theme_name}", que está entre os mais críticos na organização.

### Contexto da empresa: 
A Mindsight é uma startup que produz soluções tecnológicas para o mercado de RH com foco em gestão de talentos. Em resumo, é uma empresa que trabalha 100% em home office, com colaboradores espalhados por todo o Brasil, e oferece espaço físico em São Paulo para quem quiser trabalhar de lá. 
A empresa possui um base científica forte e tem os seguintes valores como pilares: Responsabilidade compartilhada, Empreendedorismo, Autonomia, Flexibilidade e Autenticidade.

### Comentários dos colaboradores:
Estes são os recortes dos comentários associados a este tema. Cada recorte contém o comentário e sua intenção percebida (reconhecimento, sugestão, crítica ou neutra):

{json.dumps(recortes, indent=2, ensure_ascii=False)}

### Planos de ação disponíveis:
Você deve sugerir um ou mais planos de ação a partir desta lista, adaptando se necessário. Use esta base como referência metodológica:

{predefined_plans_json}

### Instruções:
- Analise os comentários para identificar os problemas mais citados ou mais críticos. 
- Escolha as ações mais adequados da lista de planos disponíveis para criar o plano de ação. 
- O plano de ação deve ser somente uma lista de ações lógicas e conectadas com o contexto da empresa. 
- Personalize os planos sugeridos conforme os comentários.

Retorne no formato markdown apenas um nome para o plano, a lista de ações do plano e uma justificativa, sem repetir comentários.

Exemplo de output:
### Plano de Ação sugerido para **Melhoria da Comunicação Interna na Mindsight**
- **Rotina de Feedback Contínuo**: Estabelecer uma rotina de feedback contínuo entre líderes e liderados para garantir que as opiniões dos colaboradores sejam ouvidas e consideradas nas decisões.   
- **Comunicação Transparente**: Implementar uma comunicação transparente sobre decisões estratégicas e mudanças organizacionais, garantindo que todos os colaboradores estejam cientes das direções e estratégias da empresa.
- **Canais de Comunicação Dedicados**: Criar canais de comunicação dedicados para anúncios importantes, separando-os de outras mensagens para evitar que informações cruciais se percam.
- **Momentos de 'Conversa Franca' e Q&A**: Organizar sessões regulares de 'conversa franca' e Q&A com a gestão para discutir abertamente as preocupações dos colaboradores e esclarecer dúvidas sobre estratégias e decisões.
- **Mapeamento e Divulgação de Processos**: Mapear e divulgar claramente os principais processos internos, como contratação, promoção e demissão, para aumentar a transparência e a compreensão entre os colaboradores.
    """

    return prompt.strip()

def generate_action_plans(critical_df, predefined_plans_json, survey_id):
    """
    Para cada tema crítico no DataFrame, gera um plano de ação baseado nos comentários e planos disponíveis.
    """
    client = get_openai_client()
    model = "gpt-4o"
    temperature = 0.0

    action_plan_results = []

    predefined_plans = json.loads(predefined_plans_json)

    for _, row in critical_df.iterrows():
        theme_name = row["theme_name"]
        ranking = row["ranking"]
        recortes = row["recortes"]

        plano_tema = next((item for item in predefined_plans if item["tema"] == theme_name), None)

        prompt = _build_prompt_for_theme(
            theme_name=theme_name,
            recortes=recortes,
            plano_tema=plano_tema
        )

        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
            )
            content = response.choices[0].message.content.strip()

            action_plan_results.append({
                "theme_name": theme_name,
                "ranking": ranking,
                "generated_action_plan": content
            })

            insert_action_plan(theme_name, content, survey_id, type = 1)

        except Exception as e:
            action_plan_results.append({
                "theme_name": theme_name,
                "ranking": ranking,
                "generated_action_plan": f"Erro ao gerar plano: {str(e)}"
            })

    #Cria resumo dos planos de ação
    overview_prompt =  f"""
Você é um especialista em clima organizacional e precisa criar um resumo dos planos de ação definidos para o RH da empresa

### Contexto da empresa: 
A Mindsight é uma startup que produz soluções tecnológicas para o mercado de RH com foco em gestão de talentos. Em resumo, é uma empresa que trabalha 100% em home office, com colaboradores espalhados por todo o Brasil, e oferece espaço físico em São Paulo para quem quiser trabalhar de lá. 
A empresa possui um base científica forte e tem os seguintes valores como pilares: Responsabilidade compartilhada, Empreendedorismo, Autonomia, Flexibilidade e Autenticidade.

### Instruções:
- Avalie e resuma os planos de ação definidos abaixo
- Os planos de ação abaixo estão relacionados aos temas mais críticos encontrados na pesquisa.
- Considere que os planos de ação foram gerados com base nas notas e nos comentários da pesquisa
- Seja objetivo, evite textos muito longos.

### Planos de ação:
{action_plan_results}
    """

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": overview_prompt}],
            temperature=temperature,
        )
        plan_review = response.choices[0].message.content.strip()
        insert_action_plan("Geral", plan_review, survey_id, type = 0)
    except Exception as e:
        return e


    return pd.DataFrame(action_plan_results), plan_review

def insert_action_plan(theme_name, content, survey_id, type):
    """

    """
    sql = text("""
        INSERT INTO action_plan (
            theme_name, action_plan, action_plan_survey_id, tipo
        ) VALUES (
            :theme_name, :content, :survey_id, :type
        )
    """)

    row = {
        "theme_name": theme_name,
        "content": content,
        "survey_id": survey_id,
        "type": type
    }

    with engine.begin() as conn:
        conn.execute(sql, row)

