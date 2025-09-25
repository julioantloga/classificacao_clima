# service/areas_service.py
from __future__ import annotations
import pandas as pd
from typing import Dict, List,  Callable, Optional
import json
from .openai_client import get_openai_client
from db_config import engine
from sqlalchemy import text
from collections import deque
import re
import numpy as np


from .areas_repository import (
    ensure_general_area,
    fetch_survey_areas,
    fetch_survey_employees,
    fetch_survey_comments,
    fetch_survey_perceptions,
    update_area_metrics_bulk,
    fetch_survey_areas_with_intents,
    get_area_perceptions,
    get_themes_score
)

# ============================================================
# Definição do Organograma
# ============================================================

def create_organizational_chart(
    df_instancia_areas: pd.DataFrame,
    df_hierarquia_areas: pd.DataFrame,
    area_survey_id: int | None = None,
) -> pd.DataFrame:
    """
    Retorna DataFrame pronto para inserir na tabela `area` com colunas:
      - area_id, area_name, area_parent, area_level, area_survey_id

    Regras:
      - Inclui TODAS as áreas (sem recorte de níveis).
      - Identifica os níveis com base no parent real: os topos originais recebem nível 0.
      - Se existir MAIS DE UM topo, cria a área 'Geral' (area_id=0, parent=NULL, level=0),
        reparenta todos os topos para a Geral e incrementa em +1 o nível de todas as áreas.
      - Se houver APENAS UM topo, não cria a Geral.

    Observações:
      - Pais inválidos (nulos, texto não numérico ou que não existam em `id`) são tratados como raiz (parent=NULL).
      - Evita loops simples no cálculo de níveis.
    """

    # ============== 1) Normaliza chaves e faz o merge instância ↔ hierarquia ==============
    # Validação mínima de colunas (opcional; comente se preferir sem guard-rails)
    req_inst = {"id", "name"}
    req_hier = {"area", "parent"}
    if not req_inst.issubset(df_instancia_areas.columns):
        faltam = ", ".join(sorted(req_inst - set(df_instancia_areas.columns)))
        raise ValueError(f"df_instancia_areas sem colunas obrigatórias: {faltam}")
    if not req_hier.issubset(df_hierarquia_areas.columns):
        faltam = ", ".join(sorted(req_hier - set(df_hierarquia_areas.columns)))
        raise ValueError(f"df_hierarquia_areas sem colunas obrigatórias: {faltam}")

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

    # ============== 2) Base (sem nível) e saneamento de parents ==============
    base = pd.DataFrame({
        "area_id":     pd.to_numeric(merged["id"], errors="coerce").astype("Int64"),
        "area_name":   merged["name"].astype(str),
        "area_parent": pd.to_numeric(merged["parent"], errors="coerce").astype("Int64"),
    })

    # remove duplicidades por area_id (se vierem); mantemos a primeira ocorrência
    base = base.dropna(subset=["area_id"]).drop_duplicates(subset=["area_id"], keep="first")

    # conjunto de IDs válidos (apenas das áreas do arquivo)
    all_ids: set[int] = set(base["area_id"].astype(int).tolist())

    # parent inválido (nulo, não-int ou não existente) vira NULL (raiz)
    def norm_parent(p) -> Optional[int]:
        if pd.isna(p):
            return None
        try:
            v = int(p)
        except Exception:
            return None
        return v if v in all_ids else None

    base["area_parent"] = base["area_parent"].apply(norm_parent).astype("Int64")

    # ============== 3) Descobre raízes originais e calcula níveis (sem Geral) ==============
    # raiz = parent NULL
    roots: List[int] = base.loc[base["area_parent"].isna(), "area_id"].astype(int).tolist()
    roots = sorted(set(roots))  # exclusão de duplicidades

    # constrói mapa de filhos ignorando pais NULL
    children: Dict[int, List[int]] = {}
    for _, r in base.iterrows():
        aid = int(r["area_id"])
        pid = r["area_parent"]
        if pd.isna(pid):
            continue
        pid = int(pid)
        if aid == pid:
            # evita laço trivial (uma área apontando para si mesma)
            continue
        children.setdefault(pid, []).append(aid)

    # BFS multi-fonte: todos os roots começam com nível 0
    levels: Dict[int, int] = {}
    q = deque(roots)
    for rt in roots:
        levels[rt] = 0

    while q:
        cur = q.popleft()
        for ch in children.get(cur, []):
            if ch == cur or ch in levels:
                continue
            levels[ch] = levels[cur] + 1
            q.append(ch)

    # aplica níveis (áreas isoladas de um root — improvável após saneamento — ficam <NA>)
    base["area_level"] = base["area_id"].apply(lambda x: levels.get(int(x), pd.NA)).astype("Int64")

    # ============== 4) Se houver mais de um topo, cria Geral e reparenta topos ==============
    if len(roots) > 1:
        # cria Geral (id=0), parent=NULL, level=0
        geral_row = pd.DataFrame([{
            "area_id": 0,
            "area_name": "Geral",
            "area_parent": pd.NA,
            "area_level": 0
        }])

        # torna todos os roots filhos da Geral
        # (parent 0), e eleva o nível de todas as áreas em +1
        base.loc[base["area_id"].isin(roots), "area_parent"] = 0
        base["area_level"] = base["area_level"].apply(lambda v: (int(v) + 1) if not pd.isna(v) else pd.NA).astype("Int64")

        # concatena Geral
        full = pd.concat([geral_row, base], ignore_index=True)
        full = full.drop_duplicates(subset=["area_id"], keep="first")

    elif len(roots) == 1:
        # apenas um root: transforma ele em Geral
        root_id = roots[0]
        
        # atualiza o root para virar a área Geral
        base.loc[base["area_id"] == root_id, ["area_id","area_name", "area_parent", "area_level"]] = [
            0, "Geral", pd.NA, 0
        ]
        
        # atualiza os filhos do root para terem parent = 0
        base.loc[base["area_parent"] == root_id, "area_parent"] = 0

        full = base.copy()

    else:
        # sem root (caso extremo), mantém como está
        full = base.copy()

    # ============== 5) Finaliza com area_survey_id e ordenação ==============
    full["area_survey_id"] = area_survey_id
    full = full.loc[:, ["area_id", "area_name", "area_parent", "area_level", "area_survey_id"]]
    # ordena: nível (NA primeiro, por segurança), depois parent, depois id
    full = full.sort_values(["area_level", "area_parent", "area_id"], na_position="first").reset_index(drop=True)
    return full

# ============================================================
# Definição das Métricas de Áreas
# ============================================================

def _normalize_intent(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip().lower()
    if s.startswith("reconhec"):
        return "reconhecimento"
    if s.startswith("crític") or s.startswith("critic"):
        return "critica"
    if s.startswith("sugest"):
        return "sugestao"
    if s.startswith("neutr"):
        return "neutro"
    return None

def _compute_area_levels(areas: pd.DataFrame) -> pd.Series:
    """
    Calcula nível da área (0 = topo) de forma robusta:
    - trata auto-parent (area_id == area_parent) como topo
    - quebra ciclos (A->B->A) e IDs de parent inexistentes
    - trata parent não-numérico como nulo
    """
    # mapa area_id -> parent (int ou None)
    def to_int_or_none(x):
        try:
            if pd.isna(x) or x is None or str(x).strip() == "":
                return None
            return int(x)
        except Exception:
            return None

    parent = {int(r.area_id): to_int_or_none(r.area_parent) for _, r in areas.iterrows()}
    memo: Dict[int, int] = {}
    visiting: set[int] = set()

    def level_of(aid: int) -> int:
        # base: geral
        if aid == 0:
            memo[aid] = 0
            return 0
        if aid in memo:
            return memo[aid]

        p = parent.get(aid, None)
        # topo: sem parent, parent inválido, ou auto-parent → nível 0
        if p is None or p == aid:
            memo[aid] = 0
            return 0

        # quebra ciclo
        if aid in visiting:
            memo[aid] = 0
            return 0

        visiting.add(aid)
        # se parent não existe no dict, trate como topo
        if p not in parent:
            lvl = 0
        else:
            lvl = level_of(p) + 1
        visiting.remove(aid)

        memo[aid] = lvl
        return lvl

    levels = {int(aid): level_of(int(aid)) for aid in areas["area_id"].tolist()}
    return pd.Series(levels, name="level")

def _build_children_map(areas: pd.DataFrame) -> Dict[int, List[int]]:
    def to_int_or_none(x):
        try:
            if pd.isna(x) or x is None or str(x).strip() == "":
                return None
            return int(x)
        except Exception:
            return None

    children: Dict[int, List[int]] = {}
    for _, row in areas.iterrows():
        aid = int(row["area_id"])
        pid = to_int_or_none(row["area_parent"])
        # ignora auto-parent (evita 0->0 e A->A)
        if pid is None or pid == aid:
            continue
        children.setdefault(pid, []).append(aid)
    return children

def build_recortes_by_theme_intent(sub_perceptions: pd.DataFrame) -> Dict[str, Dict[str, list]]:
    """
    Retorna:
    {
      "<Tema>": {
         "critica": [<recorte1>, <recorte2>, ...],
         "sugestao": [...],
         "reconhecimento": [...],
         "neutro": [...]
      },
      ...
    }
    """
    if sub_perceptions.empty:
        return {}

    df = sub_perceptions.copy()
    df["perception_theme"] = df["perception_theme"].fillna("Sem tema").astype(str).str.strip()
    df["intent_norm"] = df["intent_norm"].fillna("")
    df["perception_comment_clipping"] = df["perception_comment_clipping"].fillna("").astype(str).str.strip()

    # filtra recortes não vazios
    df = df[df["perception_comment_clipping"] != ""]

    # inicializa estrutura
    out: Dict[str, Dict[str, list]] = {}

    for _, r in df.iterrows():
        tema = r["perception_theme"]
        intent = r["intent_norm"]  # 'critica', 'sugestao', 'reconhecimento', 'neutro'
        clip = r["perception_comment_clipping"]
        if not intent:
            continue  # ignora intenções não mapeadas

        bucket = out.setdefault(tema, {"critica": [], "sugestao": [], "reconhecimento": [], "neutro": []})
        bucket[intent].append(clip)

    return out

def _descendants(area_id: int, children_map: Dict[int, List[int]]) -> List[int]:
    stack = [area_id]
    out: List[int] = []
    visited: set[int] = set()
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        out.append(cur)
        for ch in children_map.get(cur, []):
            if ch not in visited:
                stack.append(ch)
    return out

# ---- métricas individuais ----
def metric_employee_number(subtree_employees: pd.DataFrame) -> int:
    return int(subtree_employees["employee_id"].nunique())

def metric_comments_number(subtree_comments: pd.DataFrame) -> int:
    return int(subtree_comments["comment_id"].nunique())

def metric_intent_counts(subtree_perceptions: pd.DataFrame) -> Dict[str, int]:
    if subtree_perceptions.empty:
        return {"critica": 0, "sugestao": 0, "reconhecimento": 0, "neutro": 0, "total": 0}
    inten = subtree_perceptions["intent_norm"].value_counts().to_dict()
    c = {
        "critica": int(inten.get("critica", 0)),
        "sugestao": int(inten.get("sugestao", 0)),
        "reconhecimento": int(inten.get("reconhecimento", 0)),
        "neutro": int(inten.get("neutro", 0)),
    }
    c["total"] = sum(c.values())
    return c

def metric_response_rate(employee_number: int, commenters_number: int) -> float:
    if employee_number <= 0:
        return 0.0
    return float(commenters_number) / float(employee_number)

def metric_area_score(
    subtree_perceptions: pd.DataFrame,
    respondents_area: int,      # respondentes da área
    employees_area: int,        # total de funcionários na área
    employees_global: int,      # total de funcionários na empresa
    k: int = 5                  # suavização (Laplace smoothing)
) -> dict:
    """
    Calcula os scores de uma área a partir das percepções dos colaboradores.

    - Cada percepção tem um peso:
        reconhecimento=+2, sugestao=-1, critica=-2, neutro=0
    - Score de percepção (interno à área):
        média das notas dos respondentes / (respondentes_area + k)
        normalizado para [-100, 100]
    - Confiança:
        respondentes_area / employees_area
    - Peso da área na empresa:
        sqrt(employees_area / employees_global)
    - Score final (para ranking global):
        score_perception * confiança * peso_ajustado
    """
    # validações iniciais
    if employees_area <= 0 or employees_global <= 0 or respondents_area <= 0:
        return 0

    if subtree_perceptions.empty:
        return 0

    # pesos por tipo de percepção
    weight = {
        "reconhecimento":  2,
        "sugestao":       -1,
        "critica":        -2,
        "neutro":          0,
    }

    # soma dos pesos de todos os comentários (cada percepção pode contribuir)
    total_weight = subtree_perceptions["intent_norm"].map(weight).fillna(0).sum()

    # score de percepção da área (escala -100 a +100)
    media = total_weight / (respondents_area + k)
    score_perception = (media / 2.0) * 100.0

    # fatores de ajuste
    confidence = respondents_area / float(employees_area)          # taxa de resposta
    peso_ajustado = np.sqrt(employees_area / float(employees_global))

    # score final para ranking
    score_final = score_perception * confidence * peso_ajustado

    return float(score_final)

def metric_area_intents_json(
    counts: Dict[str, int],
    themes_counts: Dict[str, Dict[str, int]],
    area_employee_number: int,
    commenters_number: int,
    response_rate: float,
    peso_area: float,
    area_score: float,
    recortes_by_theme_intent: Dict[str, Dict[str, list]],
    notas_area: Dict[str, int] 
) -> str:
    
    # --- Rankings a partir de themes_counts ---
    W_CRIT = 2
    W_SUG  = 1

    # Ranking de piores (maior nota = pior): crit*2 + sug
    scores_piores = {
        tema: int(c.get("critica", 0) or 0) * W_CRIT + int(c.get("sugestao", 0) or 0) * W_SUG
        for tema, c in (themes_counts or {}).items()
    }
    itens_piores = sorted(scores_piores.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    ranking_temas_criticados = [
        {"tema": tema, "ranking_position": i}
        for i, (tema, _) in enumerate(itens_piores, 1)
    ]

    # Ranking de melhores (maior = melhor): reconhecimentos
    scores_melhores = {
        tema: int(c.get("reconhecimento", 0) or 0)
        for tema, c in (themes_counts or {}).items()
    }
    itens_melhores = sorted(scores_melhores.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    ranking_temas_reconhecidos = [
        {"tema": tema, "ranking_position": i}
        for i, (tema, _) in enumerate(itens_melhores, 1)
    ]

    # --- JSON final (sem 'temas_citados') ---
    data = {
        "metricas": {
            "funcionarios": int(area_employee_number),
            "respondentes": int(commenters_number),
            "adesao": round(float(response_rate) * 100.0, 2),
            "criticas": int(counts.get("critica", 0)),
            "sugestoes": int(counts.get("sugestao", 0)),
            "reconhecimentos": int(counts.get("reconhecimento", 0)),
            "neutros": int(counts.get("neutro", 0)),
            "peso_area": round(float(peso_area), 2),
            "score_area": round(float(area_score), 2),
        },
        "ranking_temas_criticados": ranking_temas_criticados,
        "ranking_temas_reconhecidos": ranking_temas_reconhecidos,
        "Nota das perguntas": notas_area,
        "recortes": recortes_by_theme_intent or {}
    }
    return json.dumps(data, ensure_ascii=False)

def compute_area_metrics_python(
    survey_id: int,
    df_notas_areas,
    min_level: int = 0,
    max_level: int = 999,
    min_commenters: int = 3,
) -> pd.DataFrame:
    """
    Calcula métricas por área em Python e retorna um DataFrame com:
      [area_id,
       area_employee_number,
       area_comments_number,
       area_criticism_number,
       area_suggestions_number,
       area_recognition_number,
       area_response_rate,
       area_intents (json str),
       area_score,
       (extra p/ debug) area_level]

    Observações:
    - Todas as métricas consideram a subárvore (área + descendentes).
    - O JSON salvo em area_intents segue o formato já acordado.
    - Áreas de nível 0 são sempre calculadas, mesmo fora do range informado.
    """

    # --- Carrega dados da base
    df_areas     = fetch_survey_areas(survey_id)
    df_employees = fetch_survey_employees(survey_id)
    df_comments  = fetch_survey_comments(survey_id)
    df_perc      = fetch_survey_perceptions(survey_id)

    # --- Sanitiza tipos/chaves mínimas
    for df, col in [(df_areas, "area_id"),
                    (df_employees, "employee_id"),
                    (df_employees, "employee_area_id"),
                    (df_comments, "comment_employee_id")]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "area_id" in df_areas.columns:
        df_areas = df_areas.dropna(subset=["area_id"]).copy()
        df_areas["area_id"] = df_areas["area_id"].astype(int)

    if not df_employees.empty:
        df_employees = df_employees.dropna(subset=["employee_id", "employee_area_id"]).copy()
        df_employees["employee_id"] = df_employees["employee_id"].astype(int)
        df_employees["employee_area_id"] = df_employees["employee_area_id"].astype(int)

    if not df_comments.empty:
        df_comments = df_comments.dropna(subset=["comment_employee_id"]).copy()
        df_comments["comment_employee_id"] = df_comments["comment_employee_id"].astype(int)

    # --- Níveis e mapa de filhos
    children = _build_children_map(df_areas)

    # --- Percepções enriquecidas com employee_id e normalização de intenção
    perc = df_perc.copy()
    if not perc.empty:
        perc = perc.merge(
            df_comments[["comment_id", "comment_employee_id", "comment_area_id", "comment_survey_id"]],
            left_on="perception_comment_id",
            right_on="comment_id",
            how="left"
        )

        # normaliza intenção
        perc["intent_norm"] = perc["perception_intension"].map(_normalize_intent)
        
        # garante coluna de recorte de comentário
        if "perception_comment_clipping" not in perc.columns:
            perc["perception_comment_clipping"] = pd.NA
    else:
        perc = pd.DataFrame(columns=[
            "perception_id", "perception_comment_id", "perception_intension", "perception_theme",
            "comment_id", "comment_employee_id", "comment_area_id", "comment_survey_id",
            "intent_norm", "perception_comment_clipping"
        ])

    # Comentadores globais do survey (para o peso da área)
    commenters_global = int(df_comments["comment_employee_id"].nunique()) if not df_comments.empty else 0
    
    # quantidade de pessoas da empresa toda
    employees_global = int(df_employees["employee_id"].nunique()) if not df_employees.empty else 0

    rows = []

    df_notas_areas = df_notas_areas[['area_id', 'pergunta', 'porcentagem insatisfeitos', 'nota', 'nota da empresa']].dropna()

    # --- Processa cada área
    for _, area_row in df_areas.iterrows():
        aid = int(area_row["area_id"])
        lvl = int(area_row.get("area_level", 0))

        # --- NOVA REGRA: sempre incluir áreas de nível 0
        if lvl != 0 and (lvl < min_level or lvl > max_level):
            continue

        # Subárvore (área + descendentes)
        sub_ids = _descendants(aid, children)
        sub_set = set(sub_ids)

        # Agrupa por pergunta e transforma cada grupo em um dicionário
        notas_area = (
            df_notas_areas
            .groupby("pergunta")
            .apply(lambda x: x.iloc[0].to_dict())  # Pega a primeira linha de cada pergunta
            .to_dict()
        )

        # quantidade de pessoas da área (considerando sub-áreas)
        sub_emp = df_employees[
            (df_employees["employee_area_id"].isin(sub_set)) &
            (df_employees["employee_survey_id"] == survey_id)
        ]
        area_employee_number = metric_employee_number(sub_emp)

        # Comentários feitos por esses funcionários
        if not df_comments.empty and area_employee_number > 0:
            sub_com = df_comments[
                (df_comments["comment_area_id"].isin(sub_set)) &
                (df_comments["comment_survey_id"] == survey_id)
            ]
        else:
            sub_com = df_comments.iloc[0:0]

        # Percepções ligadas a esses comentários
        if not perc.empty and not sub_com.empty:
            sub_perc = perc[
                (perc["comment_area_id"].isin(sub_set)) &
                (perc["comment_survey_id"] == survey_id)
            ]

        else:
            sub_perc = perc.iloc[0:0]

        # quantidade de pessoas que comentaram na área (considerando sub-áreas)
        area_commenters_number = int(sub_com["comment_employee_id"].nunique()) if not sub_com.empty else 0
        if area_commenters_number < min_commenters:
            continue

        # Contagens gerais por intenção
        intent_counts = metric_intent_counts(sub_perc)
        crit = intent_counts["critica"]
        sug  = intent_counts["sugestao"]
        rec  = intent_counts["reconhecimento"]

        # Contagem por tema × intenção
        themes_counts = metric_theme_counts(sub_perc)

        response_rate = metric_response_rate(area_employee_number, area_commenters_number)
        peso_area     = (area_employee_number / employees_global) if commenters_global else 0.0
        area_score    = metric_area_score(sub_perc, area_commenters_number, area_employee_number, employees_global)

        # Recortes organizados por tema × intenção
        recortes_by_theme_intent = build_recortes_by_theme_intent(sub_perc)

        # JSON final (para gravar em area_intents)
        intents_json = metric_area_intents_json(
            counts=intent_counts,
            themes_counts=themes_counts,
            area_employee_number=area_employee_number,
            commenters_number=area_commenters_number,
            response_rate=response_rate,
            peso_area=peso_area,
            area_score=area_score,
            recortes_by_theme_intent=recortes_by_theme_intent,
            notas_area = notas_area
        )

        # Linha de métricas para update em área
        rows.append({
            "area_id": aid,
            "area_level": lvl,
            "area_employee_number": int(area_employee_number),
            "area_comments_number": int(area_commenters_number),
            "area_criticism_number": int(crit),
            "area_suggestions_number": int(sug),
            "area_recognition_number": int(rec),
            "area_response_rate": round(float(response_rate),2),
            "area_intents": intents_json,
            "area_score": round(float(area_score),2),
        })

    res_df = pd.DataFrame(rows)
    print(f"[area-metrics] linhas calculadas: {len(res_df)}  (min_level={min_level}, max_level={max_level}, min_commenters={min_commenters})")
    return res_df

def compute_and_update_area_metrics_python(
    survey_id: int,
    df_notas_areas,
    min_level: int,
    max_level: int,
    min_commenters: int,
) -> int:
    """
    Calcula métricas em Python e persiste no Postgres.
    Retorna quantas áreas foram atualizadas.
    """

    df = compute_area_metrics_python(survey_id, df_notas_areas, min_level, max_level, min_commenters)
    return update_area_metrics_bulk(survey_id, df)

def compute_and_update_general_metrics(survey_id: int) -> int:
    """
    Consolida as métricas da área 'Geral' (area_id=0) a partir de suas filhas diretas (area_parent=0).

    Regras:
      - Se houver 0 filhas → zera tudo.
      - Se houver 1 filha  → copia exatamente as métricas da filha.
      - Se houver >1 filhas:
          * soma: employee, comments, criticism, suggestions, recognition, score (soma dos scores das filhas)
          * response_rate: média ponderada por funcionários -> sum(rr_i * emp_i) / sum(emp_i)
    Retorna 1 se atualizou a linha da Geral; 0 caso contrário.
    """
    from .areas_repository import fetch_survey_areas, fetch_area_metrics_for_ids, update_area_0_metrics

    # pega organograma para descobrir filhas diretas da Geral
    df_areas = fetch_survey_areas(survey_id)  # area_id, area_parent, area_name, area_survey_id
    if df_areas.empty:
        # nada a consolidar
        return update_area_0_metrics(
            survey_id,
            area_employee_number=0,
            area_comments_number=0,
            area_criticism_number=0,
            area_suggestions_number=0,
            area_recognition_number=0,
            area_response_rate=0.0,
            area_score=0.0,
        )

    # filhas diretas de 0 (e != 0)
    child_ids = (
        df_areas[(df_areas["area_parent"].fillna(0).astype(int) == 0) & (df_areas["area_id"].astype(int) != 0)]
        ["area_id"].astype(int).tolist()
    )

    if not child_ids:
        # sem filhas → zera
        return update_area_0_metrics(
            survey_id,
            area_employee_number=0,
            area_comments_number=0,
            area_criticism_number=0,
            area_suggestions_number=0,
            area_recognition_number=0,
            area_response_rate=0.0,
            area_score=0.0,
        )

    # busca métricas atuais das filhas
    dfm = fetch_area_metrics_for_ids(survey_id, child_ids)

    if dfm.empty:
        # sem métricas nas filhas → zera
        return update_area_0_metrics(
            survey_id,
            area_employee_number=0,
            area_comments_number=0,
            area_criticism_number=0,
            area_suggestions_number=0,
            area_recognition_number=0,
            area_response_rate=0.0,
            area_score=0.0,
        )

    if len(dfm) == 1:
        # copia exatamente
        r = dfm.iloc[0]
        return update_area_0_metrics(
            survey_id,
            area_employee_number=int(r["area_employee_number"]),
            area_comments_number=int(r["area_comments_number"]),
            area_criticism_number=int(r["area_criticism_number"]),
            area_suggestions_number=int(r["area_suggestions_number"]),
            area_recognition_number=int(r["area_recognition_number"]),
            area_response_rate=float(r["area_response_rate"]),
            area_score=float(r["area_score"]),
        )

    # agregado de múltiplas filhas
    emp_sum   = int(dfm["area_employee_number"].sum())
    com_sum   = int(dfm["area_comments_number"].sum())
    cri_sum   = int(dfm["area_criticism_number"].sum())
    sug_sum   = int(dfm["area_suggestions_number"].sum())
    rec_sum   = int(dfm["area_recognition_number"].sum())
    score_sum = float(dfm["area_score"].sum())

    # média ponderada por funcionários para a taxa de resposta
    if emp_sum > 0:
        rr_weighted = float((dfm["area_response_rate"] * dfm["area_employee_number"]).sum() / emp_sum)
    else:
        rr_weighted = 0.0

    return update_area_0_metrics(
        survey_id,
        area_employee_number=emp_sum,
        area_comments_number=com_sum,
        area_criticism_number=cri_sum,
        area_suggestions_number=sug_sum,
        area_recognition_number=rec_sum,
        area_response_rate=rr_weighted,
        area_score=score_sum,
    )

def metric_theme_counts(sub_perceptions: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """
    Retorna um dicionário: { tema -> {critica, sugestao, reconhecimento, neutro, total} }
    """
    if sub_perceptions.empty:
        return {}

    df = sub_perceptions.copy()
    # tema vazio -> "Sem tema"
    df["perception_theme"] = df["perception_theme"].fillna("Sem tema").astype(str).str.strip()
    df["intent_norm"] = df["intent_norm"].fillna("")

    # pivot por tema x intenção
    pivot = (
        df.pivot_table(
            index="perception_theme",
            columns="intent_norm",
            values="perception_id",
            aggfunc="count",
            fill_value=0
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    out: Dict[str, Dict[str, int]] = {}
    for _, row in pivot.iterrows():
        tema = row["perception_theme"]
        crit = int(row.get("critica", 0))
        sug  = int(row.get("sugestao", 0))
        rec  = int(row.get("reconhecimento", 0))
        neu  = int(row.get("neutro", 0))
        out[tema] = {
            "critica": crit,
            "sugestao": sug,
            "reconhecimento": rec,
            "neutro": neu,
            "total": crit + sug + rec + neu
        }
    return out

# ============================================================
# Resumo das áreas
# ============================================================

def _build_area_review_prompt(area_name: str, area_json: dict | str) -> str:

    REVIEW_EXAMPLE = f"""

Os comentários e as notas da pesquisa  demosntram maior insatisfação com Liderança e Gestão, Carga de Trabalho e Cultura e Valores Organizacionais, com relatos recorrentes 
sobre distanciamento da liderança, pressão por resultados e desalinhamento cultural. A área apresenta percepção abaixo da média da empresa em tópicos relacionados à benefícios e alinhamento com a estratégia.

Oportunidades:
Percepções negativas sobre a liderança, incluindo falta de preparo, contradições, cobrança excessiva e ausência de apoio.
Carga de trabalho considerada excessiva e metas desconectadas do contexto da área.
Falta de reconhecimento, baixa frequência de conversas individuais e desconexão com os valores da cultura.

Destaques:
Orgulho pela cultura da empresa e identificação com o produto e as pessoas.
Colaboração entre colegas e ambiente de equipe coeso.

Recortes:
"O auxílio home office está abaixo do que as outras empresas pagam"
"Eu amo trabalhar nessa empresa"
"Está tudo bem"...
    """
    
    if isinstance(area_json, str):
        json_str = area_json
    else:
        json_str = json.dumps(area_json, ensure_ascii=False, indent=2)

    return f"""
Você é um especialista em análise de dados qualitativos de pesquisa de engajamento. 
Sua objetivo é sugerir oportunidades de melhoria e emcontrar pontos de reconhecimento de uma área a partir dos dados obtidos na pesquisa de clima organizacional.

Nome da área: {area_name}

Faça a análise:
- Analizar dados de entrada (Json):
- Analisar se as métricas (exceto a adesão) trás algo que foge do comum.
- Analisar a nota das perguntas. Encontre insihts dentro das perguntas com maior numero de insatisfeitos e maior divergêcia com a nota da empresa.
- Analisar o ranking de temas criticados (ranking_temas_criticados) e os recortes relacionados a eles (recortes). O tema mais alto no ranking é o tema mais crítico.
- Analisar o ranking de temas reconhecidos (ranking_temas_reconhecidos) e os recortes relacionados a eles (recortes). O tema mais alto no ranking é o tema mais reconhecido.

Crie o resumo:
- Criar um resumo enxuto, para a diretoria, enfaizando os 3 temas com mais críticas ou sugestões da área.
- Enfatizar os 3 temas mais criticados. Seja coerente com o ranking.
- Enfatizar a pergunta com maior porcentagem de insatisfeito.
- Apresentar *oportunidades de melhoria* (a partir da análise dos rankings, dos recortes e das perguntas com insatisfeitos).
- Apresentar as oportunidades e reconhecimentos em formato de bulet point, onde cada linha deve representar uma oportunidade de melhoria ou reconhecimento.
- Apresentar a "voz" do colaborador, listando os recortes relacionados aos temas citados no resumo. Priorize recortes com maior recorrência.
- Evitar dar sugestões de solução, foque em trazer o que pode ser melhorado.


O output deve ter o seguinte formato:
<Análise enxuta dos dados da pesquisa>
Oportunidades:<lista das oportunidades de melhoria>
Destaques:<lista dos pontos de reconhecimentos>
Recortes:<lista recortes relacionados aos temas citados no resumo>

Dados de entrada, estruturados em JSON, que representam a percepção dos colaboradores encontradas nos comentários da área na pesquisa de clima organizacoinal:
{json_str}

Impórtante: Não utilize cercas Markdown de início/fim no output. Ex: ```html ... ```

Exemplo de Resposta:
{REVIEW_EXAMPLE}
"""

def _build_ajust_area_review_prompt(area_name: str, json_area: str, area_review: str = "") -> str:

    REVIEW_EXAMPLE = f"""
<div class="show_review">

<div><p>Os comentários e as notas da pesquisa  demosntram maior insatisfação com Liderança e Gestão, Carga de Trabalho e Cultura e Valores Organizacionais, com relatos recorrentes 
sobre distanciamento da liderança, pressão por resultados e desalinhamento cultural. A área apresenta percepção abaixo da média da empresa em tópicos relacionados à benefícios e alinhamento com a estratégia.
</p>
</div>

<div class='review_item'>Oportunidades:</div>
<div>
<ul>
<li>Percepções negativas sobre a liderança, incluindo falta de preparo, contradições, cobrança excessiva e ausência de apoio.</li>
<li>Carga de trabalho considerada excessiva e metas desconectadas do contexto da área.</li>
<li>Falta de reconhecimento, baixa frequência de conversas individuais e desconexão com os valores da cultura.</li>
</ul>
</div>

<div class='review_text'>Destaques:</p>
<div>
<ul>
<li>Orgulho pela cultura da empresa e identificação com o produto e as pessoas.</li>
<li>Colaboração entre colegas e ambiente de equipe coeso.</li>
</ul>
</div>
</div>

<div class='review_text'>Recortes:</p>
<div>
Os colaboradores dizem que estão amando trabalhar na empresa, dizendo que o auxílio home office está abaixo do que as outras empresas pagam. As palavras mais usadas foram: VR, Líder, distante, presente...
</div>
</div>
    """

    prompt = f"""
#Objetivo    
Refinar, Normalizar e aprimorar o resumo da {area_name}.

Resumo da área:
{area_review}

#Gere um novo resumo a partir dessas instrições
- Concentre as oportunidades de melhoria e reconhecimento em *até* 3 bulet points.
- Remova incoerências e contradições entre oportunidades e reconhecimentos.
-- por exemplo: Evite dizer que "a liderança enfrenta críticas por falta de apoio" em oportunidades e "A liderança é reconhecida por sua capacidade de gestão e apoio" em reconhecimentos.
- Elimine termos sensasionalistas como: 'muito', 'fortemente', 'extremamente', 'severa', 'incrível', 'urgente'... e troque por termos mais neutros.
- Faça um resumo dos recortes listados preservando as palavras dos colaboradores.


#Output
O output deve ser em html e seguir exatamento o formato do resumo de entrada:
<div class="show_review">
<div><p><Análise enxuta dos dados da pesquisa></p></div>
<div class="review_item">Oportunidades:</div> <div><ul><lista das oportunidades de melhoria></ul></div>
<div id="review_item">Destaques:</div> <div><ul><lista dos pontos de reconhecimentos><ul></div>
<div id="review_item">Recortes:</div> <div><resumo dos recortes></div>
</div>

Não utilize cercas Markdown de início/fim no output. Ex: ```html ... ```

Exemplo de saída:
{REVIEW_EXAMPLE}
"""
    return prompt.strip()

def _build_area_plan_prompt(area_name: str, json_area: str, area_review: str = "") -> str:

    ACTION_PLANS = """
[
  { "acao": "Revisar e Atualizar Sistemas de Gestão", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Automatizar Processos Manuais Repetitivos", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Implementar Programa Estruturado de Comunicação Interna", "tema": "Comunicação Interna" },
  { "acao": "Padronizar Comunicados Importantes da Liderança", "tema": "Comunicação Interna" },
  { "acao": "Implementar Treinamentos sobre Feedback Construtivo", "tema": "Liderança e Gestão" },
  { "acao": "Estruturar Rituais de Feedback Trimestrais", "tema": "Liderança e Gestão" },
  { "acao": "Aprimorar Qualidade das Avaliações de Desempenho", "tema": "Liderança e Gestão" },
  { "acao": "Implementar Programa de Reconhecimento por Resultados", "tema": "Reconhecimento e Valorização" },
  { "acao": "Implementar Programa de Reconhecimento entre Pares", "tema": "Reconhecimento e Valorização" },
  { "acao": "Implementar Programa de Bem-Estar e Saúde Mental", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Oferecer Sessões de Apoio Psicológico", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Implementar Onboarding com Mentoria Estruturada", "tema": "Engajamento e Motivação" },
  { "acao": "Revisar Metas para Torná-las SMART", "tema": "Liderança e Gestão" },
  { "acao": "Implementar Sessões de Escuta Ativa da Liderança", "tema": "Comunicação Interna" },
  { "acao": "Implementar PDIs para Colaboradores de Alto Potencial", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Acompanhar Execução dos Planos de Desenvolvimento Individual", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Implementar Benefício de Saúde Ampliado", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Benefício de Alimentação/Refeição", "tema": "Remuneração e Benefícios" },
  { "acao": "Revisar Benefício de Transporte", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Benefício Educacional", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Benefício de Flexibilidade", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Reuniões de 1:1 Estruturadas", "tema": "Liderança e Gestão" },
  { "acao": "Aprimorar Qualidade das Reuniões de 1:1", "tema": "Liderança e Gestão" },
  { "acao": "Revisar Atividades de Multi Função", "tema": "Carga de Trabalho" },
  { "acao": "Reconhecer Formalmente Atuação Multi Função", "tema": "Reconhecimento e Valorização" },
  { "acao": "Implementar Pesquisa de Satisfação Contínua", "tema": "Engajamento e Motivação" },
  { "acao": "Implementar Pesquisas de Pulso (Pulse Surveys)", "tema": "Engajamento e Motivação" },
  { "acao": "Implementar Pesquisa de Satisfação com Benefícios", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Pesquisa de Efetividade da Comunicação", "tema": "Comunicação Interna" },
  { "acao": "Implementar Novas Ferramentas Digitais", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Revisar Ferramentas Digitais Existentes", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Capacitar Colaboradores no Uso de Softwares", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Criar Comitês Temáticos por Área", "tema": "Autonomia e Tomada de Decisão" },
  { "acao": "Realizar Benchmarking com Outras Empresas", "tema": "Cultura e Valores Organizacionais" },
  { "acao": "Incentivar Liderança Feminina com Treinamentos", "tema": "Diversidade e Inclusão e Equidade" },
  { "acao": "Aprimorar Transparência em Processos de Decisão", "tema": "Liderança e Gestão" },
  { "acao": "Implementar Calendário de Ações de Reconhecimento", "tema": "Reconhecimento e Valorização" },
  { "acao": "Implementar Iniciativas de Sustentabilidade", "tema": "Cultura e Valores Organizacionais" },
  { "acao": "Oferecer Treinamento em Gestão de Projetos", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Implementar Pesquisa de Saída Estruturada", "tema": "Engajamento e Motivação" },
  { "acao": "Redesenhar Fluxos de Trabalho Ineficientes", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Implementar Campanhas de Saúde Preventiva", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Implementar Treinamento de Inclusão Digital", "tema": "Diversidade e Inclusão e Equidade" },
  { "acao": "Aprimorar Comunicação da Alta Gestão", "tema": "Comunicação Interna" },
  { "acao": "Implementar Programa de Intraempreendedorismo", "tema": "Autonomia e Tomada de Decisão" },
  { "acao": "Revisar Política de Metas e Objetivos", "tema": "Liderança e Gestão" },
  { "acao": "Implementar Treinamentos de Trabalho em Equipe", "tema": "Relacionamento com a equipe" },
  { "acao": "Revisar Jornada de Trabalho", "tema": "Carga de Trabalho" },
  { "acao": "Implementar Plataforma de Reconhecimento Online", "tema": "Reconhecimento e Valorização" },
  { "acao": "Criar Grupos de Afinidade", "tema": "Diversidade e Inclusão e Equidade" },
  { "acao": "Revisar Critérios de Promoção", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Implementar Programa de Voluntariado Corporativo", "tema": "Cultura e Valores Organizacionais" },
  { "acao": "Implementar Treinamento em Atendimento ao Cliente Interno", "tema": "Relacionamento entre equipes" },
  { "acao": "Revisar Canais de Denúncia", "tema": "Assédio" },
  { "acao": "Implementar Política de Prevenção ao Assédio", "tema": "Assédio" },
  { "acao": "Revisar Condutas de Abuso de Autoridade", "tema": "Abuso de autoridade" },
  { "acao": "Implementar Programa de Conscientização contra Preconceito", "tema": "Preconceito" },
  { "acao": "Aprimorar Cultura Organizacional com Workshops de Valores", "tema": "Cultura e Valores Organizacionais" },
  { "acao": "Implementar Rodadas de Escuta com a Liderança", "tema": "Sem tema" },
  { "acao": "Implementar Benefício de Auxílio Home Office", "tema": "Remuneração e Benefícios" },
  { "acao": "Revisar Estrutura Física dos Escritórios", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Implementar Ferramentas de Comunicação Colaborativa", "tema": "Comunicação Interna" },
  { "acao": "Aprimorar Treinamento de Liderança Situacional", "tema": "Liderança e Gestão" },
  { "acao": "Implementar Avaliações 360 Graus", "tema": "Liderança e Gestão" },
  { "acao": "Criar Programa de Reconhecimento Mensal", "tema": "Reconhecimento e Valorização" },
  { "acao": "Promover Semana de Bem-Estar e Saúde", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Oferecer Programas de Ginástica Laboral", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Implementar PDI para Novos Gestores", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Revisar Política de Bonificação Variável", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Pesquisas de Clima Trimestrais", "tema": "Engajamento e Motivação" },
  { "acao": "Criar Comitês de Inovação", "tema": "Autonomia e Tomada de Decisão" },
  { "acao": "Implementar Ferramentas de Automação de RH", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Aprimorar Treinamento de Comunicação Não Violenta", "tema": "Relacionamento com a equipe" },
  { "acao": "Criar Programas de Integração Interáreas", "tema": "Relacionamento entre equipes" },
  { "acao": "Implementar Campanhas de Diversidade", "tema": "Diversidade e Inclusão e Equidade" },
  { "acao": "Oferecer Treinamento sobre Assédio e Condutas Éticas", "tema": "Assédio" },
  { "acao": "Criar Programa de Prevenção de Burnout", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Revisar Critérios de Promoção para Gestores", "tema": "Liderança e Gestão" },
  { "acao": "Implementar Pesquisa de Engajamento de Equipes Remotas", "tema": "Engajamento e Motivação" },
  { "acao": "Aprimorar Política de Feedback Rápido", "tema": "Liderança e Gestão" },
  { "acao": "Criar Programa de Desenvolvimento Técnico Avançado", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Implementar Política de Zero Tolerância ao Preconceito", "tema": "Preconceito" },
  { "acao": "Criar Comitês de Cultura Organizacional", "tema": "Cultura e Valores Organizacionais" },
  { "acao": "Implementar Reconhecimento Público em Eventos", "tema": "Reconhecimento e Valorização" },
  { "acao": "Promover Oficinas de Liderança Colaborativa", "tema": "Liderança e Gestão" },
  { "acao": "Oferecer Benefício de Apoio Financeiro em Educação", "tema": "Remuneração e Benefícios" },
  { "acao": "Implementar Programa de Mobilidade Interna", "tema": "Desenvolvimento e Carreira" },
  { "acao": "Criar Espaços de Descanso no Escritório", "tema": "Ambiente e Bem-estar no Trabalho" },
  { "acao": "Revisar Políticas de Carga Horária Extra", "tema": "Carga de Trabalho" },
  { "acao": "Implementar Comunicação Direta via CEO Newsletter", "tema": "Comunicação Interna" },
  { "acao": "Criar Programa de Reconhecimento de Iniciativas Sustentáveis", "tema": "Cultura e Valores Organizacionais" },
  { "acao": "Implementar Ferramentas de BI para Gestão de Pessoas", "tema": "Recursos, Ferramentas e Estrutura" },
  { "acao": "Aprimorar Reuniões de Alinhamento Estratégico", "tema": "Comunicação Interna" },
  { "acao": "Implementar Programa de Treinamento Cruzado", "tema": "Relacionamento entre equipes" }
]
"""

    REVIEW_EXAMPLE = f"""
<div class="show_review">
<div><p>As ações propostas estão direcionadas em resolvr problemas relacionados à insatisfação são Liderança e Gestão, Carga de Trabalho e Cultura e Valores Organizacionais, com relatos recorrentes 
sobre distanciamento da liderança, pressão por resultados e desalinhamento cultural.</p>
</div>

<div class='review_item'>Plano de ação:</div>
<div>
<ul>
<li>Implementar Programa de Bem-Estar e Saúde Mental.</li>
<li>Aprimorar Cultura de Feedback.</li>
</ul>
</div>

</div>
"""

    # Ajuste aqui conforme for necessário para leitura das intenções (strings JSON, dict, etc.)
    objetivos = "Melhorar o ambiente de trabalho e fortalecer a cultura de colaboração entre equipes."
    restricoes = "Não contratar mais pessoas neste momento. Não alterar estruturas salariais."

    prompt = f"""
#Objetivo    
Você é um especialista em cultura, engajamento e clima organizacional.
Sua tarefa é elaborar um plano de ação para a área {area_name} com base nos dados da pesquisa de clima organizacional.

#Instruções
- Leia atentamente ao resumo da área. As ações sugeridas devem **responder diretamente às oportunidades de melhorias e às situações relatadas nele**
- Priorize sugerir soluções para os temas com maior volume de críticas e sugestões.
- Concentrar as ações em até 3 bulet points.
- Evite sugestões genéricas como “melhorar a comunicação” ou “valorizar os colaboradores”. Seja técnico e específico.
- Caso uma lista de restrições seja fornecida, **Não inclua ações consideradas inviáveis**.
- Caso uma lista de objetivos seja fornecida, ressalte ela no plano de ação e priorize as ações de maior impacto nos objetivos da organização.
- Evite repetir dados.

#Output
O output sem em html e ter o seguinte formato:

<div class="show_review">
<div><p><Análise do plano de ação proposto></p></div>
<div class="review_item">Plano de ação:</div> <div><ul><lista de ações></ul></div>
</div>

Não utilize cercas Markdown de início/fim no output. Ex: ```html ... ```

#Dados de entrada: 

Objetivos da área:
{objetivos}

Ações que devem ser evitadas:
{restricoes}

Inspire-se nessa lista de planos de ação:
{ACTION_PLANS}

Resumo da área:
{area_review}

Exemplo de saída:
{REVIEW_EXAMPLE}
"""
    return prompt.strip()

def _is_empty_intents_payload(payload) -> bool:
    """
    True quando não há conteúdo útil no area_intents.
    Aceita str (JSON) ou dict (jsonb já desserializado).
    Evita ser restritivo demais: se for dict não-vazio, consideramos OK.
    """
    if payload is None:
        return True

    # Se já veio como dict/list do jsonb → válido se não vazio
    if isinstance(payload, (dict, list)):
        return len(payload) == 0

    # Se veio como string
    if isinstance(payload, str):
        st = payload.strip()
        if st == "" or st == "{}" or st == "[]":
            return True
        # Tenta parsear JSON; se não der, ainda assim consideramos "não vazio"
        # para não bloquear a geração de resumo.
        try:
            obj = json.loads(st)
            if isinstance(obj, (dict, list)):
                return len(obj) == 0
        except Exception:
            return False  # string não vazia e não-json → deixa seguir
        return False

    # Outros tipos (int, float, etc.): se tem valor → considera não-vazio
    return False

def generate_and_save_area_reviews(
    survey_id: int,
    model: str = "gpt-4o",
    temperature: float = 0.0,
    overwrite: bool = False,
    max_chars: int = 15000,
    on_progress: Optional[Callable[[int, str, str], None]] = None,
    df_plan: Optional[pd.DataFrame] = None
) -> int:
    """
    Gera resumos por área (apenas com area_intents != null e area_id != 0).
    Se on_progress for fornecido, é chamado por área com status: 'ok' | 'indisponivel' | 'erro'.
    """
    client = get_openai_client()
    df_areas = df_plan if df_plan is not None else fetch_survey_areas_with_intents(survey_id)
    if df_areas.empty:
        return 0

    # --- Filtro: ignora área_id == 0
    df_areas = df_areas[df_areas["area_id"] != 0]

    if "area_review" in df_areas.columns and not overwrite:
        mask_empty = df_areas["area_review"].isna() | (df_areas["area_review"].astype(str).str.strip() == "")
        df_areas = df_areas[mask_empty]
    if df_areas.empty:
        return 0

    out_rows = []
    for _, row in df_areas.iterrows():
        area_id = int(row["area_id"])
        area_name = row["area_name"]
        intents_payload = row["area_intents"]

        try:
            if _is_empty_intents_payload(intents_payload):
                out_rows.append({"area_id": area_id, "area_review": "Resumo indisponível por falta de informações"})
                if on_progress: on_progress(area_id, area_name, "indisponivel")
                continue

            if isinstance(intents_payload, str) and max_chars and len(intents_payload) > max_chars:
                intents_payload = intents_payload[:max_chars]

            #GERA RESUMO
            prompt = _build_area_review_prompt(area_name, intents_payload)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
            )
            content = resp.choices[0].message.content.strip()
            
            match = re.search(r'<div id="show_review">.*?</div>\s*$', content, flags=re.DOTALL)
            if match:
                content = match.group(0)

            #REFINA RESUMO
            prompt_ajust = _build_ajust_area_review_prompt(area_name, intents_payload, content)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt_ajust}],
                temperature=temperature,
            )
            content_final = resp.choices[0].message.content.strip()            

            match = re.search(r'<div id="show_review">.*?</div>\s*$', content_final, flags=re.DOTALL)
            if match:
                content_final = match.group(0)

            out_rows.append({"area_id": area_id, "area_review": content_final})
            if on_progress: on_progress(area_id, area_name, "ok")

        except Exception as e:
            out_rows.append({"area_id": area_id, "area_review": f"[ERRO AO GERAR RESUMO: {e}]"})
            if on_progress: on_progress(area_id, area_name, "erro")

    df_reviews = pd.DataFrame(out_rows)
    return update_area_reviews_bulk(survey_id, df_reviews)

def _merge_dict_sum(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
    out = dict(a)
    for k, v in (b or {}).items():
        out[k] = int(out.get(k, 0)) + int(v or 0)
    return out

def _merge_temas_citados(dst: Dict[str, Dict[str, int]], src: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    out = {k: dict(v) for k, v in (dst or {}).items()}
    for tema, bloc in (src or {}).items():
        out[tema] = _merge_dict_sum(out.get(tema, {}), bloc)
    return out

def _merge_recortes(dst: Dict[str, Dict[str, List[str]]], src: Dict[str, Dict[str, List[str]]]) -> Dict[str, Dict[str, List[str]]]:
    out = {t: {i: list(lst) for i, lst in intents.items()} for t, intents in (dst or {}).items()}
    for tema, intents in (src or {}).items():
        if tema not in out:
            out[tema] = {"critica": [], "sugestao": [], "reconhecimento": [], "neutro": []}
        for intent in ("critica", "sugestao", "reconhecimento", "neutro"):
            out[tema][intent].extend(intents.get(intent, []))
    return out

def _parse_area_intents_json(s: str | dict | None) -> dict:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}

def ensure_general_parenting(survey_id: int, min_level: int = 0) -> None:
    """
    Regras:
    - Garante a existência da área Geral (id=0, parent=0).
    - Todas as áreas com area_parent NULL ou 0 passam a ser filhas da Geral.
    - Se, após isso, a Geral continuar sem filhas, pegamos as áreas do menor nível
      (isto é, o nível “topo” encontrado) e atribuímos parent=0 para elas.
    """
    # 1) garante 'Geral'
    ensure_general_area(survey_id)

    # 2) torna filhas da Geral todas as áreas com parent NULL ou 0 (exceto a própria 0)
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE area
                   SET area_parent = 0
                 WHERE area_survey_id = :sid
                   AND area_id <> 0
                   AND (area_parent IS NULL OR area_parent = 0)
            """),
            {"sid": survey_id}
        )

    # 3) checa se a Geral tem filhas agora
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT COUNT(*) 
                  FROM area
                 WHERE area_survey_id = :sid
                   AND area_id <> 0
                   AND area_parent = 0
            """),
            {"sid": survey_id}
        ).first()
    has_children = (row and int(row[0]) > 0)

    if has_children:
        return

    # 4) se ainda não há filhas, escolhe o menor nível da hierarquia e define essas áreas como filhas da Geral
    df_all = fetch_survey_areas(survey_id)  # area_id, area_parent, area_name, area_survey_id
    if df_all.empty:
        return

    # calcula nível por Python
    levels = _compute_area_levels(df_all)
    # ignora a Geral
    df_all2 = df_all[df_all["area_id"] != 0].copy()
    if df_all2.empty:
        return
    df_all2["level"] = df_all2["area_id"].map(levels).fillna(0).astype(int)
    min_found = int(df_all2["level"].min())
    top_ids = df_all2.loc[df_all2["level"] == min_found, "area_id"].astype(int).tolist()
    if not top_ids:
        return

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE area
                   SET area_parent = 0
                 WHERE area_survey_id = :sid
                   AND area_id = ANY(:ids)
            """),
            {"sid": survey_id, "ids": top_ids}
        )

def compute_general_area_metrics_and_json(
    survey_id: int,
    min_level: int = 0
) -> dict:
    """
    Etapas:
      - garante área 'Geral' (area_id=0)
      - encontra áreas do nível `min_level` (ex.: 0), exclui a própria 0
      - se houver apenas 1 área no topo, copia suas métricas;
        senão, agrega todas as do topo.
      - monta JSON da área geral (temas_citados, recortes, resumos_areas)
      - persiste métricas + JSON em area_id=0
    Retorna o JSON (dict) da área geral.
    """
    # --- GARANTE parenting antes de calcular o geral
    ensure_general_parenting(survey_id, min_level=min_level)

    # descobrir TOP como filhas diretas da Geral (area_parent = 0), exceto a própria Geral
    df_meta = fetch_survey_areas(survey_id)
    if df_meta.empty:
        payload = pd.DataFrame([{
            "area_id": 0,
            "area_employee_number": 0,
            "area_comments_number": 0,
            "area_criticism_number": 0,
            "area_suggestions_number": 0,
            "area_recognition_number": 0,
            "area_response_rate": 0.0,
            "area_intents": json.dumps({}, ensure_ascii=False),
            "area_score": 0.0,
        }])
        update_area_metrics_bulk(survey_id, payload)
        return {}

    top_ids = df_meta[
        (df_meta["area_id"] != 0) & (df_meta["area_parent"].fillna(0) == 0)
    ]["area_id"].astype(int).tolist()

    df_full = fetch_survey_areas_with_intents(survey_id)
    df_top  = df_full[df_full["area_id"].isin(top_ids)].copy()

    # sem áreas topo → zera
    if df_top.empty:
        payload = pd.DataFrame([{
            "area_id": 0,
            "area_employee_number": 0,
            "area_comments_number": 0,
            "area_criticism_number": 0,
            "area_suggestions_number": 0,
            "area_recognition_number": 0,
            "area_response_rate": 0.0,
            "area_intents": json.dumps({}, ensure_ascii=False),
            "area_score": 0.0,
        }])
        update_area_metrics_bulk(survey_id, payload)
        return {}

    # caso só 1 área no topo → copia
    if len(df_top) == 1:
        r = df_top.iloc[0]
        intents = _parse_area_intents_json(r.get("area_intents"))
        # adiciona resumos_areas (apenas filhas diretas de Geral, i.e., topo)
        resumos = {}
        if str(r.get("area_review") or "").strip():
            resumos[str(r["area_name"])] = str(r["area_review"]).strip()
        if intents:
            intents["resumos_areas"] = resumos
        else:
            intents = {"metricas": {}, "temas_citados": {}, "recortes": {}, "resumos_areas": resumos}

        payload = pd.DataFrame([{
            "area_id": 0,
            "area_employee_number": int(r.get("area_employee_number") or 0),
            "area_comments_number": int(r.get("area_comments_number") or 0),
            "area_criticism_number": int(r.get("area_criticism_number") or 0),
            "area_suggestions_number": int(r.get("area_suggestions_number") or 0),
            "area_recognition_number": int(r.get("area_recognition_number") or 0),
            "area_response_rate": float(r.get("area_response_rate") or 0.0),
            "area_intents": json.dumps(intents, ensure_ascii=False),
            "area_score": float(r.get("area_score") or 0.0),
        }])
        update_area_metrics_bulk(survey_id, payload)
        return intents

    # agregado de várias áreas topo
    total_emp = total_comments = total_crit = total_sug = total_rec = 0
    sum_score = 0.0
    total_respondentes = 0

    temas_citados_aggr: Dict[str, Dict[str, int]] = {}
    recortes_aggr: Dict[str, Dict[str, List[str]]] = {}
    resumos_areas: Dict[str, str] = {}

    for _, r in df_top.iterrows():
        total_emp      += int(r.get("area_employee_number") or 0)
        total_comments += int(r.get("area_comments_number") or 0)
        total_crit     += int(r.get("area_criticism_number") or 0)
        total_sug      += int(r.get("area_suggestions_number") or 0)
        total_rec      += int(r.get("area_recognition_number") or 0)
        sum_score      += float(r.get("area_score") or 0.0)

        intents = _parse_area_intents_json(r.get("area_intents"))
        met = intents.get("metricas", {})
        total_respondentes += int(met.get("respondentes") or 0)

        temas_citados_aggr = _merge_temas_citados(temas_citados_aggr, intents.get("temas_citados") or {})
        recortes_aggr      = _merge_recortes(recortes_aggr, intents.get("recortes") or {})

        rev = str(r.get("area_review") or "").strip()
        if rev:
            resumos_areas[str(r["area_name"])] = rev

    response_rate = (float(total_respondentes) / float(total_emp)) if total_emp > 0 else 0.0

    geral_json = {
        "metricas": {
            "funcionarios": int(total_emp),
            "respondentes": int(total_respondentes),
            "adesao": round(response_rate * 100.0, 2),
            "criticas": int(total_crit),
            "sugestoes": int(total_sug),
            "reconhecimentos": int(total_rec),
            "neutros": 0,
            "peso_area": 1.0,
            "score_area": round(sum_score, 6),
        },
        "temas_citados": temas_citados_aggr,
        "recortes": recortes_aggr,
        "resumos_areas": resumos_areas
    }

    payload = pd.DataFrame([{
        "area_id": 0,
        "area_employee_number": int(total_emp),
        "area_comments_number": int(total_comments),
        "area_criticism_number": int(total_crit),
        "area_suggestions_number": int(total_sug),
        "area_recognition_number": int(total_rec),
        "area_response_rate": float(response_rate),
        "area_intents": json.dumps(geral_json, ensure_ascii=False),
        "area_score": float(sum_score),
    }])
    update_area_metrics_bulk(survey_id, payload)
    return geral_json

def generate_and_save_area_plans(survey_id: int, model: str, temperature: float, overwrite: bool, on_progress=None):
    """
    Gera e salva planos de ação por área.
    """
    from service.areas_repository import save_area_plan
    
    df_plan = fetch_survey_areas_with_intents(survey_id)
    client = get_openai_client()

    df_plan = df_plan[
        (df_plan["area_id"] != 0) &
        (df_plan["area_intents"].notna()) &
        (df_plan["area_intents"].astype(str).str.strip() != "")
    ]

    if not overwrite and "area_plan" in df_plan.columns:
        mask_empty = df_plan["area_plan"].isna() | (df_plan["area_plan"].astype(str).str.strip() == "")
        df_plan = df_plan[mask_empty]
    
    updated = 0

    for _, row in df_plan.iterrows():
        area_id = row["area_id"]
        area_name = row["area_name"]
        json_area = row["area_intents"]
        area_review = row.get("area_review", "")
        
        try:
            prompt = _build_area_plan_prompt(
                area_name=area_name,
                json_area=json_area,
                area_review=area_review
            )

            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature
            )
            
            choices = resp.choices
            if not choices or not choices[0].message or not choices[0].message.content:
                raise ValueError("Resposta inválida da API OpenAI")
            
            plan_text = choices[0].message.content.strip()
            match = re.search(r'<div id="show_review">.*?</div>\s*$', plan_text, flags=re.DOTALL)
            if match:
                plan_text = match.group(0)

            save_area_plan(area_id, plan_text)

            updated += 1
            if on_progress:
                on_progress(area_id, area_name, "ok")

        except Exception as e:
            if on_progress:
                on_progress(area_id, area_name, "erro", str(e))

    return updated

# ============================================================
# Análises
# ============================================================

def get_themes_intents(area_id: int,survey_id: int) -> pd.DataFrame:
    """
    Retorna a quantidade de percepções (tema x intenção) para os funcionários de uma determinada área.
    Args:
        area_id (int): ID da área selecionada.
    Returns:
        pd.DataFrame: DataFrame com colunas ['tema', 'intencao', 'qtd']
    """
    if not area_id:
        return pd.DataFrame(columns=["tema", "intencao", "qtd"])

    query = text("""
        SELECT 
            perception_theme AS tema,
            perception_intension AS intencao,
            COUNT(*) AS qtd
        FROM perception
        WHERE perception_survey_id = :survey_id AND perception_area_id = :area_id
        GROUP BY perception_theme, perception_intension
        ORDER BY perception_theme, perception_intension
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"area_id": area_id, "survey_id": survey_id})

    return df

# ============================================================
# FUNÇÕES V2
# ============================================================

def closed_question_classification (pergunta, temas):
    
    client = get_openai_client()
    model: str = "gpt-4o"
    temperature: float = 0.0

    prompt_user = f"""
Você é um especialista em pesquisa de cilma organizacional e precisa classificar essa questão em um dos temas abaixo:

Questão:
"{pergunta}"

Temas disponíveis:
{temas}

Output: somente o nome do tema
"""
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt_user}],
        )
        tema = resp.choices[0].message.content.strip()
        return tema
    except Exception as e:
        print(f"Erro ao classificar pergunta: {pergunta}\n{e}")
        return "Erro na classificação"

def comment_score_calc(survey_id, df_areas):
    df_perception = get_area_perceptions(survey_id)

    perception_weights = {
        "Neutro": 0,
        "Sugestão": -1,
        "Crítica": -2,
        "Reconhecimento": 2
    }

    #aplica o peso de cada percepção no data frame
    df_perception["score"] = df_perception["perception_intension"].map(perception_weights).fillna(0)

    ### 1 ### SOMENTE DIRETOS
    # cria uma nova df com tema por área e a coluna score já calculada (média das percepções)
    df_grouped = (
        df_perception
        .groupby(["perception_area_id", "perception_theme"])["score"]
        .mean()
        .reset_index()
    )

    # cria o dicionáio
    direct_scores = (
        df_grouped
        .pivot(index="perception_area_id", columns="perception_theme", values="score")
        .to_dict(orient="index")
    )

    # garante o formato correto dos dados
    direct_scores = {
        int(area_id): theme_scores
        for area_id, theme_scores in direct_scores.items()
    }

    ### 2 ### TODA A HIERARQUIA DA ÁREA
    children_map = {}
    for _, row in df_areas.iterrows():
        parent = row["area_parent"]
        child = row["area_id"]
        if parent is not None:
            children_map.setdefault(parent, []).append(child)

    # Mapeamento final com score total
    final_scores = {}

    for _, area_row in df_areas.iterrows():
        aid = int(area_row["area_id"])

        # Lista de todas as áreas na cadeia da atual
        descendants = _descendants(aid, children_map)

        # Filtra percepções de toda a cadeia
        df_cadeia = df_perception[df_perception["perception_area_id"].isin(descendants)]

        # Agrupa e calcula média dos scores por tema
        df_grouped_total = (
            df_cadeia
            .groupby(["perception_area_id", "perception_theme"])["score"]
            .mean()
            .to_dict()
        )

        # Monta estrutura final: direto + total
        final_scores[aid] = {
            "direto": direct_scores.get(aid, {}),
            "total": df_grouped_total
        }

    ### 3 ### NORMALIZAÇÃO (0 a 100)
    min_val, max_val = -2, 2
    for aid, area_scores in final_scores.items():
        for level in ["direto", "total"]:
            normalized = {}
            for theme, score in area_scores.get(level, {}).items():
                normalized[theme] = ((score - min_val) / (max_val - min_val)) * 100
            area_scores[level] = normalized

    return final_scores

def get_theme_ranking (survey_id):
    df_theme_ranking = get_themes_score(survey_id)
    ranking_total = calculate_theme_ranking(df_theme_ranking)
    return ranking_total

def calculate_theme_ranking(df_theme_ranking):
    df = df_theme_ranking.copy()

    # Cálculo das notas
    df["nota_geral"] = (
        df["score"] 
        + df["dissatisfied_score"] 
        + df["comment_score"] * 1.3
    )

    df["nota_direta"] = (
        df["direct_score"] 
        + df["direct_dissatisfied_score"] 
        + df["direct_comment_score"] * 1.3
    )
    return df

def calculate_theme_average(df_theme_ranking, survey_id):
    df = df_theme_ranking.copy()

    # Agrupa por tema e calcula médias
    df_avg = (
        df.groupby("theme_name")
        .agg(
            nota_geral=("nota_geral", "mean"),
            nota_direta=("nota_direta", "mean")
        )
        .reset_index()
    )

    # Adiciona survey_id
    df_avg["survey_id"] = survey_id

    return df_avg
