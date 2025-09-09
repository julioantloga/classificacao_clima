# service/perception_service.py
from typing import Dict, List, Tuple
import re

from service.openai_client import get_openai_client
from service.perception_repository import (
    fetch_employee_comments_grouped,
    insert_perceptions,
    delete_perceptions_for_survey,
)

# -------- Prompt builders --------

def _build_system_prompt():
    prompt_system = f"""
Vou te dar um exemplo de resposta para você seguir o padrão.

Considerando a pergunta: O meu espaço de trabalho no home office é adequado para o trabalho que estou fazendo
e o comentário: O computador oferecido pela empresa não é bom e acabo tendo que usar o meu pessoal, além disso poderiam nos oferecer um auxilio homeoffice melhor
O output foi esse: Recursos, Ferramentas e Estrutura - Crítica - O computador oferecido pela empresa não é bom e acabo tendo que usar o meu pessoal.| Recursos, Ferramentas e Estrutura - Sugestão - poderiam nos oferecer um auxilio homeoffice melhor.

Considerando a pergunta: Justifique sua resposta
e o comentário: Me sinto valorizado e vejo futuro aqui
O output foi esse: Reconhecimento e Valorização - Reconhecimento - Me sinto valorizado.| Desenvolvimento e Carreira - Reconhecimento - vejo futuro aqui.

Considerando a pergunta: O meu espaço de trabalho no home office é adequado para o trabalho que estou fazendo
e o comentário: Gosto do meu líder
O output foi esse: Sem tema - Neutro - Gosto do meu líder.

Considerando a pergunta: O quanto você recomendaria a sua liderança de equipe como uma boa liderança para se trabalhar?
e o comentário: 'Certas situações trouxeram um clima pesado e sobrecarregado ao time. Momentos difíceis existem, mas trazer as coisas de uma forma leve, com certeza teriam um melhor efeito. Tem ações muito positivas, mas certos comportamentos precisam ser revistos. Existem atividades no time que precisam ser guiadas por uma liderança.'
O output foi esse: Liderança e Gestão - Sugestão - 'Momentos difíceis existem, mas trazer as coisas de uma forma leve, com certeza teriam um melhor efeito. Existem atividades no time que precisam ser guiadas por uma liderança' .| Ambiente e Bem-estar no Trabalho - Crítica - 'Certas situações trouxeram um clima pesado e sobrecarregado ao time'.| Liderança e Gestão - Crítica - 'Tem ações muito positivas, mas certos comportamentos precisam ser revistos'
    """
    return prompt_system

def _build_user_prompt(comments: List[dict], temas: List[str]) -> str:
    # Cria o bloco "question_list"
    lines = []
    for item in comments:
        q = str(item["question"]).strip()
        c = str(item["comment"]).strip()
        lines.append(f"pergunta: {q}\ncomentario: {c}\n")
    question_list = "\n".join(lines)

    temas_txt = ", ".join(temas) if temas else "Sem tema"
    prompt_system = f"""
Você é um analista sênior de clima organizacional, especializado em interpretar dados qualitativos de pesquisas de engajamento.
Classifique os comentários das perguntas ou afirmações abaixo por tema e intenção, utilizando a seguinte lista de temas:
{temas_txt},

e a seguinte lista de intenções: Reconhecimento, Crítica, Sugestão e Neutro.

Seu objetivo é identificar um ou mais temas e intenções relacionados aos comentários e trazer o recorte do comentário referente à classificação:

{question_list}

Instruções:
- Não classifique um comentário com o mesmo tema e intenção mais de uma vez, mesmo que haja comentários parecidos em perguntas diferentes.

Responda estritamente no formato:

Pergunta1: <pergunta>
Comentário1: <comentario>
Tema1: <tema> - <intenção> - <recorte>.| <tema> - <intenção> - <recorte>,...

Pergunta2: <pergunta>
Comentário2: <comentario>
Tema2: <tema> - <intenção> - <recorte>.| <tema> - <intenção> - <recorte>,...
"""
    return prompt_system

# -------- Parser do output --------
_BLOCK_RE = re.compile(
    r"Pergunta\d+:\s*(?P<pergunta>.+?)\s*[\r\n]+Coment[aá]rio\d+:\s*(?P<comentario>.+?)\s*[\r\n]+Tema\d+:\s*(?P<temas>.+?)(?:[\r\n]{2,}|\Z)",
    re.DOTALL | re.IGNORECASE
)

def _parse_model_output(raw: str) -> List[dict]:
    """
    Retorna lista de blocos:
    [{ 'pergunta': str, 'comentario': str, 'pairs': [ (tema, intenção, recorte), ... ] }, ...]
    """
    results = []
    for m in _BLOCK_RE.finditer(raw.strip()):
        pergunta = m.group("pergunta").strip()
        comentario = m.group("comentario").strip()
        temas_line = m.group("temas").strip()

        # Split por "|" em percepções múltiplas
        pairs = []
        for chunk in temas_line.split("|"):
            part = chunk.strip().rstrip(".")
            if not part:
                continue
            # esperado: "<tema> - <intenção> - <recorte>"
            bits = [b.strip(" '\"") for b in part.split(" - ")]
            if len(bits) >= 3:
                tema, intencao, recorte = bits[0], bits[1], " - ".join(bits[2:])
                pairs.append((tema, intencao, recorte))
        results.append({"pergunta": pergunta, "comentario": comentario, "pairs": pairs})
    return results

# -------- Resolve comment_id pelo texto (pergunta+comentário) --------

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()

def _resolve_comment_id(block: dict, items: List[dict]) -> int | None:
    """
    Tenta achar o comment_id do bloco comparando pergunta e comentário.
    Estratégia: match exato normalizado; se não achar, tenta 'comentário' como substring.
    """
    p = _normalize(block["pergunta"])
    c = _normalize(block["comentario"])

    # 1) match exato
    for it in items:
        if _normalize(it["question"]) == p and _normalize(it["comment"]) == c:
            return it["comment_id"]

    # 2) substring do comentário (fallback leve)
    for it in items:
        if _normalize(it["question"]) == p and c and c in _normalize(it["comment"]):
            return it["comment_id"]

    return None

# -------- Execução: por survey, por employee --------

def classify_and_save_perceptions(
    survey_id: int,
    temas: List[str],
    model: str = "gpt-4o-mini",
    temperature: float = 0.0,
    clear_existing: bool = False
) -> Dict[str, int]:
    """
    Para o survey informado:
      - Agrupa comentários por employee (email)
      - Para cada employee, envia 1 prompt com todas as (pergunta, comentário)
      - Faz parsing do output
      - Resolve comment_id e insere percepções em batch
    Retorna stats.
    """
    if clear_existing:
        delete_perceptions_for_survey(survey_id)

    grouped = fetch_employee_comments_grouped(survey_id)
    if not grouped:
        return {"employees": 0, "perceptions": 0, "blocks_unmatched": 0, "employees_skipped": 0}

    client = get_openai_client()
    total_perc = 0
    unmatched = 0
    skipped = 0

    completion_tokens_list = []  

    for email, items in grouped.items():
        if not items:
            skipped += 1
            continue

        prompt_user = _build_user_prompt(items, temas)
        prompt_system = _build_system_prompt()

        resp = client.chat.completions.create(
            model=model,
            temperature=temperature,
            messages=[
                {"role": "system", "content": prompt_system},
                {"role": "user", "content": prompt_user},
            ],
            max_tokens=1000
        )
        content = resp.choices[0].message.content
        
        completion_tokens_list.append(resp.usage.completion_tokens)

        blocks = _parse_model_output(content)
        payload = []
        for blk in blocks:
            cid = _resolve_comment_id(blk, items)
            if not cid:
                unmatched += 1
                continue
            for tema, intencao, recorte in blk["pairs"]:
                payload.append({
                    "perception_comment_id": cid,
                    "perception_comment_clipping": recorte[:1000] if recorte else None,  # proteção básica
                    "perception_theme": tema[:255] if tema else None,
                    "perception_intension": intencao[:100] if intencao else None,
                    "perception_survey_id": survey_id,
                    "perception_area_id": next((i["area_id"] for i in items if i["comment_id"] == cid), None)
                })

        total_perc += insert_perceptions(payload)

    return {
        "employees": len(grouped),
        "perceptions": total_perc,
        "blocks_unmatched": unmatched,
        "employees_skipped": skipped,
        "completion_tokens": completion_tokens_list
    }
