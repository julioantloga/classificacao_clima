from flask import Flask, Response, request, render_template, send_from_directory, jsonify
from sqlalchemy import text
import pandas as pd
import os
from db_config import engine
import tempfile, uuid, threading, time
from service.progress import progress_bus, timed_step

from service.classification_service import (
    data_preprocessing,
    persist_questions_and_comments
)

from service.person_service import person_preprocessing

from service.survey_repository import (
    insert_survey,
    get_survey,
    get_comments_with_perceptions,
    list_areas_with_non_null_score,
    list_perception_themes_for_survey,
    get_area_review_plan
)

from service.areas_repository import (
    insert_areas,
    fetch_survey_areas_with_intents
)

from service.person_repository import (
    insert_person
)

from service.perception_service import classify_and_save_perceptions

from service.areas_service import (
    create_organizational_chart,
    compute_and_update_area_metrics_python,
    generate_and_save_area_reviews,
    generate_and_save_area_plans,
    get_themes_intents
)

from service.general_review import (
    generate_and_save_general_review,
    generate_and_save_general_plan,
    get_ranking_area,
    get_ranking_general_themes
)




app = Flask(__name__, static_folder="static", template_folder="templates")

def read_csv_flex(src):
    """
    Leitura robusta:
      - Flask FileStorage (tem .stream)
      - file-like (tem .read / .seek)
      - caminho (str/Path)
    Tenta UTF-8 e cai para Latin-1. Detecta separador automaticamente (engine='python').
    """
    import io
    import pandas as pd
    from pathlib import Path

    def _read_buffer(buf):
        buf.seek(0)
        try:
            return pd.read_csv(buf, sep=None, engine="python", encoding="utf-8", on_bad_lines="skip")
        except UnicodeDecodeError:
            buf.seek(0)
            return pd.read_csv(buf, sep=None, engine="python", encoding="latin-1", on_bad_lines="skip")

    # Caso 1: FileStorage (Flask)
    if hasattr(src, "stream"):
        return _read_buffer(src.stream)

    # Caso 2: caminho para arquivo
    if isinstance(src, (str, Path)):
        p = Path(src)
        with p.open("rb") as f:
            # lê bytes e cria BytesIO para poder tentar encodings
            data = f.read()
        return _read_buffer(io.BytesIO(data))

    # Caso 3: file-like
    if hasattr(src, "read"):
        # garante seek
        try:
            src.seek(0)
        except Exception:
            src = io.BytesIO(src.read())
        return _read_buffer(src)

    raise TypeError("read_csv_flex: tipo de origem não suportado")

@app.route("/")
def home():
    return send_from_directory("static", "index.html")

@app.route('/api/surveys')
def api_surveys():
    from service.survey_repository import list_surveys
    surveys = list_surveys()
    return jsonify(surveys)


@app.route('/dashboard/<page>')
def dashboard_page(page):

    survey_id = request.args.get("survey_id", type=int)
    survey = get_survey(survey_id)

    if page == "overview":
        data = get_area_review_plan(0, survey_id)
        area_review = data ["area_review"]
        area_review = area_review.replace('\n', '')
        area_review = area_review.replace('<br>', '')
        ranking_areas = get_ranking_area(survey_id).to_dict(orient="records")
        ranking_temas = get_ranking_general_themes(survey_id).to_dict(orient="records")

        return render_template("overview.html",survey=survey, data=data, area_review=area_review, ranking_areas=ranking_areas, ranking_temas=ranking_temas)
    
    if page == "area":        
        areas = list_areas_with_non_null_score(survey_id)
        return render_template("area.html", survey=survey, areas=areas)
    
    if page == "comments":
        rows = get_comments_with_perceptions(survey_id)
        areas = list_areas_with_non_null_score(survey_id)
        themes = list_perception_themes_for_survey(survey_id)

        return render_template("comments.html", survey=survey, rows=rows, areas=areas, themes=themes)

@app.route('/dashboard/areas/search', methods=['GET'])
def dashboard_areas_search():
    survey_id = request.args.get("survey_id", type=int)
    area_raw = request.args.get("area", default="")
    selected_area = int(area_raw) if area_raw not in (None, "",) else 0  # vazio => 0

    survey = get_survey(survey_id)
    areas = list_areas_with_non_null_score(survey_id)
    data = get_area_review_plan(area_id=selected_area, survey_id=survey_id)  # << chama sua função

    # Gráfico de temas x intenções
    themes_intents_df = get_themes_intents(area_id=selected_area, survey_id=survey_id) if selected_area else None
    themes_intents_data = themes_intents_df.to_dict(orient='records') if themes_intents_df is not None else []
    print(themes_intents_data)
    return render_template("area.html",
                           survey=survey,
                           areas=areas,
                           data=data,
                           selected_area=selected_area,
                           themes_intents_data=themes_intents_data)

@app.route("/classifica_comentarios", methods=["POST"])
def classifica_comentarios_route():
    if "campanha" not in request.files or "pessoas" not in request.files:
        return jsonify({"error": "Arquivos 'campanha' e 'pessoas' são obrigatórios."}), 400

    campanha_file = request.files["campanha"]
    pessoas_file  = request.files["pessoas"]
    instancia_areas_file  = request.files["instancia_areas"]
    hierarquia_areas_file  = request.files["hierarquia_areas"]
    person_areas_file  = request.files["person_areas"]
    df_notas_areas_file  = request.files["nota_areas"]

    try:
        # Ler CSVs
        df_campanha = read_csv_flex(campanha_file)
        df_person = read_csv_flex(pessoas_file)
        df_instancia_areas = read_csv_flex(instancia_areas_file)
        df_hierarquia_areas = read_csv_flex(hierarquia_areas_file)
        df_person_areas = read_csv_flex(person_areas_file)
        df_notas_areas = read_csv_flex(df_notas_areas_file)

        # Campos da seção "Configuração"
        config = {
            "nome_pesquisa": request.form.get("nome_pesquisa"),            
            "org_top": request.form.get("org_top", type=int),
            "org_bottom": request.form.get("org_bottom", type=int),
        }

        survey_id = insert_survey(config.get("nome_pesquisa"))

        # Limpa e organiza os dados da campanha antes de rodarmos as classificações
        df_processed = data_preprocessing(df_campanha, df_person, survey_id)

        # Salva as áreas e hierarquia na base de dados 
        df_areas = create_organizational_chart(df_instancia_areas, df_hierarquia_areas, survey_id)
        areas_update = insert_areas(df_areas)
        
        # Salva os participantes com área_id
        df_employee = person_preprocessing(df_person,df_person_areas,survey_id)
        employee_update = insert_person(df_employee)

        # Salva as questões e os comentários na base de dados
        df_comments = data_preprocessing(df_campanha, df_person, survey_id)
        persist_stats = persist_questions_and_comments(df_comments)
        
        temas = [
            "Sem tema",
            "Liderança e Gestão",
            "Comunicação Interna",
            "Reconhecimento e Valorização",
            "Desenvolvimento e Carreira",
            "Cultura e Valores Organizacionais",
            "Relacionamento com a equipe",
            "Relacionamento entre equipes",
            "Ambiente e Bem-estar no Trabalho",
            "Carga de Trabalho",
            "Remuneração e Benefícios",
            "Diversidade e Inclusão e Equidade",
            "Recursos, Ferramentas e Estrutura",
            "Engajamento e Motivação",
            "Autonomia e Tomada de Decisão",
            "Assédio",
            "Abuso de autoridade",
            "Preconceito"
        ]

        stats = classify_and_save_perceptions(
            survey_id=survey_id,
            temas=temas,
            model="gpt-4o",          # ou "gpt-4o-mini" para custo menor
            temperature=0.0,
            clear_existing=False     # True se quiser limpar percepções do survey antes
        )

        # Monta tabela HTML (limitando para não pesar a página)
        shown = 200
        display_df = df_comments.head(shown).copy()
        table_html = display_df.to_html(
            classes="striped highlight responisve-table",
            index=False,
            border=0
        )

        return render_template(
            "resultados.html",
            message="Criação dos funcionários concluída com sucesso!",
            meta=config,
            table_html=table_html,
            row_count=len(df_employee),
            shown_rows=len(display_df),
        ), 200

    except ValueError as ve:
        return render_template(
            "resultados.html",
            message=f"Erro de validação: {ve}",
            meta={},
            table_html="",
            row_count=0,
            shown_rows=0,
        ), 400

    except Exception as e:
        return render_template(
            "resultados.html",
            message=f"Erro ao processar: {e}",
            meta={},
            table_html="",
            row_count=0,
            shown_rows=0,
        ), 500

@app.post("/classifica_comentarios_async")
def classifica_comentarios_async():
    """
    Inicia processamento em background e retorna job_id.
    Frontend deve abrir SSE em /events/<job_id>.
    """
    required = ["campanha", "pessoas", "instancia_areas", "hierarquia_areas", "person_areas", "nota_areas"]
    for r in required:
        if r not in request.files:
            return jsonify({"error": f"Arquivo '{r}' é obrigatório."}), 400

    # Config
    config = {
        "nome_pesquisa": request.form.get("nome_pesquisa"),
        "org_top": request.form.get("org_top", type=int),
        "org_bottom": request.form.get("org_bottom", type=int),
    }

    # Salva arquivos temporários
    tmpdir = tempfile.mkdtemp(prefix="clima_")
    paths = {}
    for key in required:
        f = request.files[key]
        path = os.path.join(tmpdir, f"{key}.csv")
        f.save(path)
        paths[key] = path

    job_id = str(uuid.uuid4())
    progress_bus.open(job_id)

    # dispara o worker
    th = threading.Thread(target=_worker_pipeline, args=(job_id, paths, config), daemon=True)
    th.start()

    return jsonify({"job_id": job_id})

@app.get("/events/<job_id>")
def sse_events(job_id: str):
    def generate():
        for chunk in progress_bus.stream(job_id):
            yield chunk
    return Response(generate(), mimetype="text/event-stream")

def _worker_pipeline(job_id: str, paths: dict, config: dict):
    """
    Executa o pipeline completo e publica progresso por SSE.
    """
    import traceback
    try:

        progress_bus.put(job_id, {"event": "info", "message": "Iniciando processamento..."})

        min_lvl = int(config.get("org_top") or 0)
        max_lvl = int(config.get("org_bottom") or 999)

        # 1) Survey
        def _survey():
            name = config.get("nome_pesquisa") or f"Pesquisa {time.strftime('%Y-%m-%d %H:%M')}"
            return insert_survey(name)
        survey_id = timed_step(job_id, "Criar pesquisa (survey)", _survey)
        # divulga o survey_id para o front poder montar o link de resultado
        progress_bus.put(job_id, {"event": "survey", "survey_id": survey_id})

        # 2) Leitura CSVs
        def _read_csvs():
            df_campanha        = read_csv_flex(paths["campanha"])
            df_person          = read_csv_flex(paths["pessoas"])
            df_instancia_areas = read_csv_flex(paths["instancia_areas"])
            df_hierarquia_areas= read_csv_flex(paths["hierarquia_areas"])
            df_person_areas    = read_csv_flex(paths["person_areas"])
            df_notas_areas    = read_csv_flex(paths["nota_areas"])

            return df_campanha, df_person, df_instancia_areas, df_hierarquia_areas, df_person_areas, df_notas_areas
        (df_campanha, df_person, df_instancia_areas, df_hierarquia_areas, df_person_areas, df_notas_areas) = \
            timed_step(job_id, "Ler arquivos (CSV)", _read_csvs)

        # 3) Pré-processar campanha → df_final
        df_processed = timed_step(job_id, "Normalizar respostas (df_final)", data_preprocessing,df_campanha, df_person, survey_id)

        # 4) Áreas (instância + hierarquia)
        def _areas():
            df_areas = create_organizational_chart(df_instancia_areas, df_hierarquia_areas, survey_id)
            return insert_areas(df_areas)
        areas_count = timed_step(job_id, "Inserir áreas/hierarquia", _areas)

        # 5) Pessoas (alocação ativa)
        def _people():
            df_employee = person_preprocessing(df_person, df_person_areas, survey_id)
            return insert_person(df_employee)
        employees_count = timed_step(job_id, "Inserir funcionários", _people)

        # 6) Perguntas + Comentários
        q_c_stats = timed_step(job_id, "Inserir questões e comentários", persist_questions_and_comments, df_processed)
        progress_bus.put(job_id, {"event": "stats", "scope": "questions_comments", "data": q_c_stats})

        # 7) Classificar Percepções
        temas = [
            "Sem tema",
            "Liderança e Gestão",
            "Comunicação Interna",
            "Reconhecimento e Valorização",
            "Desenvolvimento e Carreira",
            "Cultura e Valores Organizacionais",
            "Relacionamento com a equipe",
            "Relacionamento entre equipes",
            "Ambiente e Bem-estar no Trabalho",
            "Carga de Trabalho",
            "Remuneração e Benefícios",
            "Diversidade e Inclusão e Equidade",
            "Recursos, Ferramentas e Estrutura",
            "Engajamento e Motivação",
            "Autonomia e Tomada de Decisão",
            "Assédio",
            "Abuso de autoridade",
            "Preconceito",
        ]
        perc_stats = timed_step(job_id, "Classificar percepções", classify_and_save_perceptions, survey_id,temas, "gpt-4o", 0.0, False)
        progress_bus.put(job_id, {"event": "stats", "scope": "perceptions", "data": perc_stats})

        # 8) Calcular scores de áreas (Python)
        min_commenters = 1
        areas_upd = timed_step(job_id, "Calcular o scores de áreas",compute_and_update_area_metrics_python,survey_id, df_notas_areas, min_lvl, max_lvl, min_commenters)
        progress_bus.put(job_id, {"event": "stats", "scope": "area_metrics_py", "data": {"areas_atualizadas": areas_upd}})

        progress_bus.put(job_id, {"event": "info", "message": "Pipeline concluído com sucesso."})
    
    except Exception as e:
        traceback.print_exc()
        progress_bus.put(job_id, {"event": "error", "message": str(e)})
    
    finally:
        progress_bus.put(job_id, {"event": "done"})
        progress_bus.close(job_id)

# Verifica se survey existe
@app.get("/api/survey_exists")
def api_survey_exists():
    sid = request.args.get("survey_id", type=int)
    if not sid:
        return jsonify({"exists": False}), 200
    with engine.begin() as conn:
        row = conn.execute(text("SELECT 1 FROM survey WHERE survey_id = :sid"), {"sid": sid}).first()
    return jsonify({"exists": bool(row)}), 200

@app.get("/api/survey_exists/<int:sid>")
def api_survey_exists_path(sid: int):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT 1 FROM survey WHERE survey_id = :sid"), {"sid": sid}).first()
    return jsonify({"exists": bool(row)}), 200

# Dispara geração dos resumos de áreas (sincrono e simples)
@app.post("/generate_area_reviews")
def generate_area_reviews_route():
    try:
        data = request.get_json(silent=True) or {}
        survey_id = int(data.get("survey_id"))
        overwrite = bool(data.get("overwrite", False))

        # valida se survey existe
        with engine.begin() as conn:
            row = conn.execute(text("SELECT 1 FROM survey WHERE survey_id = :sid"), {"sid": survey_id}).first()
        if not row:
            return jsonify({"error": "Pesquisa não encontrada."}), 400

        from service.areas_service import generate_and_save_area_reviews
        updated = generate_and_save_area_reviews(
            survey_id=survey_id,
            model="gpt-4o",
            temperature=0.0,
            overwrite=overwrite
        )
        return jsonify({"updated": int(updated)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.post("/generate_area_reviews_async")
def generate_area_reviews_async():
    """
    Dispara geração de resumos de áreas (somente áreas com intents) em background.
    Retorna job_id e o front abre SSE em /area_review_events/<job_id>.
    """
    data = request.get_json(silent=True) or {}
    survey_id = data.get("survey_id", None)
    overwrite = bool(data.get("overwrite", False))

    try:
        survey_id = int(survey_id)
    except Exception:
        return jsonify({"error": "survey_id inválido"}), 400

    # valida survey existe
    row = None
    with engine.begin() as conn:
        row = conn.execute(text("SELECT 1 FROM survey WHERE survey_id = :sid"), {"sid": survey_id}).first()
    if not row:
        return jsonify({"error": "Pesquisa não encontrada."}), 400

    job_id = str(uuid.uuid4())
    progress_bus.open(job_id)

    th = threading.Thread(
        target=_worker_area_reviews,
        args=(job_id, survey_id, overwrite),
        daemon=True
    )
    th.start()
    return jsonify({"job_id": job_id}), 200

@app.get("/area_review_events/<job_id>")
def area_review_events(job_id: str):
    def generate():
        for chunk in progress_bus.stream(job_id):
            yield chunk
    return Response(generate(), mimetype="text/event-stream")

def _worker_area_reviews(job_id: str, survey_id: int, overwrite: bool):
    """
    Worker: gera resumos por área (com SSE) e, em seguida, gera o resumo geral.
    
    Eventos SSE:
      - 'area_review': {area_id, area_name, status}   (ok|indisponivel|erro)
      - 'general_review': {status}
      - 'info'/'error'/'done'
    """
    try:
            
        progress_bus.put(job_id, {"event": "info", "message": "Iniciando geração de resumos por área..."})

        # --- Carrega áreas com JSON de intents
        df_plan = fetch_survey_areas_with_intents(survey_id)

        # --- Filtra apenas áreas com area_intents não nulo e diferentes de vazio
        df_plan = df_plan[
            df_plan["area_id"] != 0  # ignora área geral
            & df_plan["area_intents"].notna()
            & (df_plan["area_intents"].astype(str).str.strip() != "")
        ]

        # --- Se não for overwrite, remove áreas já com resumo existente
        if not overwrite and "area_review" in df_plan.columns:
            mask_empty = df_plan["area_review"].isna() | (df_plan["area_review"].astype(str).str.strip() == "")
            df_plan = df_plan[mask_empty]

        progress_bus.put(job_id, {"event": "info", "message": f"Áreas a processar: {len(df_plan)}"})

        # --- Callback SSE por área
        def on_progress(area_id: int, area_name: str, status: str):
            progress_bus.put(job_id, {
                "event": "area_review",
                "area_id": area_id,
                "area_name": area_name,
                "status": status
            })
        # --- (1) Gera os resumos por área (exceto área 0)
        updated = generate_and_save_area_reviews(
            survey_id=survey_id,
            model="gpt-4o",
            temperature=0.0,
            overwrite=overwrite,
            on_progress=on_progress,
        )
        progress_bus.put(job_id, {"event": "info", "message": f"Resumos de áreas atualizados: {updated}."})

        # --- (2) Gera o resumo geral (executivo) com base na área_id = 0
        ok = generate_and_save_general_review(
             survey_id=survey_id,
             model="gpt-4o",
             temperature=0.0,
         )
        progress_bus.put(job_id, {"event": "general_review", "status": "ok" if ok else "indisponivel"})
  
    except Exception as e:
        progress_bus.put(job_id, {"event": "error", "message": str(e)})

    finally:
        progress_bus.put(job_id, {"event": "done"})
        progress_bus.close(job_id)

@app.post("/generate_plans_async")
def generate_plans_async():
    
    try:
        data = request.get_json(force=True, silent=True) or {}
        survey_id = int(data.get("survey_id"))
        overwrite = bool(data.get("overwrite", False))
    except Exception:
        return jsonify({"error": "Payload inválido"}), 400

    if not survey_id or survey_id < 1:
        return jsonify({"error": "survey_id inválido"}), 400

    job_id = str(uuid.uuid4())
    progress_bus.open(job_id)
    # dispara thread do worker
    t = threading.Thread(target=_worker_plans, args=(job_id, survey_id, overwrite), daemon=True)
    t.start()

    return jsonify({"job_id": job_id}), 202

@app.get("/plan_events/<job_id>")
def plan_events(job_id: str):
    def generate():
        for chunk in progress_bus.stream(job_id):
            yield chunk
    return Response(generate(), mimetype="text/event-stream")

def _worker_plans(job_id: str, survey_id: int, overwrite: bool):
    """
    Worker: gera planos de ação por área (com SSE).
    Eventos SSE:
      - 'plan': {area_id, area_name, status}   (ok|indisponivel|erro}
      - 'info'/'error'/'done'
    """
    try:
        progress_bus.put(job_id, {"event": "info", "message": "Iniciando geração de planos por área..."})

        df_plan = fetch_survey_areas_with_intents(survey_id)

        df_plan = df_plan[
            (df_plan["area_id"] != 0) &
            (df_plan["area_intents"].notna()) &
            (df_plan["area_intents"].astype(str).str.strip() != "")
        ]

        if not overwrite and "action_plan" in df_plan.columns:
            mask_empty = df_plan["action_plan"].isna() | (df_plan["action_plan"].astype(str).str.strip() == "")
            df_plan = df_plan[mask_empty]

        progress_bus.put(job_id, {"event": "info", "message": f"Áreas a processar: {len(df_plan)}"})

        # Callback por área
        def on_progress(area_id: int, area_name: str, status: str, message: str = None):
            payload = {
                "event": "plan",
                "area_id": area_id,
                "area_name": area_name,
                "status": status
            }
            if message:
                payload["message"] = message
            progress_bus.put(job_id, payload)

        
        # Gera os planos de ação por área
        updated = generate_and_save_area_plans(
            survey_id=survey_id,
            model="gpt-4o",
            temperature=0.0,
            overwrite=overwrite,
            on_progress=on_progress,
        )

        progress_bus.put(job_id, {"event": "info", "message": f"Planos atualizados: {updated}."})

        # gerar plano geral
        ok = generate_and_save_general_plan(
            survey_id=survey_id,
            model="gpt-4o",
            temperature=0.0
        )
        progress_bus.put(job_id, {"event": "info", "message": f"Plano geral {'gerado' if ok else 'não disponível'}."})

    except Exception as e:
        progress_bus.put(job_id, {"event": "error", "message": str(e)})
    finally:
        progress_bus.put(job_id, {"event": "done"})
        progress_bus.close(job_id)

############################################
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)