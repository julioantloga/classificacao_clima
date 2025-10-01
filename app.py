from flask import Flask, Response, request, render_template, send_from_directory, jsonify
from sqlalchemy import text
import pandas as pd
import os
from db_config import engine
import tempfile, uuid, threading, time
from service.progress import progress_bus, timed_step
import markdown

from service.comment_repository import (
    get_comment_perceptions_search
)

from service.classification_service import (
    data_preprocessing,
    persist_questions_and_comments,
    define_category_themes,
    save_themes_score
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
    fetch_survey_areas_with_intents,
    update_theme_ranking_scores,
)

from service.person_repository import (
    insert_person
)

from service.perception_service import classify_and_save_perceptions

from service.areas_service import (
    create_organizational_chart,
    compute_and_update_area_metrics_python,
    generate_and_save_area_reviews,
    get_themes_intents,
    comment_score_calc,
    get_theme_ranking,
    calculate_theme_average,
    
)

from service.general_review import (
    generate_and_save_general_review,
    get_ranking_area,
    get_ranking_general_themes,
    save_general_ranking,
    get_comment_clippings_for_critical_themes,
    generate_action_plans,
    get_general_action_plan
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
        
        def markdown_to_html(text):
            if pd.isna(text):
                return '—'
            return markdown.markdown(text, extensions=['extra'])

        data = get_general_action_plan(survey_id)
        data['action_plan'] = data['action_plan'].apply(markdown_to_html)
        #data['action_plan'] = data['action_plan'].str.replace('\n', '<br>', regex=False)
        data = data.to_dict(orient="records")
        return render_template("overview.html",survey=survey, data=data)
    
    if page == "area":        
        areas = list_areas_with_non_null_score(survey_id)
        return render_template("area.html", survey=survey, areas=areas)
    
    if page == "comments":

        rows = get_comments_with_perceptions(survey_id)
        areas = list_areas_with_non_null_score(survey_id)
        themes = list_perception_themes_for_survey(survey_id)
        selected_area = 0
        selected_theme = "all"
        selected_intention = "all"

        return render_template("comments.html",
                survey=survey,
                rows=rows,
                areas=areas,
                themes=themes,
                selected_area = selected_area,
                selected_theme = selected_theme,
                selected_intention = selected_intention
            )


#Busca o resumo por área 
@app.route('/dashboard/areas/search', methods=['GET'])
def dashboard_areas_search():
    survey_id = request.args.get("survey_id", type=int)
    area_raw = request.args.get("area", default="")
    selected_area = int(area_raw) if area_raw not in (None, "",) else 0  # vazio => 0

    survey = get_survey(survey_id)
    areas = list_areas_with_non_null_score(survey_id)
    data = get_area_review_plan(area_id=selected_area, survey_id=survey_id) 
    
    # Gráfico de temas x intenções
    themes_intents_df = get_themes_intents(area_id=selected_area, survey_id=survey_id) if selected_area else None
    themes_intents_data = themes_intents_df.to_dict(orient='records') if themes_intents_df is not None else []
    
    return render_template("area.html",
                           survey=survey,
                           areas=areas,
                           data=data,
                           selected_area=selected_area,
                           themes_intents_data=themes_intents_data)


#Busca de comentários 
@app.route('/dashboard/comments/search', methods=['GET'])
def dashboard_comments_search():
    survey = request.args.get("survey_id", type=int)

    rows = get_comments_with_perceptions(survey)
    areas = list_areas_with_non_null_score(survey)
    themes = list_perception_themes_for_survey(survey)
    
    selected_area = request.args.get("area", default="")
    selected_intention = request.args.get("intention", default="").strip()
    selected_theme = request.args.get("theme", default="").strip()
    
    rows = get_comment_perceptions_search(survey, selected_area, selected_intention, selected_theme)
    
    return render_template("comments.html",
                           survey=survey,
                           #data=data,
                           rows=rows,
                           areas=areas,
                           themes=themes,
                           selected_area=selected_area,
                           selected_intention=selected_intention,
                           selected_theme=selected_theme)


#Descartes
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
    df_notas_categorias_file  = request.files["nota_categorias"]

    try:
        # Ler CSVs
        df_campanha = read_csv_flex(campanha_file)
        df_person = read_csv_flex(pessoas_file)
        df_instancia_areas = read_csv_flex(instancia_areas_file)
        df_hierarquia_areas = read_csv_flex(hierarquia_areas_file)
        df_person_areas = read_csv_flex(person_areas_file)
        df_notas_areas = read_csv_flex(df_notas_areas_file)
        df_notas_categorias = read_csv_flex(df_notas_categorias_file)

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
    required = ["campanha", "pessoas", "instancia_areas", "hierarquia_areas", "person_areas", "nota_areas", "nota_categorias"]
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


#Classifica os Comentários e analisa os dados
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

        # Leitura CSVs
        def _read_csvs():
            df_campanha        = read_csv_flex(paths["campanha"])
            df_person          = read_csv_flex(paths["pessoas"])
            df_instancia_areas = read_csv_flex(paths["instancia_areas"])
            df_hierarquia_areas= read_csv_flex(paths["hierarquia_areas"])
            df_person_areas    = read_csv_flex(paths["person_areas"])
            df_notas_areas    = read_csv_flex(paths["nota_areas"])
            df_notas_categorias    = read_csv_flex(paths["nota_categorias"])


            return df_campanha, df_person, df_instancia_areas, df_hierarquia_areas, df_person_areas, df_notas_areas, df_notas_categorias
        
        (df_campanha, df_person, df_instancia_areas, df_hierarquia_areas, df_person_areas, df_notas_areas, df_notas_categorias) = timed_step(job_id, "1 - lendo arquivos (csv)...", _read_csvs)
        
        # 1) Survey
        def _survey():
            name = config.get("nome_pesquisa")
            return insert_survey(name)

        survey_id = timed_step(job_id, "1 - criando pesquisa...", _survey)
        progress_bus.put(job_id, {"event": "survey", "survey_id": survey_id}) #check na barra de progresso


        # 2) Pessoas (alocação ativa)
        def _people():
            #normaliza data frame
            df_employee = person_preprocessing(df_person, df_person_areas, survey_id)
            #insere employees na base de dados
            count = insert_person(df_employee)
            return df_employee, count
        
        (df_employee, count_employee) = timed_step(job_id, "2 - inserindo funcionários...", _people)
        progress_bus.put(job_id, {"event": "person", "funcionários adicionados": count_employee}) #check na barra de progresso

        # 3) Áreas (instância + hierarquia)
        def _areas():
            # normaliza data frame e organiza hierarquia de áreas
            df_areas = create_organizational_chart(df_instancia_areas, df_hierarquia_areas, survey_id)
            # insere áreas na base de dados
            count = insert_areas(df_areas)    
            return df_areas, count
        
        df_areas, areas_count = timed_step(job_id, "Inserindo áreas e definindo organograma...", _areas)
        progress_bus.put(job_id, {"event": "area", "Áreas adicionadas": areas_count}) #check na barra de progresso

        # 4) Monta base de temas por área
        def _themes():
            df_area_category = df_notas_categorias[["categoria"]].drop_duplicates().reset_index(drop=True)
            return define_category_themes (df_area_category, temas)

        df_category_themes = timed_step(job_id, "Atribuindo categorias aos temas...", _themes)

        df_notas_theme = df_notas_categorias.merge(
            df_category_themes,     # DataFrame com categoria e tema
            on="categoria",         # coluna em comum
            how="left"              # mantém todas as linhas de df_notas_categorias
        )

        save_themes_score (df_notas_theme, survey_id)
        progress_bus.put(job_id, {"event": "themes", "Temas classificados": "-"}) #check na barra de progresso
        
        # 4) Pré-processar campanha → df_final

        # Completar e-mails na df_campanha a partir do Nome e Sobrenome
        df_temp = pd.merge(
            df_campanha,
            df_person,
            how='left',
            left_on=['Nome', 'Sobrenome'],
            right_on=['nome', 'sobrenome']
        )

        df_campanha['Email'] = df_campanha['Email'].where(
            df_campanha['Email'].notna(),
            df_temp['email']
        )

        df_processed = timed_step(job_id, "Normalizar respostas (df_final)", data_preprocessing, df_campanha, survey_id, df_employee)
           
        # 6) Perguntas + Comentários
        q_c_stats = timed_step(job_id, "Inserir questões e comentários", persist_questions_and_comments, df_processed)
        progress_bus.put(job_id, {"event": "stats", "scope": "questions_comments", "data": q_c_stats})

        # 7) Classificar Percepções
        perc_stats = timed_step(job_id, "Classificar percepções", classify_and_save_perceptions, survey_id, temas, "gpt-4o", 0.0, False)
        progress_bus.put(job_id, {"event": "stats", "scope": "perceptions", "tokens_saida": "completion_tokens", "data": perc_stats})

        # 8) Calcula nota dos comentários do tema
        def _themes_by_comment():
            
            scores = comment_score_calc(survey_id, df_areas)
            update_theme_ranking_scores(survey_id, scores)
            theme_ranking = get_theme_ranking(survey_id)
            df_ranking = calculate_theme_average (theme_ranking, survey_id)
 
            # Ordena pela menor nota geral
            df_ranking = df_ranking.sort_values(by="nota_geral", ascending=True).reset_index(drop=True)
            df_ranking['ranking'] = range(1, len(df_ranking) + 1)
 
            df_ranking = df_ranking.sort_values(by="nota_direta", ascending=True).reset_index(drop=True)
            df_ranking['ranking_direta'] = range(1, len(df_ranking) + 1)

            return df_ranking
        
        ranking_final = timed_step(job_id, "Calculando nota dos comentários...", _themes_by_comment)
        save_general_ranking(ranking_final)

        # 8) Calcular scores de áreas (Python)
        min_commenters = 3
        areas_upd = timed_step(job_id, "Calcular o scores de áreas",compute_and_update_area_metrics_python,survey_id, df_notas_areas, min_lvl, max_lvl, min_commenters)
        progress_bus.put(job_id, {"event": "stats", "scope": "area_metrics_py", "data": {"areas_atualizadas": areas_upd}})

        # 9) Classificar perguntas fechadas da pesquisa por tema

        # df_perguntas_fechadas = (
        #     df_notas_areas[['pergunta']]
        #     .drop_duplicates()
        #     .reset_index(drop=True)
        # )
        
        # tema_perguntas_fechadas = []
        
        # for pergunta in df_perguntas_fechadas['pergunta']:
            
        #     tema = timed_step(job_id, f"""Classificando questao: {pergunta}""", closed_question_classification, pergunta, temas_perguntas)
        #     tema_perguntas_fechadas.append((pergunta, tema))
        #     time.sleep(1.2)
        #     progress_bus.put(job_id, {"event": "perguntas classificadas", "message": tema_perguntas_fechadas})

        # #garante o df
        # df_areas_perguntas_fechadas = pd.DataFrame(tema_perguntas_fechadas, columns=["pergunta", "tema"])

        # df_notas_areas = df_notas_areas.merge(
        #     df_areas_perguntas_fechadas,
        #     on='pergunta',
        #     how='left'
        # )

        # df_notas_areas['nota'] = pd.to_numeric(df_notas_areas['nota'])
        
        # df_nota_area_temas = (
        #     df_notas_areas
        #     .dropna(subset=['nota', 'tema'])  # Remove linhas sem nota ou sem tema
        #     .groupby(['area_id', 'tema'], as_index=False)
        #     .agg(media=('nota', 'mean'))  # Calcula a média
        # )

        # print (df_nota_area_temas)

        # progress_bus.put(job_id, {"event": "info", "message": f"Pipeline concluído com sucesso. {df_nota_area_temas}"})

        progress_bus.put(job_id, {"event": "info", "message": f"Pipeline concluído com sucesso."})
    
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


# Dispara geração dos planos de ação 
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
    Worker: gera planos de ação.
    """
    
    try:

        progress_bus.put(job_id, {"event": "info", "message": "Iniciando geração de planos de ação..."})
        
        df_planos_de_acao = pd.read_csv("file/planos_de_acao.csv", sep=";")
        
        critical_themes = get_comment_clippings_for_critical_themes(survey_id)
        predefined_plans_json = df_planos_de_acao[df_planos_de_acao["theme_name"].isin(critical_themes["theme_name"])]
        

        action_plans, plan_review = generate_action_plans(critical_themes, predefined_plans_json, survey_id)

        #df_plan = fetch_survey_areas_with_intents(survey_id)

        # df_plan = df_plan[
        #     (df_plan["area_id"] != 0) &
        #     (df_plan["area_intents"].notna()) &
        #     (df_plan["area_intents"].astype(str).str.strip() != "")
        # ]

        # if not overwrite and "action_plan" in df_plan.columns:
        #     mask_empty = df_plan["action_plan"].isna() | (df_plan["action_plan"].astype(str).str.strip() == "")
        #     df_plan = df_plan[mask_empty]

        # progress_bus.put(job_id, {"event": "info", "message": f"Áreas a processar: {len(df_plan)}"})

        # # Callback por área
        # def on_progress(area_id: int, area_name: str, status: str, message: str = None):
        #     payload = {
        #         "event": "plan",
        #         "area_id": area_id,
        #         "area_name": area_name,
        #         "status": status
        #     }
        #     if message:
        #         payload["message"] = message
        #     progress_bus.put(job_id, payload)

        
        # # Gera os planos de ação por área
        # updated = generate_and_save_area_plans(
        #     survey_id=survey_id,
        #     model="gpt-4o",
        #     temperature=0.0,
        #     overwrite=overwrite,
        #     on_progress=on_progress,
        # )

        # progress_bus.put(job_id, {"event": "info", "message": f"Planos atualizados: {updated}."})

        # # gerar plano geral
        # ok = generate_and_save_general_plan(
        #     survey_id=survey_id,
        #     model="gpt-4o",
        #     temperature=0.0
        # )
        # progress_bus.put(job_id, {"event": "info", "message": f"Plano geral {'gerado' if ok else 'não disponível'}."})

    except Exception as e:
        progress_bus.put(job_id, {"event": "error", "message": str(e)})
    finally:
        progress_bus.put(job_id, {"event": "done"})
        progress_bus.close(job_id)

############################################
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)