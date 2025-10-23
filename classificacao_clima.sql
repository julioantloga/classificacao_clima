--
-- PostgreSQL database dump
--

-- Dumped from database version 16.10 (Debian 16.10-1.pgdg13+1)
-- Dumped by pg_dump version 17.5

-- Started on 2025-10-23 11:40:46

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 230 (class 1259 OID 25156)
-- Name: action_plan; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.action_plan (
    id integer NOT NULL,
    theme_name character varying(255),
    action_plan text,
    action_plan_survey_id integer NOT NULL,
    tipo integer
);


ALTER TABLE public.action_plan OWNER TO postgres;

--
-- TOC entry 229 (class 1259 OID 25155)
-- Name: action_plan_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.action_plan_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.action_plan_id_seq OWNER TO postgres;

--
-- TOC entry 3422 (class 0 OID 0)
-- Dependencies: 229
-- Name: action_plan_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.action_plan_id_seq OWNED BY public.action_plan.id;


--
-- TOC entry 224 (class 1259 OID 24578)
-- Name: area; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.area (
    id bigint NOT NULL,
    area_id bigint NOT NULL,
    area_name text NOT NULL,
    area_parent bigint,
    area_survey_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    area_score numeric,
    area_employee_number integer,
    area_comments_number integer,
    area_criticism_number integer,
    area_suggestions_number integer,
    area_recognition_number integer,
    area_response_rate numeric,
    area_intents jsonb,
    area_review text,
    area_level integer,
    area_plan text
);


ALTER TABLE public.area OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 24577)
-- Name: area_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.area_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.area_id_seq OWNER TO postgres;

--
-- TOC entry 3423 (class 0 OID 0)
-- Dependencies: 223
-- Name: area_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.area_id_seq OWNED BY public.area.id;


--
-- TOC entry 220 (class 1259 OID 16551)
-- Name: comment; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.comment (
    comment_id integer NOT NULL,
    comment text NOT NULL,
    comment_employee_id integer NOT NULL,
    comment_question_id integer NOT NULL,
    comment_survey_id integer,
    comment_area_id integer
);


ALTER TABLE public.comment OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 16550)
-- Name: comment_comment_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.comment_comment_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.comment_comment_id_seq OWNER TO postgres;

--
-- TOC entry 3424 (class 0 OID 0)
-- Dependencies: 219
-- Name: comment_comment_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.comment_comment_id_seq OWNED BY public.comment.comment_id;


--
-- TOC entry 232 (class 1259 OID 25288)
-- Name: config_empresa; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.config_empresa (
    id integer NOT NULL,
    sobre_empresa text,
    valores text,
    politicas text,
    canais_comunicacao text,
    armazenamento_info text,
    acoes_rh text,
    metricas text,
    survey_id integer
);


ALTER TABLE public.config_empresa OWNER TO postgres;

--
-- TOC entry 231 (class 1259 OID 25287)
-- Name: config_empresa_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.config_empresa_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.config_empresa_id_seq OWNER TO postgres;

--
-- TOC entry 3425 (class 0 OID 0)
-- Dependencies: 231
-- Name: config_empresa_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.config_empresa_id_seq OWNED BY public.config_empresa.id;


--
-- TOC entry 226 (class 1259 OID 24598)
-- Name: employee; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.employee (
    id bigint NOT NULL,
    employee_id integer NOT NULL,
    employee_email text NOT NULL,
    employee_name text,
    employee_area_id integer NOT NULL,
    employee_manager_id integer NOT NULL,
    employee_survey_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.employee OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 24597)
-- Name: employee_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.employee_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.employee_id_seq OWNER TO postgres;

--
-- TOC entry 3426 (class 0 OID 0)
-- Dependencies: 225
-- Name: employee_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.employee_id_seq OWNED BY public.employee.id;


--
-- TOC entry 222 (class 1259 OID 16570)
-- Name: perception; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.perception (
    perception_id integer NOT NULL,
    perception_comment_id integer NOT NULL,
    perception_comment_clipping text,
    perception_theme character varying(255),
    perception_intension character varying(100),
    perception_survey_id integer,
    perception_area_id integer
);


ALTER TABLE public.perception OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 16569)
-- Name: perception_perception_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.perception_perception_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.perception_perception_id_seq OWNER TO postgres;

--
-- TOC entry 3427 (class 0 OID 0)
-- Dependencies: 221
-- Name: perception_perception_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.perception_perception_id_seq OWNED BY public.perception.perception_id;


--
-- TOC entry 218 (class 1259 OID 16504)
-- Name: question; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.question (
    question_id integer NOT NULL,
    question_name character varying(500) NOT NULL,
    question_survey_id integer NOT NULL
);


ALTER TABLE public.question OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 16503)
-- Name: question_question_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.question_question_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.question_question_id_seq OWNER TO postgres;

--
-- TOC entry 3428 (class 0 OID 0)
-- Dependencies: 217
-- Name: question_question_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.question_question_id_seq OWNED BY public.question.question_id;


--
-- TOC entry 216 (class 1259 OID 16492)
-- Name: survey; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.survey (
    survey_id integer NOT NULL,
    survey_name character varying(255) NOT NULL
);


ALTER TABLE public.survey OWNER TO postgres;

--
-- TOC entry 215 (class 1259 OID 16491)
-- Name: survey_survey_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.survey_survey_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.survey_survey_id_seq OWNER TO postgres;

--
-- TOC entry 3429 (class 0 OID 0)
-- Dependencies: 215
-- Name: survey_survey_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.survey_survey_id_seq OWNED BY public.survey.survey_id;


--
-- TOC entry 228 (class 1259 OID 25149)
-- Name: theme_ranking; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.theme_ranking (
    id integer NOT NULL,
    area_id integer NOT NULL,
    theme_name character varying(255) NOT NULL,
    score numeric(5,2) NOT NULL,
    dissatisfied_score numeric(5,2),
    comment_score numeric(5,2),
    ranking integer,
    survey_id integer NOT NULL,
    direct_score numeric(5,2),
    direct_dissatisfied_score numeric(5,2),
    direct_comment_score numeric(5,2),
    direct_ranking integer
);


ALTER TABLE public.theme_ranking OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 25148)
-- Name: theme_ranking_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.theme_ranking_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.theme_ranking_id_seq OWNER TO postgres;

--
-- TOC entry 3430 (class 0 OID 0)
-- Dependencies: 227
-- Name: theme_ranking_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.theme_ranking_id_seq OWNED BY public.theme_ranking.id;


--
-- TOC entry 3252 (class 2604 OID 25159)
-- Name: action_plan id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.action_plan ALTER COLUMN id SET DEFAULT nextval('public.action_plan_id_seq'::regclass);


--
-- TOC entry 3247 (class 2604 OID 24581)
-- Name: area id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.area ALTER COLUMN id SET DEFAULT nextval('public.area_id_seq'::regclass);


--
-- TOC entry 3245 (class 2604 OID 16554)
-- Name: comment comment_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.comment ALTER COLUMN comment_id SET DEFAULT nextval('public.comment_comment_id_seq'::regclass);


--
-- TOC entry 3253 (class 2604 OID 25291)
-- Name: config_empresa id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.config_empresa ALTER COLUMN id SET DEFAULT nextval('public.config_empresa_id_seq'::regclass);


--
-- TOC entry 3249 (class 2604 OID 24601)
-- Name: employee id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.employee ALTER COLUMN id SET DEFAULT nextval('public.employee_id_seq'::regclass);


--
-- TOC entry 3246 (class 2604 OID 16573)
-- Name: perception perception_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.perception ALTER COLUMN perception_id SET DEFAULT nextval('public.perception_perception_id_seq'::regclass);


--
-- TOC entry 3244 (class 2604 OID 16507)
-- Name: question question_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.question ALTER COLUMN question_id SET DEFAULT nextval('public.question_question_id_seq'::regclass);


--
-- TOC entry 3243 (class 2604 OID 16495)
-- Name: survey survey_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.survey ALTER COLUMN survey_id SET DEFAULT nextval('public.survey_survey_id_seq'::regclass);


--
-- TOC entry 3251 (class 2604 OID 25152)
-- Name: theme_ranking id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.theme_ranking ALTER COLUMN id SET DEFAULT nextval('public.theme_ranking_id_seq'::regclass);


--
-- TOC entry 3269 (class 2606 OID 25163)
-- Name: action_plan action_plan_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.action_plan
    ADD CONSTRAINT action_plan_pkey PRIMARY KEY (id);


--
-- TOC entry 3263 (class 2606 OID 24586)
-- Name: area area_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.area
    ADD CONSTRAINT area_pkey PRIMARY KEY (id);


--
-- TOC entry 3259 (class 2606 OID 16558)
-- Name: comment comment_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT comment_pkey PRIMARY KEY (comment_id);


--
-- TOC entry 3271 (class 2606 OID 25295)
-- Name: config_empresa config_empresa_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.config_empresa
    ADD CONSTRAINT config_empresa_pkey PRIMARY KEY (id);


--
-- TOC entry 3265 (class 2606 OID 24606)
-- Name: employee employee_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.employee
    ADD CONSTRAINT employee_pkey PRIMARY KEY (id);


--
-- TOC entry 3261 (class 2606 OID 16577)
-- Name: perception perception_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.perception
    ADD CONSTRAINT perception_pkey PRIMARY KEY (perception_id);


--
-- TOC entry 3257 (class 2606 OID 16511)
-- Name: question question_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.question
    ADD CONSTRAINT question_pkey PRIMARY KEY (question_id);


--
-- TOC entry 3255 (class 2606 OID 16497)
-- Name: survey survey_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.survey
    ADD CONSTRAINT survey_pkey PRIMARY KEY (survey_id);


--
-- TOC entry 3267 (class 2606 OID 25154)
-- Name: theme_ranking theme_ranking_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.theme_ranking
    ADD CONSTRAINT theme_ranking_pkey PRIMARY KEY (id);


--
-- TOC entry 3273 (class 2606 OID 16564)
-- Name: comment fk_comment_question; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT fk_comment_question FOREIGN KEY (comment_question_id) REFERENCES public.question(question_id) ON DELETE CASCADE;


--
-- TOC entry 3272 (class 2606 OID 16512)
-- Name: question fk_question_survey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.question
    ADD CONSTRAINT fk_question_survey FOREIGN KEY (question_survey_id) REFERENCES public.survey(survey_id) ON DELETE CASCADE;


-- Completed on 2025-10-23 11:41:01

--
-- PostgreSQL database dump complete
--

