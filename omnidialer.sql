--
-- PostgreSQL database dump
--

-- Dumped from database version 14.9 (Debian 14.9-1.pgdg110+1)
-- Dumped by pg_dump version 14.9 (Debian 14.9-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
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
-- Name: jobs; Type: TABLE; Schema: public; Owner: omnidialer
--


CREATE TABLE public.jobs (
    id SERIAL PRIMARY KEY,
    job_id TEXT UNIQUE NOT NULL,
    job_name TEXT NOT NULL,
    datime TIMESTAMPTZ NOT NULL DEFAULT now(),
    parametros JSONB,
    status INTEGER NOT NULL,
    error TEXT
);

CREATE INDEX idx_jobs_job_name ON public.jobs(job_name);
CREATE INDEX idx_jobs_datime ON public.jobs(datime);
CREATE INDEX idx_jobs_status ON public.jobs(status);
CREATE INDEX idx_jobs_name_status ON public.jobs(job_name, status);

ALTER TABLE public.jobs OWNER TO omnidialer;

--
-- Name: system.control; Type: TABLE; Schema: public; Owner: omnidialer
--

CREATE TABLE public.system_control (
    id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    is_active BOOLEAN NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);

ALTER TABLE public.system_control OWNER TO omnidialer;

INSERT INTO public.system_control (id, is_active) VALUES (true, true);

--
-- Name: campaign; Type: TABLE; Schema: public; Owner: omnidialer
--

CREATE TABLE public.campaign (
    id integer NOT NULL,
    oml_status integer NOT NULL,
    name character varying(128) NOT NULL,
    start_date date,
    end_date date,
    duplicates_control integer NOT NULL,
    priority integer NOT NULL,
    strategy character varying(128) NOT NULL,
    wait integer NOT NULL,
    max_channels integer NOT NULL,
    initial_predictive_model boolean NOT NULL,
    initial_boost_factor numeric(3,1),
    sunday boolean NOT NULL,
    monday boolean NOT NULL,
    tuesday boolean NOT NULL,
    wednesday boolean NOT NULL,
    thursday boolean NOT NULL,
    friday boolean NOT NULL,
    saturday boolean NOT NULL,
    hour_start time without time zone NOT NULL,
    hour_ends time without time zone NOT NULL,
    contact_strategy integer[],
    dialer_status integer NOT NULL,
    statistics JSONB,
    metadata JSONB,
    customdialerdst boolean NOT NULL,
    prefix character varying(128)
);


ALTER TABLE public.campaign OWNER TO omnidialer;


--
-- Name: campaign_id_seq; Type: SEQUENCE; Schema: public; Owner: omnidialer
--

CREATE SEQUENCE public.campaign_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.campaign_id_seq OWNER TO omnidialer;

--
-- Name: campaign_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: omnidialer
--

ALTER SEQUENCE public.campaign_id_seq OWNED BY public.campaign.id;


--
-- Name: contact; Type: TABLE; Schema: public; Owner: omnidialer
--

CREATE TABLE public.contact (
    id integer NOT NULL,
    phone character varying(128) NOT NULL,
    data text NOT NULL,
    is_original boolean NOT NULL
);


ALTER TABLE public.contact OWNER TO omnidialer;

--
-- Name: contact_id_seq; Type: SEQUENCE; Schema: public; Owner: omnidialer
--

CREATE SEQUENCE public.contact_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.contact_id_seq OWNER TO omnidialer;

--
-- Name: contact_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: omnidialer
--

ALTER SEQUENCE public.contact_id_seq OWNED BY public.contact.id;


--
-- Name: contact_in_campaign; Type: TABLE; Schema: public; Owner: omnidialer
--

CREATE TABLE public.contact_in_campaign (
    id_campaign integer NOT NULL,
    id_contact integer NOT NULL,
    id integer NOT NULL,
    status integer NOT NULL,
    status_pstn boolean NOT NULL DEFAULT false,
    final_status integer NOT NULL,
    schedule_aborted boolean NOT NULL DEFAULT false,
    disposition_option integer,
    history text[],
    phone_numbers_list text[],
    phone_number_index integer
);


ALTER TABLE public.contact_in_campaign OWNER TO omnidialer;

--
-- Name: contact_in_campaign_id_seq; Type: SEQUENCE; Schema: public; Owner: omnidialer
--

CREATE SEQUENCE public.contact_in_campaign_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.contact_in_campaign_id_seq OWNER TO omnidialer;

--
-- Name: contact_in_campaign_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: omnidialer
--

ALTER SEQUENCE public.contact_in_campaign_id_seq
    OWNED BY public.contact_in_campaign.id;

ALTER SEQUENCE public.contact_in_campaign_id_seq
    OWNER TO omnidialer;


--
-- Name: COLUMN contact_in_campaign.id_campaign; Type: COMMENT; Schema: public; Owner: omnidialer
--

COMMENT ON COLUMN public.contact_in_campaign.id_campaign IS 'foreign key to campaign table';


--
-- Name: COLUMN contact_in_campaign.id_contact; Type: COMMENT; Schema: public; Owner: omnidialer
--

COMMENT ON COLUMN public.contact_in_campaign.id_contact IS 'link to contact table';

--
-- Name: incidence_rules; Type: TABLE; Schema: public; Owner: omnidialer
--

CREATE TABLE public.incidence_rules (
    id integer NOT NULL,
    status integer NOT NULL,
    status_custom character varying(128),
    max_attempt integer NOT NULL,
    retry_later integer NOT NULL,
    in_mode integer NOT NULL,
    campaign_id integer NOT NULL,
    CONSTRAINT incidence_rules_in_mode_check CHECK ((in_mode >= 0)),
    CONSTRAINT incidence_rules_status_check CHECK ((status >= 0))
);


ALTER TABLE public.incidence_rules OWNER TO omnidialer;


--
-- Name: incidence_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: omnidialer
--

CREATE SEQUENCE public.incidence_rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.incidence_rules_id_seq OWNER TO omnidialer;

--
-- Name: incidence_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: omnidialer
--

ALTER SEQUENCE public.incidence_rules_id_seq OWNED BY public.incidence_rules.id;


--
-- Name: campaign id; Type: DEFAULT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.campaign ALTER COLUMN id SET DEFAULT nextval('public.campaign_id_seq'::regclass);


--
-- Name: contact id; Type: DEFAULT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.contact ALTER COLUMN id SET DEFAULT nextval('public.contact_id_seq'::regclass);


--
-- Name: incidence_rules id; Type: DEFAULT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.incidence_rules ALTER COLUMN id SET DEFAULT nextval('public.incidence_rules_id_seq'::regclass);



--
-- Name: contact_in_campaign id; Type: DEFAULT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.contact_in_campaign ALTER COLUMN id SET DEFAULT nextval('public.contact_in_campaign_id_seq'::regclass);



--
-- Name: campaign campaign_pkey; Type: CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.campaign
    ADD CONSTRAINT campaign_pkey PRIMARY KEY (id);


--
-- Name: contact contact_pkey; Type: CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.contact
    ADD CONSTRAINT contact_pkey PRIMARY KEY (id);


--
-- Name: incidence_rules incidence_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.incidence_rules
    ADD CONSTRAINT incidence_rules_pkey PRIMARY KEY (id);


--
-- Name: contact_in_campaign primary_key_contact_in_campaign; Type: CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.contact_in_campaign
    ADD CONSTRAINT primary_key_contact_in_campaign PRIMARY KEY (id);


--
-- Name: fki_foreign_key_contact; Type: INDEX; Schema: public; Owner: omnidialer
--

CREATE INDEX fki_foreign_key_contact ON public.contact_in_campaign USING btree (id_contact);


--
-- Name: fki_foreign_key_campaign; Type: INDEX; Schema: public; Owner: omnidialer
--

CREATE INDEX fki_foreign_key_campaign ON public.contact_in_campaign USING btree (id_campaign);
CREATE INDEX idx_contact_in_campaign_camp_status ON public.contact_in_campaign USING btree (id_campaign, status);
CREATE INDEX idx_contact_in_campaign_camp_final_status ON public.contact_in_campaign USING btree (id_campaign, final_status);

--
-- Name: incidence_rules_campaign_id_707899e9; Type: INDEX; Schema: public; Owner: omnidialer
--

CREATE INDEX incidence_rules_campaign_id_707899e9 ON public.incidence_rules USING btree (campaign_id);

--
-- Name: contact_in_campaign foreign_key_campaign; Type: FK CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.contact_in_campaign
    ADD CONSTRAINT foreign_key_campaign FOREIGN KEY (id_campaign) REFERENCES public.campaign(id) ON DELETE CASCADE NOT VALID;


--
-- Name: contact_in_campaign foreign_key_contact; Type: FK CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.contact_in_campaign
    ADD CONSTRAINT foreign_key_contact FOREIGN KEY (id_contact) REFERENCES public.contact(id) ON DELETE CASCADE NOT VALID;

--
-- Name: incidence_rules re_campaign_id_707899e9_fk_ominicont; Type: FK CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.incidence_rules
    ADD CONSTRAINT re_campaign_id_707899e9_fk_ominicont FOREIGN KEY (campaign_id) REFERENCES public.campaign(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED ;


CREATE TABLE public.incidence_rules_disposition (
    id integer NOT NULL,
    disposition_option_id integer NOT NULL,
    max_attempt integer NOT NULL,
    retry_later integer NOT NULL,
    in_mode integer NOT NULL,
    campaign_id integer NOT NULL,
    CONSTRAINT incidence_rules_in_mode_check CHECK ((in_mode >= 0))
);

ALTER TABLE public.incidence_rules_disposition OWNER TO omnidialer;

--
-- Name: incidence_rules_disposition_id_seq; Type: SEQUENCE; Schema: public; Owner: omnidialer
--

CREATE SEQUENCE public.incidence_rules_disposition_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.incidence_rules_disposition_id_seq OWNER TO omnidialer;

--
-- Name: incidence_rules_disposition_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: omnidialer
--

ALTER SEQUENCE public.incidence_rules_disposition_id_seq OWNED BY public.incidence_rules_disposition.id;

--
-- Name: incidence_rules_disposition id; Type: DEFAULT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.incidence_rules_disposition ALTER COLUMN id SET DEFAULT nextval('public.incidence_rules_disposition_id_seq'::regclass);

--
-- Name: incidence_rules_disposition incidence_rules_disposition_pkey; Type: CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.incidence_rules_disposition
    ADD CONSTRAINT incidence_rules_disposition_pkey PRIMARY KEY (id);

--
-- Name: incidence_rules_disposition_campaign_disposition_id_707899e9; Type: INDEX; Schema: public; Owner: omnidialer
--

CREATE INDEX incidence_rules_campaign_disposition_id_707899e9 ON public.incidence_rules_disposition USING btree (campaign_id);

--
-- Name: incidence_rules_disposition re_campaign_id_707899ea_fk_ominicont; Type: FK CONSTRAINT; Schema: public; Owner: omnidialer
--

ALTER TABLE ONLY public.incidence_rules_disposition
    ADD CONSTRAINT re_campaign_id_707899ea_fk_ominicont FOREIGN KEY (campaign_id) REFERENCES public.campaign(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED ;

--
-- PostgreSQL database dump complete
--
