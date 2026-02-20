# ======================================
# app.py - NewsAnalyzer AI v9.0
# ======================================

# ======================================
# Importaciones
# ======================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, NamedStyle
from collections import defaultdict, Counter
from difflib import SequenceMatcher
from copy import deepcopy
import datetime
import io
import re
import time
from unidecode import unidecode
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import json
import asyncio
import hashlib
from typing import List, Dict, Tuple, Optional, Any
import joblib
import gc

# ======================================
# Detección de versión de OpenAI
# ======================================
import openai as openai_module

try:
    from openai import AsyncOpenAI, OpenAI
    OPENAI_NEW_API = True
except ImportError:
    OPENAI_NEW_API = False
    import openai

# ======================================
# Configuración general
# ======================================
st.set_page_config(
    page_title="NewsAnalyzer AI",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="collapsed"
)

OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

CONCURRENT_REQUESTS = 50
SIMILARITY_THRESHOLD_TONO = 0.88
SIMILARITY_THRESHOLD_TITULOS = 0.93
MAX_CONTEXT_WINDOW = 200
MIN_CONTEXT_WINDOW = 80

NUM_TEMAS_PRINCIPALES = 20
UMBRAL_FUSION_SUBTEMAS = 0.78
UMBRAL_FUSION_TEMAS = 0.80

PRICE_INPUT_1M = 0.10
PRICE_OUTPUT_1M = 0.40
PRICE_EMBEDDING_1M = 0.02

for _counter_key in ['tokens_input', 'tokens_output', 'tokens_embedding']:
    if _counter_key not in st.session_state:
        st.session_state[_counter_key] = 0

CIUDADES_COLOMBIA = {
    "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla",
    "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales",
    "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria",
    "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja",
    "florencia", "sincelejo", "riohacha", "yopal", "santa marta",
    "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu",
    "puerto carreño", "inírida", "inirida", "san josé del guaviare",
    "antioquia", "atlántico", "atlantico", "bolívar", "bolivar",
    "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare",
    "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba",
    "cundinamarca", "guainía", "guainia", "guaviare", "huila",
    "la guajira", "magdalena", "meta", "nariño", "narino",
    "norte de santander", "putumayo", "quindío", "quindio", "risaralda",
    "san andrés", "san andres", "santander", "sucre", "tolima",
    "valle del cauca", "vaupés", "vaupes", "vichada"
}
GENTILICIOS_COLOMBIA = {
    "bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino",
    "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños",
    "antioqueña", "antioqueñas", "paisa", "paisas", "medellense",
    "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno",
    "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos",
    "barranquillero", "barranquilleros", "cartagenero", "cartageneros",
    "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños",
    "bumangués", "santandereano", "santandereanos", "boyacense",
    "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses",
    "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés",
    "cordobeses", "caucano", "caucanos", "chocoano", "chocoanos",
    "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro",
    "guajiros", "llanero", "llaneros", "amazonense", "amazonenses",
    "colombiano", "colombianos", "colombiana", "colombianas"
}

# ======================================
# Stopwords y Patrones Léxicos
# ======================================
STOPWORDS_ES = set(
    "a ante bajo cabe con contra de desde durante en entre hacia hasta "
    "mediante para por segun sin so sobre tras y o u e la el los las un "
    "una unos unas lo al del se su sus le les mi mis tu tus nuestro "
    "nuestros vuestra vuestras este esta estos estas ese esa esos esas "
    "aquel aquella aquellos aquellas que cual cuales quien quienes cuyo "
    "cuya cuyos cuyas como cuando donde cual es son fue fueron era eran "
    "sera seran seria serian he ha han habia habian hay hubo habra habria "
    "estoy esta estan estaba estaban estamos estar estare estaria "
    "estuvieron estarian estuvo asi ya mas menos tan tanto cada".split()
)

POS_BRAND_PATTERNS = [
    r"lanz[aó]|lanzamiento|lanzar[aá]|estrena|habilita|inaugur[aó]",
    r"nuev[oa]\s+\w+",
    r"apertur[aó]|abri[óo]\s+(su|una|nuevo)",
    r"alianza|acuerdo|convenio|colaboraci[oó]n|asociaci[oó]n|fusi[oó]n",
    r"crecimiento|crecieron|aument[oó]|ganancia|utilidad|beneficio",
    r"récord|record|hito|supera(r|ndo|ción)",
    r"reconocimiento|premio|premia|galardon|destac[aó]|lidera",
    r"innova(ción|dor|ndo)|moderniza|transforma(ción)?|digitaliza",
    r"éxito|exitoso|logr[oó]|consolida|fortalece",
    r"sostenible|responsab(le|ilidad)|compromiso|bienestar",
    r"patrocin(io|a|ador)|auspicia|apoya|respalda",
    r"atiende|activa\s+plan|gestiona|responde\s+ante|lidera\s+respuesta",
    r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)",
    r"avanza(r|do|da|ndo)?",
    r"benefici(a|o|ando|ar)",
    r"garantizar|seguridad|calidad|excelencia",
    r"eficien(te|cia)|oportunidad(es)?|potencial",
    r"satisfaccion|confianza|credibilidad",
]

NEG_BRAND_PATTERNS = [
    r"demanda(do|da|n)?|denuncia(do|da)?|sancion(ado|ada|es)?|multa(do|da)?",
    r"investiga(do|da|ción)|irregularidad|fraud[e]|escándalo",
    r"crisis|quiebra|default|pérdi(da|das)|déficit",
    r"ca[eí]da|baja(ron)?|desplom[eó]|retroceso",
    r"fall[aó]|interrupci[oó]n|suspende|cierra|cancel[aó]",
    r"filtraci[oó]n|hackeo|ataque\s+cibern|phishing",
    r"queja|reclamo|reclama(ción|ciones)|incumpl(e|imiento)",
    r"problema(s|tica)?|dificultad|deterioro",
    r"conflicto|disputa|huelga|boicot|protest[aó]",
    r"rechaz[aó]|neg[oó]|desmiente",
    r"riesgo|amenaza|alerta|alarma|preocupa(ción|nte)",
    r"retras(o|a|ar|ado)",
    r"negativ(o|a|os|as)",
]

CRISIS_KEYWORDS = re.compile(
    r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|"
    r"afectaciones|damnificados|tragedia|alerta\s+roja|zozobra)\b",
    re.IGNORECASE
)
RESPONSE_VERBS = re.compile(
    r"\b(atiend[eE]|activ[aó]|decret[aó]|respond[eió]|trabaj[aó]|"
    r"lider[aó]|enfrenta|gestion[aó]|declar[aó]|anunci[aó])\b",
    re.IGNORECASE
)

POS_COMPILED = [re.compile(rf"(?:{p})", re.IGNORECASE) for p in POS_BRAND_PATTERNS]
NEG_COMPILED = [re.compile(rf"(?:{p})", re.IGNORECASE) for p in NEG_BRAND_PATTERNS]


# ======================================
# Estilos CSS — Editorial v9
# ======================================
def load_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Source+Serif+4:opsz,wght@8..60,300;8..60,400;8..60,600;8..60,700;8..60,800;8..60,900&family=Inter:wght@300;400;500;600;700&display=swap');

    /* ===== GLOBAL ===== */
    .stApp {
        background-color: #FAF9F6 !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        color: #1A1A1A;
    }

    /* ===== MASTHEAD ===== */
    .editorial-masthead {
        text-align: center;
        padding: 2.5rem 2rem 1.8rem 2rem;
        margin-bottom: 0;
    }
    .editorial-masthead .edition-line {
        font-family: 'Inter', sans-serif;
        font-size: 0.65rem;
        font-weight: 500;
        letter-spacing: 5px;
        text-transform: uppercase;
        color: #999;
        margin-bottom: 0.6rem;
    }
    .editorial-masthead .logo-title {
        font-family: 'Source Serif 4', Georgia, 'Times New Roman', serif;
        font-size: 3rem;
        font-weight: 900;
        color: #111;
        letter-spacing: -1.5px;
        line-height: 1.05;
        margin: 0;
    }
    .editorial-masthead .logo-title span.accent {
        color: #B5302A;
    }
    .editorial-masthead .tagline {
        font-family: 'Inter', sans-serif;
        font-size: 0.72rem;
        font-weight: 400;
        color: #777;
        letter-spacing: 1.5px;
        margin-top: 0.5rem;
    }
    .masthead-rule {
        border: none;
        border-top: 1px solid #DDD;
        margin: 0 0 0.3rem 0;
    }
    .masthead-rule-thick {
        border: none;
        border-top: 3px solid #111;
        margin: 0 0 1.5rem 0;
    }

    /* ===== SECTION HEADERS ===== */
    .section-header {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 1.5rem;
        font-weight: 800;
        color: #111;
        padding-bottom: 0.4rem;
        margin: 2rem 0 0.3rem 0;
        letter-spacing: -0.5px;
        line-height: 1.2;
    }
    .section-subheader {
        font-family: 'Inter', sans-serif;
        font-size: 0.7rem;
        font-weight: 500;
        letter-spacing: 2.5px;
        text-transform: uppercase;
        color: #AAA;
        margin-bottom: 1.2rem;
        padding-bottom: 0.8rem;
        border-bottom: 1px solid #E5E5E0;
    }

    /* ===== METRIC CARDS ===== */
    .metric-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0;
        margin: 1.5rem 0;
        border-top: 2px solid #111;
        border-bottom: 1px solid #DDD;
    }
    .metric-item {
        flex: 1;
        min-width: 120px;
        padding: 1.3rem 1rem;
        text-align: center;
        border-right: 1px solid #E8E8E3;
    }
    .metric-item:last-child {
        border-right: none;
    }
    .metric-item .m-value {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 1.8rem;
        font-weight: 800;
        color: #111;
        line-height: 1;
        letter-spacing: -0.5px;
    }
    .metric-item .m-value.c-green { color: #2D7D46; }
    .metric-item .m-value.c-amber { color: #A67C00; }
    .metric-item .m-value.c-blue { color: #1A56DB; }
    .metric-item .m-value.c-red { color: #B5302A; }
    .metric-item .m-label {
        font-family: 'Inter', sans-serif;
        font-size: 0.6rem;
        font-weight: 600;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #AAA;
        margin-top: 0.45rem;
    }

    /* ===== COMPLETION CARD ===== */
    .completion-card {
        background: #fff;
        border: 1px solid #E5E5E0;
        border-left: 3px solid #2D7D46;
        padding: 1.5rem 1.8rem;
        margin: 1.5rem 0;
    }
    .completion-card .c-title {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 1.15rem;
        font-weight: 700;
        color: #111;
        margin-bottom: 0.2rem;
    }
    .completion-card .c-sub {
        font-family: 'Inter', sans-serif;
        font-size: 0.82rem;
        color: #666;
    }

    /* ===== BUTTONS ===== */
    .stButton > button {
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 1.5px !important;
        font-size: 0.72rem !important;
        border-radius: 0 !important;
        padding: 0.75rem 2rem !important;
        transition: all 0.15s ease !important;
        text-transform: uppercase !important;
    }
    .stButton > button[kind="primary"] {
        background-color: #111 !important;
        color: #fff !important;
        border: 1.5px solid #111 !important;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #B5302A !important;
        border-color: #B5302A !important;
    }
    .stButton > button[kind="secondary"] {
        background-color: transparent !important;
        color: #111 !important;
        border: 1.5px solid #111 !important;
    }
    .stButton > button[kind="secondary"]:hover {
        background-color: #111 !important;
        color: #fff !important;
    }

    /* ===== FORMS ===== */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        font-family: 'Inter', sans-serif !important;
        border-radius: 0 !important;
        border: 1px solid #D5D5D0 !important;
        font-size: 0.88rem !important;
        background: #fff !important;
    }
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #111 !important;
        box-shadow: 0 0 0 1px #111 !important;
    }
    .stSelectbox > div > div { border-radius: 0 !important; }

    /* ===== TABS ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 1px solid #DDD;
    }
    .stTabs [data-baseweb="tab"] {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 0.72rem;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #AAA;
        padding: 0.7rem 1.8rem;
        border-radius: 0;
        border-bottom: 2px solid transparent;
        margin-bottom: -1px;
    }
    .stTabs [aria-selected="true"] {
        color: #111 !important;
        border-bottom: 2px solid #B5302A !important;
        background: transparent !important;
    }

    /* ===== FILE UPLOADER ===== */
    .stFileUploader > div {
        border-radius: 0 !important;
        border: 1px dashed #CCC !important;
        background: #fff !important;
    }

    /* ===== PROGRESS ===== */
    .stProgress > div > div > div > div {
        background-color: #B5302A !important;
    }

    /* ===== ALERTS ===== */
    .stAlert { border-radius: 0 !important; }

    /* ===== INFO BOX ===== */
    .info-box {
        background: #F0EFEB;
        border-left: 3px solid #111;
        padding: 1rem 1.3rem;
        font-size: 0.85rem;
        color: #333;
        margin: 1rem 0;
        font-family: 'Inter', sans-serif;
    }
    .info-box strong { color: #111; }

    /* ===== FOOTER ===== */
    .editorial-footer {
        text-align: center;
        padding: 2rem 0 1rem 0;
        border-top: 1px solid #E5E5E0;
        margin-top: 3rem;
    }
    .editorial-footer p {
        font-family: 'Inter', sans-serif;
        font-size: 0.65rem;
        letter-spacing: 2.5px;
        text-transform: uppercase;
        color: #CCC;
        margin: 0;
    }

    /* ===== HIDE DEFAULTS ===== */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDataFrame { border-radius: 0 !important; }
    </style>
    """, unsafe_allow_html=True)


# ======================================
# Componentes UI
# ======================================
def render_masthead():
    today = datetime.datetime.now()
    months_es = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
                 7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
    date_str = f"{today.day} de {months_es[today.month]} de {today.year}"
    st.markdown(f"""
    <div class="editorial-masthead">
        <div class="edition-line">{date_str} · Edición Digital</div>
        <h1 class="logo-title">News<span class="accent">Analyzer</span></h1>
        <div class="tagline">Inteligencia Artificial Aplicada al Análisis de Medios</div>
    </div>
    <hr class="masthead-rule">
    <hr class="masthead-rule-thick">
    """, unsafe_allow_html=True)


def render_section_header(title: str, subtitle: str = ""):
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="section-subheader">{subtitle}</div>', unsafe_allow_html=True)


def render_metrics(metrics: List[Dict]):
    items = ""
    for m in metrics:
        accent = m.get("accent", "")
        items += f'<div class="metric-item"><div class="m-value {accent}">{m["value"]}</div><div class="m-label">{m["label"]}</div></div>'
    st.markdown(f'<div class="metric-row">{items}</div>', unsafe_allow_html=True)


def render_completion_card(title: str, subtitle: str):
    st.markdown(f"""
    <div class="completion-card">
        <div class="c-title">✓ {title}</div>
        <div class="c-sub">{subtitle}</div>
    </div>
    """, unsafe_allow_html=True)


def render_info_box(text: str):
    st.markdown(f'<div class="info-box">{text}</div>', unsafe_allow_html=True)


def render_footer():
    st.markdown("""
    <div class="editorial-footer">
        <p>v9.0 · NewsAnalyzer AI · Johnathan Cortés</p>
    </div>
    """, unsafe_allow_html=True)


# ======================================
# Wrapper OpenAI (v0.x y v1.x)
# ======================================
def _setup_openai_key():
    api_key = st.secrets.get("OPENAI_API_KEY", "")
    if not api_key:
        st.error("OPENAI_API_KEY no encontrada."); st.stop()
    if OPENAI_NEW_API:
        return api_key
    else:
        openai.api_key = api_key
        try: openai.aiosession.set(None)
        except: pass
        return api_key


def _count_usage(resp):
    if resp is None: return
    if OPENAI_NEW_API:
        u = getattr(resp, 'usage', None)
        if u:
            st.session_state['tokens_input'] += getattr(u, 'prompt_tokens', 0)
            st.session_state['tokens_output'] += getattr(u, 'completion_tokens', 0)
    else:
        u = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
        if u:
            st.session_state['tokens_input'] += (u.get('prompt_tokens', 0) if isinstance(u, dict) else getattr(u, 'prompt_tokens', 0))
            st.session_state['tokens_output'] += (u.get('completion_tokens', 0) if isinstance(u, dict) else getattr(u, 'completion_tokens', 0))


def _count_embedding_usage(resp):
    if resp is None: return
    if OPENAI_NEW_API:
        u = getattr(resp, 'usage', None)
        if u: st.session_state['tokens_embedding'] += getattr(u, 'total_tokens', 0)
    else:
        u = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
        if u: st.session_state['tokens_embedding'] += (u.get('total_tokens', 0) if isinstance(u, dict) else getattr(u, 'total_tokens', 0))


def call_with_retries(func, *args, max_retries=3, **kwargs):
    delay = 1
    for attempt in range(max_retries):
        try: return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay); delay *= 2


async def acall_with_retries(func, *args, max_retries=3, **kwargs):
    delay = 1
    for attempt in range(max_retries):
        try: return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            await asyncio.sleep(delay); delay *= 2


def _create_embedding_sync(input_texts):
    if OPENAI_NEW_API:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        return client.embeddings.create(input=input_texts, model=OPENAI_MODEL_EMBEDDING)
    else:
        return openai.Embedding.create(input=input_texts, model=OPENAI_MODEL_EMBEDDING)


def _create_chat_sync(messages, max_tokens=50, temperature=0.0, response_format=None):
    kw = {"model": OPENAI_MODEL_CLASIFICACION, "messages": messages,
          "max_tokens": max_tokens, "temperature": temperature}
    if response_format: kw["response_format"] = response_format
    if OPENAI_NEW_API:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        return client.chat.completions.create(**kw)
    else:
        return openai.ChatCompletion.create(**kw)


async def _create_chat_async(messages, max_tokens=50, temperature=0.0, response_format=None):
    kw = {"model": OPENAI_MODEL_CLASIFICACION, "messages": messages,
          "max_tokens": max_tokens, "temperature": temperature}
    if response_format: kw["response_format"] = response_format
    if OPENAI_NEW_API:
        client = AsyncOpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        return await client.chat.completions.create(**kw)
    else:
        return await openai.ChatCompletion.acreate(**kw)


def _get_chat_content(resp):
    if OPENAI_NEW_API:
        return resp.choices[0].message.content.strip()
    else:
        msg = resp.choices[0].message
        return (msg.content if hasattr(msg, 'content') else resp["choices"][0]["message"]["content"]).strip()


def _get_embedding_data(resp):
    if OPENAI_NEW_API: return resp.data
    else: return resp["data"] if isinstance(resp, dict) else resp.data


# ======================================
# Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    render_masthead()
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        render_section_header("Acceso al Portal")
        with st.form("password_form"):
            password = st.text_input("Contraseña", type="password", label_visibility="collapsed", placeholder="Ingrese su contraseña")
            if st.form_submit_button("Ingresar", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True; st.rerun()
                else: st.error("Contraseña incorrecta")
    return False


def norm_key(text):
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))


def string_norm_label(s):
    if not s: return ""
    s = unidecode(s.lower()); s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(t for t in s.split() if t not in STOPWORDS_ES)


def normalize_title_for_comparison(title):
    if not isinstance(title, str): return ""
    tmp = re.split(r"\s*[:|\-|]\s*", title, 1)
    return re.sub(r"\W+", " ", tmp[0] if tmp else title).lower().strip()


def clean_title_for_output(title):
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()


def corregir_texto(text):
    if not isinstance(text, str): return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    m = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if m: text = text[m.start():]
    if text and not text.endswith("..."): text = text.rstrip(".") + "..."
    return text


def normalizar_tipo_medio(tipo_raw):
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {"fm":"Radio","am":"Radio","radio":"Radio","aire":"Televisión","cable":"Televisión",
               "tv":"Televisión","television":"Televisión","televisión":"Televisión",
               "senal abierta":"Televisión","señal abierta":"Televisión","diario":"Prensa",
               "prensa":"Prensa","revista":"Revista","revistas":"Revista","online":"Internet",
               "internet":"Internet","digital":"Internet","web":"Internet"}
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")


def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink: return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        m = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if m: return {"value": "Link", "url": m.group(1)}
    return {"value": cell.value, "url": None}


# ======================================
# Limpieza de temas
# ======================================
def limpiar_tema(tema):
    if not tema: return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    tema = re.sub(r'[.,:;!?]+$', '', tema)
    if not tema: return "Sin tema"
    tema = tema[0].upper() + tema[1:]
    invalid_trailing = {"en","de","del","la","el","y","o","con","sin","por","para","sobre","al","los","las","un","una","su","sus"}
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_trailing: palabras.pop()
    if len(palabras) > 6: palabras = palabras[:6]
    tema = " ".join(palabras)
    return tema if tema else "Sin tema"


def limpiar_tema_geografico(tema, marca, aliases):
    if not tema: return "Sin tema"
    tema_lower = tema.lower()
    all_brand = [marca.lower()] + [a.lower() for a in aliases if a]
    for bn in all_brand:
        tema_lower = re.sub(rf'\b{re.escape(bn)}\b', '', tema_lower, flags=re.IGNORECASE)
        tema_lower = re.sub(rf'\b{re.escape(unidecode(bn))}\b', '', tema_lower, flags=re.IGNORECASE)
    for c in CIUDADES_COLOMBIA: tema_lower = re.sub(rf'\b{re.escape(c)}\b', '', tema_lower, flags=re.IGNORECASE)
    for g in GENTILICIOS_COLOMBIA: tema_lower = re.sub(rf'\b{re.escape(g)}\b', '', tema_lower, flags=re.IGNORECASE)
    for f in ["en colombia","de colombia","del pais","en el pais","nacional","territorio nacional"]:
        tema_lower = re.sub(rf'\b{re.escape(f)}\b', '', tema_lower, flags=re.IGNORECASE)
    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
    if not palabras: return "Sin tema"
    tl = " ".join(palabras)
    tl = tl[0].upper() + tl[1:]
    return limpiar_tema(tl)


# ======================================
# Embeddings con deduplicación
# ======================================
def get_embeddings_batch(textos, batch_size=100):
    if not textos: return []
    t2i = defaultdict(list)
    for i, t in enumerate(textos): t2i[(t or "")[:2000]].append(i)
    ut = list(t2i.keys())
    ue = {}
    for i in range(0, len(ut), batch_size):
        batch = ut[i:i+batch_size]
        bc = [t if t.strip() else " " for t in batch]
        try:
            resp = call_with_retries(_create_embedding_sync, bc)
            _count_embedding_usage(resp)
            ed = _get_embedding_data(resp)
            for j, e in enumerate(ed):
                ue[batch[j]] = e.embedding if hasattr(e, 'embedding') else e["embedding"]
        except:
            for j, texto in enumerate(batch):
                try:
                    resp = _create_embedding_sync([texto if texto.strip() else " "])
                    _count_embedding_usage(resp)
                    ed = _get_embedding_data(resp)
                    ue[texto] = ed[0].embedding if hasattr(ed[0], 'embedding') else ed[0]["embedding"]
                except: ue[texto] = None
    res = [None] * len(textos)
    for tk, indices in t2i.items():
        emb = ue.get(tk)
        for idx in indices: res[idx] = emb
    return res


# ======================================
# DSU
# ======================================
class DSU:
    def __init__(self, n):
        self.parent = list(range(n)); self.rank = [0]*n
    def find(self, i):
        while self.parent[i] != i: self.parent[i] = self.parent[self.parent[i]]; i = self.parent[i]
        return i
    def union(self, i, j):
        ri, rj = self.find(i), self.find(j)
        if ri == rj: return
        if self.rank[ri] < self.rank[rj]: ri, rj = rj, ri
        self.parent[rj] = ri
        if self.rank[ri] == self.rank[rj]: self.rank[ri] += 1
    def components(self):
        comp = defaultdict(list)
        for i in range(len(self.parent)): comp[self.find(i)].append(i)
        return dict(comp)


# ======================================
# Agrupación genérica
# ======================================
def agrupar_textos_similares(textos, umbral):
    if not textos or len(textos) < 2: return {}
    embs = get_embeddings_batch(textos)
    vi = [i for i, e in enumerate(embs) if e is not None]
    if len(vi) < 2: return {}
    em = np.array([embs[i] for i in vi])
    dm = np.clip(1 - cosine_similarity(em), 0, 2); np.fill_diagonal(dm, 0)
    cl = AgglomerativeClustering(n_clusters=None, distance_threshold=1-umbral, metric="precomputed", linkage="average").fit(dm)
    g = defaultdict(list)
    for i, l in enumerate(cl.labels_): g[l].append(vi[i])
    return {gid: grp for gid, grp in enumerate(g.values()) if len(grp) >= 2}


def agrupar_por_titulo_similar(titulos):
    gid, grupos, used = 0, {}, set()
    nt = [normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(nt)):
        if i in used or not nt[i]: continue
        ga = [i]; used.add(i)
        for j in range(i+1, len(nt)):
            if j in used or not nt[j]: continue
            if SequenceMatcher(None, nt[i], nt[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                ga.append(j); used.add(j)
        if len(ga) >= 2: grupos[gid] = ga; gid += 1
    return grupos


def seleccionar_representante(indices, textos):
    if len(indices) == 1: return indices[0], textos[indices[0]]
    st_t = [textos[i] for i in indices]
    embs = get_embeddings_batch(st_t)
    ve, vi = [], []
    for k, e in enumerate(embs):
        if e is not None: ve.append(e); vi.append(indices[k])
    if not ve: return indices[0], textos[indices[0]]
    M = np.array(ve); c = M.mean(axis=0, keepdims=True)
    s = cosine_similarity(M, c).reshape(-1)
    b = int(np.argmax(s))
    return vi[b], textos[vi[b]]


# ======================================
# Extracción de marca desde PKL
# ======================================
def extraer_marca_de_pkl(pkl_file):
    try:
        pkl_file.seek(0); pipeline = joblib.load(pkl_file); pkl_file.seek(0)
        for a in ['marca','brand','client','cliente','target_name','brand_name','client_name']:
            if hasattr(pipeline, a):
                v = getattr(pipeline, a)
                if isinstance(v, str) and v.strip(): return v.strip()
        if hasattr(pipeline, 'steps'):
            for sn, so in pipeline.steps:
                for a in ['marca','brand','client','cliente']:
                    if hasattr(so, a):
                        v = getattr(so, a)
                        if isinstance(v, str) and v.strip(): return v.strip()
        if hasattr(pipeline, 'metadata'):
            m = pipeline.metadata
            if isinstance(m, dict):
                for k in ['marca','brand','client','cliente']:
                    if k in m and isinstance(m[k], str): return m[k].strip()
        return None
    except: return None


# ======================================
# CLASIFICADOR DE TONO v8
# ======================================
class ClasificadorTonoV8:
    def __init__(self, marca, aliases):
        self.marca = marca; self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex(marca, aliases)

    def _build_brand_regex(self, marca, aliases):
        names = [marca] + [a for a in (aliases or []) if a]
        pats = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        if not pats: return re.compile(r"(a^b)")
        return re.compile(r"\b(" + "|".join(pats) + r")\b", re.IGNORECASE)

    def _extract_brand_contexts(self, texto):
        tl = unidecode(texto.lower())
        matches = list(self.brand_pattern.finditer(tl))
        if not matches: return [texto[:400]]
        ctx = []; oraciones = re.split(r'(?<=[.!?])\s+', texto)
        for match in matches[:5]:
            pos = match.start()
            snip = tl[max(0,pos-30):min(len(tl),pos+80)]
            ha = bool(re.search(r'(lanz|denunci|sancion|innov|crisis|acuerd|alianz|premi|multa|demand|creci|pérdi|gananci)', snip))
            w = MAX_CONTEXT_WINDOW if ha else MIN_CONTEXT_WINDOW
            cc = 0; rel = []
            for s in oraciones:
                ss, se = cc, cc+len(s)
                if ss <= pos+w and se >= max(0,pos-w): rel.append(s.strip())
                cc = se+1
            if rel: ctx.append(" ".join(rel))
            else:
                st2 = max(0,pos-w); en = min(len(texto),pos+w)
                while en < len(texto) and texto[en] not in '.!?\n': en += 1
                ctx.append(texto[st2:en+1].strip())
        seen = set(); uniq = []
        for c in ctx:
            k = c[:100]
            if k not in seen: seen.add(k); uniq.append(c)
        return uniq[:4]

    def _analyze_brand_sentiment_rules(self, contextos):
        ps, ns = 0, 0
        for contexto in contextos:
            t = unidecode(contexto.lower())
            bm = self.brand_pattern.search(t)
            if not bm: continue
            bp = bm.start()
            pb = t[max(0,bp-40):bp]
            hn = bool(re.search(r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente)\b', pb))
            post = t[bp:min(len(t),bp+150)]
            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(post): ps += 4; continue
            for p in POS_COMPILED:
                if p.search(post): ns += 1 if hn else 0; ps += 0 if hn else 1.5
            for p in NEG_COMPILED:
                if p.search(post): ps += 1 if hn else 0; ns += 0 if hn else 1.5
            pre = t[max(0,bp-100):bp]
            for p in NEG_COMPILED:
                if p.search(pre): ns += 1
            for p in POS_COMPILED:
                if p.search(pre): ps += 1
        if ps >= 3 and ps > ns*2: return "Positivo"
        elif ns >= 3 and ns > ps*2: return "Negativo"
        return None

    async def _llm_classify_tone(self, contextos, semaphore):
        async with semaphore:
            al = ", ".join(self.aliases) if self.aliases else "ninguno"
            ct = "\n---\n".join(c[:500] for c in contextos[:3])
            prompt = f"""Eres un analista de reputación corporativa. Analiza el SENTIMIENTO específicamente hacia '{self.marca}' (alias: {al}).

Clasifica SOLO según cómo afecta la imagen de '{self.marca}', NO el tono general.

Criterios:
- Positivo: logros, lanzamientos, reconocimientos, alianzas, crecimiento, respuesta a crisis, innovación
- Negativo: críticas, sanciones, multas, demandas, pérdidas, fallos, quejas, escándalos
- Neutro: menciones informativas sin juicio claro

Ejemplos:
- "X lanzó su nueva plataforma digital" → Positivo
- "X fue multada por la SIC" → Negativo
- "En la reunión participó X" → Neutro

Fragmentos:
---
{ct}
---

JSON: {{"tono":"Positivo"|"Negativo"|"Neutro"}}"""
            try:
                resp = await acall_with_retries(_create_chat_async, messages=[{"role":"user","content":prompt}],
                                                max_tokens=30, temperature=0.0, response_format={"type":"json_object"})
                _count_usage(resp)
                data = json.loads(_get_chat_content(resp))
                tono = str(data.get("tono","Neutro")).strip().title()
                return tono if tono in ["Positivo","Negativo","Neutro"] else "Neutro"
            except: return "Neutro"

    async def _classify_group(self, texto_rep, semaphore):
        ctx = self._extract_brand_contexts(texto_rep)
        tr = self._analyze_brand_sentiment_rules(ctx)
        if tr: return tr
        return await self._llm_classify_tone(ctx, semaphore)

    async def procesar_lote_async(self, textos_concat, progress_bar, resumen_puro, titulos_puros):
        textos = textos_concat.tolist(); n = len(textos)
        progress_bar.progress(0.05, text="Agrupando noticias similares…")
        dsu = DSU(n)
        for _, idxs in agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO).items():
            for j in idxs[1:]: dsu.union(idxs[0], j)
        for _, idxs in agrupar_por_titulo_similar(titulos_puros.tolist()).items():
            for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = dsu.components()
        reps = {}
        for cid, idxs in comp.items():
            _, rt = seleccionar_representante(idxs, textos); reps[cid] = rt
        progress_bar.progress(0.15, text=f"{len(comp)} grupos identificados")
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        cids = list(reps.keys())
        tasks = [self._classify_group(reps[cid], sem) for cid in cids]
        rpg = {}; done = 0
        for ci, coro in enumerate(asyncio.as_completed(tasks)):
            tono = await coro; rpg[cids[ci]] = tono; done += 1
            if done % 10 == 0 or done == len(tasks):
                progress_bar.progress(0.15+0.80*done/len(tasks), text=f"Tono: {done}/{len(tasks)}")
        rf = [None]*n
        for cid, idxs in comp.items():
            t = rpg.get(cid, "Neutro")
            for i in idxs: rf[i] = {"tono": t}
        progress_bar.progress(1.0, text="Tono completado")
        return rf


def analizar_tono_con_pkl(textos, pkl_file):
    try:
        pkl_file.seek(0); pipeline = joblib.load(pkl_file)
        preds = pipeline.predict(textos)
        TM = {1:"Positivo","1":"Positivo",0:"Neutro","0":"Neutro",-1:"Negativo","-1":"Negativo",
              "Positivo":"Positivo","Negativo":"Negativo","Neutro":"Neutro",
              "positivo":"Positivo","negativo":"Negativo","neutro":"Neutro"}
        return [{"tono": TM.get(p, str(p).title())} for p in preds]
    except Exception as e:
        st.error(f"Error con pipeline_sentimiento.pkl: {e}"); return None


# ======================================
# CLASIFICADOR DE SUBTEMAS v9 — Lenguaje natural
# ======================================
class ClasificadorSubtemaV9:
    def __init__(self, marca, aliases):
        self.marca = marca; self.aliases = aliases or []; self.cache = {}

    def _preagrupar(self, textos, titulos, resumenes):
        def nr(t):
            if not t: return ""
            t = unidecode(str(t).lower()); t = re.sub(r'[^a-z0-9\s]','',t)
            return ' '.join(t.split()[:40])
        thi, rhi = defaultdict(list), defaultdict(list)
        for i, titulo in enumerate(titulos):
            n = nr(titulo)
            if n: thi[hashlib.md5(n.encode()).hexdigest()].append(i)
        for i, resumen in enumerate(resumenes):
            n = nr(resumen)
            if n: rhi[hashlib.md5(n[:150].encode()).hexdigest()].append(i)
        g, u, gid = {}, set(), 0
        for indices in thi.values():
            if len(indices) >= 2:
                nv = [i for i in indices if i not in u]
                if len(nv) >= 2: g[gid] = nv; u.update(nv); gid += 1
        for indices in rhi.values():
            nv = [i for i in indices if i not in u]
            if len(nv) >= 2: g[gid] = nv; u.update(nv); gid += 1
        return g

    def _cluster_lotes(self, textos, titulos, indices):
        if len(indices) < 2: return {}
        BS, gf, go = 500, {}, 0
        for bs in range(0, len(indices), BS):
            bi = indices[bs:bs+BS]
            bt = [f"{titulos[i][:150]} {textos[i][:1200]}" for i in bi]
            embs = get_embeddings_batch(bt)
            ve, vi = [], []
            for k, e in enumerate(embs):
                if e is not None: ve.append(e); vi.append(bi[k])
            if len(ve) < 2: continue
            sm = cosine_similarity(np.array(ve))
            dm = np.clip(1-sm, 0, 2); np.fill_diagonal(dm, 0)
            cl = AgglomerativeClustering(n_clusters=None, distance_threshold=0.20, metric='precomputed', linkage='average').fit(dm)
            gg = defaultdict(list)
            for i, l in enumerate(cl.labels_): gg[l].append(vi[i])
            for l, idxs in gg.items():
                if len(idxs) >= 2: gf[go+l] = idxs
            go += (max(cl.labels_)+1) if len(cl.labels_) > 0 else 0
        return gf

    def _generar_subtema(self, textos_m, titulos_m):
        tn = sorted(normalize_title_for_comparison(t) for t in titulos_m[:5])
        ck = hashlib.md5("|".join(tn).encode()).hexdigest()
        if ck in self.cache: return self.cache[ck]
        aw = []
        for t in titulos_m[:5]:
            aw.extend([w for w in string_norm_label(t).split() if w not in STOPWORDS_ES and len(w)>3])
        kw = [w for w, _ in Counter(aw).most_common(8)]
        td = "\n".join(f"- {t[:120]}" for t in titulos_m[:5])

        prompt = f"""Genera una ETIQUETA TEMÁTICA en español, con lenguaje natural y fluido, de 3 a 6 palabras, para agrupar estas noticias.

TÍTULOS:
{td}

PALABRAS CLAVE: {', '.join(kw)}

REGLAS ESTRICTAS:
1. NO incluir el nombre '{self.marca}' ni sus alias
2. NO incluir ciudades, países ni gentilicios
3. NO usar verbos conjugados
4. INCLUIR artículos y preposiciones naturales (de, del, en, para, al, la, el) para que suene fluido
5. Ser ESPECÍFICO al evento o tema concreto
6. Entre 3 y 6 palabras obligatoriamente

BUENOS EJEMPLOS:
- "Protección del manglar costero"
- "Acceso a energía digna"
- "Expansión de la red comercial"
- "Resultados financieros del trimestre"
- "Alianza para la salud rural"
- "Innovación en pagos digitales"

MALOS EJEMPLOS (no hacer):
- "Protección manglar" (falta preposición, suena robótico)
- "Acceso energía digna" (falta preposición)
- "Noticias varias" (genérico)
- "Actividades" (vago)
- "Gestión empresa" (genérico y tiene nombre)

Responde SOLO en JSON: {{"subtema": "..."}}"""

        try:
            resp = call_with_retries(_create_chat_sync, messages=[{"role":"user","content":prompt}],
                                     max_tokens=40, temperature=0.05, response_format={"type":"json_object"})
            _count_usage(resp)
            data = json.loads(_get_chat_content(resp))
            subtema = data.get("subtema", "Varios")
            subtema = limpiar_tema_geografico(limpiar_tema(subtema), self.marca, self.aliases)
            gen = {"sin tema","varios","noticias","actividad","gestión","información","nota","noticia","actividades"}
            if subtema.lower().strip() in gen:
                subtema = " ".join(kw[:4]).title() if kw else "Actividad corporativa"
            self.cache[ck] = subtema; return subtema
        except:
            subtema = " ".join(kw[:4]).title() if kw else "Actividad corporativa"
            self.cache[ck] = subtema; return subtema

    def _fusionar_similares(self, subtemas, textos):
        df_t = pd.DataFrame({'label': subtemas, 'text': textos})
        ul = list(df_t['label'].unique())
        if len(ul) < 2: return subtemas
        el = get_embeddings_batch(ul); te = get_embeddings_batch(textos)
        cc, vl, vle = [], [], []
        for i, label in enumerate(ul):
            if el[i] is None: continue
            idxs = df_t.index[df_t['label']==label].tolist()[:40]
            vecs = [te[j] for j in idxs if te[j] is not None]
            cc.append(np.mean(vecs, axis=0) if vecs else el[i])
            vl.append(label); vle.append(el[i])
        if len(vl) < 2: return subtemas
        ml, mc = np.array(vle), np.array(cc)
        sc = 0.4*cosine_similarity(ml) + 0.6*cosine_similarity(mc)
        dm = np.clip(1-sc, 0, 2); np.fill_diagonal(dm, 0)
        cl = AgglomerativeClustering(n_clusters=None, distance_threshold=1-UMBRAL_FUSION_SUBTEMAS, metric='precomputed', linkage='average').fit(dm)
        mf = {}
        for cid in set(cl.labels_):
            ci = [i for i, x in enumerate(cl.labels_) if x==cid]
            lic = [vl[i] for i in ci]
            counts = {l: sum(1 for s in subtemas if s==l) for l in lic}
            rep = max(lic, key=lambda x: (counts.get(x,0), -len(x)))
            for l in lic: mf[l] = rep
        return [mf.get(l, l) for l in subtemas]

    def procesar_lote(self, textos_concat, progress_bar, resumen_puro, titulos_puros):
        textos = textos_concat.tolist(); titulos = titulos_puros.tolist(); resumenes = resumen_puro.tolist(); n = len(textos)
        progress_bar.progress(0.05, "Pre-agrupando noticias…")
        gr = self._preagrupar(textos, titulos, resumenes)
        dsu = DSU(n)
        for idxs in gr.values():
            for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = dsu.components()
        sueltos = [idxs[0] for idxs in comp.values() if len(idxs)==1]
        progress_bar.progress(0.15, f"{len(gr)} grupos rápidos")
        if len(sueltos) > 1:
            gc2 = self._cluster_lotes(textos, titulos, sueltos)
            for idxs in gc2.values():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = dsu.components(); tg = len(comp)
        progress_bar.progress(0.30, f"Etiquetando {tg} grupos…")
        ms = {}
        for k, (lid, idxs) in enumerate(comp.items()):
            if k % 15 == 0: progress_bar.progress(0.30+0.40*k/max(tg,1), f"Etiquetando: {k}/{tg}")
            subtema = self._generar_subtema([textos[i] for i in idxs], [titulos[i] for i in idxs])
            for i in idxs: ms[i] = subtema
        sb = [ms.get(i, "Varios") for i in range(n)]; nb = len(set(sb))
        progress_bar.progress(0.75, "Fusionando subtemas similares…")
        sf = self._fusionar_similares(sb, textos); nf = len(set(sf))
        st.info(f"Subtemas: {nb} → {nf}")
        progress_bar.progress(1.0, "Subtemas listos")
        return sf


# ======================================
# Coherencia Tema-Subtema
# ======================================
def unificar_temas_por_subtema(temas, subtemas):
    s2t = defaultdict(list)
    for i, sub in enumerate(subtemas): s2t[sub].append(temas[i])
    s2tf = {}
    for sub, tl in s2t.items(): s2tf[sub] = Counter(tl).most_common(1)[0][0]
    return [s2tf[sub] for sub in subtemas]


def unificar_temas_por_similitud(temas, subtemas, textos):
    temas = unificar_temas_por_subtema(temas, subtemas)
    df_w = pd.DataFrame({'tema': temas, 'subtema': subtemas, 'texto': textos})
    us = list(df_w['subtema'].unique())
    if len(us) < 2: return temas
    te = get_embeddings_batch(textos)
    cc, vs = [], []
    for sub in us:
        idxs = df_w.index[df_w['subtema']==sub].tolist()[:30]
        vecs = [te[j] for j in idxs if te[j] is not None]
        if vecs: cc.append(np.mean(vecs, axis=0)); vs.append(sub)
    if len(vs) < 2: return temas
    m = np.array(cc); sm = cosine_similarity(m)
    dm = np.clip(1-sm, 0, 2); np.fill_diagonal(dm, 0)
    cl = AgglomerativeClustering(n_clusters=None, distance_threshold=1-UMBRAL_FUSION_TEMAS, metric='precomputed', linkage='average').fit(dm)
    mstu = {}
    for cid in set(cl.labels_):
        csubs = [vs[i] for i, l in enumerate(cl.labels_) if l==cid]
        atc = []
        for sub in csubs: atc.extend(df_w[df_w['subtema']==sub]['tema'].tolist())
        tg = Counter(atc).most_common(1)[0][0] if atc else csubs[0]
        for sub in csubs: mstu[sub] = tg
    return [mstu.get(sub, temas[i]) for i, sub in enumerate(subtemas)]


def consolidar_subtemas_en_temas(subtemas, textos, p_bar, marca="", aliases=None):
    aliases = aliases or []
    p_bar.progress(0.1, text="Analizando estructura de temas…")
    df_t = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    us = list(df_t['subtema'].unique())
    if len(us) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "Temas finalizados"); return subtemas
    el = get_embeddings_batch(us); te = get_embeddings_batch(textos)
    cc, vs, vle = [], [], []
    for i, subt in enumerate(us):
        if el[i] is None: continue
        idxs = df_t.index[df_t['subtema']==subt].tolist()[:30]
        vecs = [te[j] for j in idxs if te[j] is not None]
        cc.append(np.mean(vecs, axis=0) if vecs else el[i])
        vs.append(subt); vle.append(el[i])
    if len(vs) < 2: p_bar.progress(1.0, "Temas finalizados"); return subtemas
    ml, mc = np.array(vle), np.array(cc)
    sf = 0.35*cosine_similarity(ml) + 0.65*cosine_similarity(mc)
    df2 = np.clip(1-sf, 0, 2); np.fill_diagonal(df2, 0)
    nct = min(NUM_TEMAS_PRINCIPALES, len(vs))
    cl = AgglomerativeClustering(n_clusters=nct, metric='precomputed', linkage='average').fit(df2)
    ccc = defaultdict(list)
    for i, label in enumerate(cl.labels_): ccc[label].append(vs[i])
    p_bar.progress(0.5, text="Nombrando categorías…")
    mtf = {}
    for cid, ls in ccc.items():
        ss = ", ".join(ls[:8]); ts = []
        for subt in ls[:3]:
            rs = df_t[df_t['subtema']==subt].head(2)
            for _, r in rs.iterrows(): ts.append(str(r['texto']).split('.')[0][:100])
        ctx = "\n".join(f"- {t}" for t in ts[:5])
        prompt = f"""Genera un nombre de categoría temática en español, fluido y natural, de 2 a 4 palabras.

Subtemas a agrupar: {ss}

Noticias ejemplo:
{ctx}

Reglas:
1. Máximo 4 palabras
2. Incluir artículos y preposiciones naturales si mejora la lectura
3. No usar verbos conjugados
4. No incluir nombres de empresas
5. Ser descriptivo y preciso

Buenos ejemplos: "Resultados financieros", "Expansión comercial", "Innovación digital", "Responsabilidad social", "Regulación del sector"
Malos ejemplos: "Varios", "Noticias", "Actividades", "Gestión general"

Responde SOLO el nombre, sin comillas ni JSON."""
        try:
            resp = call_with_retries(_create_chat_sync, messages=[{"role":"user","content":prompt}], max_tokens=20, temperature=0.1)
            _count_usage(resp)
            nt = limpiar_tema(_get_chat_content(resp).replace('"','').replace('.',''))
            if not nt or nt.lower() in {"sin tema","varios","noticias"}: nt = ls[0]
        except: nt = ls[0]
        if marca: nt = limpiar_tema_geografico(nt, marca, aliases)
        for subt in ls: mtf[subt] = nt
    tf = [mtf.get(subt, subt) for subt in subtemas]
    n_t = len(set(tf))
    st.info(f"Temas consolidados en {n_t} categorías")
    p_bar.progress(1.0, "Temas finalizados")
    return tf


def analizar_temas_con_pkl(textos, pkl_file):
    try:
        pkl_file.seek(0); pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error con pipeline_tema.pkl: {e}"); return None


# ======================================
# Detección de Duplicados
# ======================================
def detectar_duplicados_avanzado(rows, key_map):
    pr = deepcopy(rows); sou, sb = {}, {}; otb = defaultdict(list)
    for i, row in enumerate(pr):
        if row.get("is_duplicate"): continue
        tipo = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio"))))
        mn = norm_key(row.get(key_map.get("menciones"))); med = norm_key(row.get(key_map.get("medio")))
        if tipo == "Internet":
            li = row.get(key_map.get("link_nota"), {}); url = li.get("url") if isinstance(li, dict) else None
            if url and mn:
                key = (url, mn)
                if key in sou:
                    row["is_duplicate"] = True; row["idduplicada"] = pr[sou[key]].get(key_map.get("idnoticia"), ""); continue
                else: sou[key] = i
            if med and mn: otb[(med, mn)].append(i)
        elif tipo in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mn and med and hora:
                key = (mn, med, hora)
                if key in sb: row["is_duplicate"] = True; row["idduplicada"] = pr[sb[key]].get(key_map.get("idnoticia"), "")
                else: sb[key] = i
    for indices in otb.values():
        if len(indices) < 2: continue
        for i in range(len(indices)):
            for j in range(i+1, len(indices)):
                i1, i2 = indices[i], indices[j]
                if pr[i1].get("is_duplicate") or pr[i2].get("is_duplicate"): continue
                t1 = normalize_title_for_comparison(pr[i1].get(key_map.get("titulo")))
                t2 = normalize_title_for_comparison(pr[i2].get(key_map.get("titulo")))
                if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    if len(t1) < len(t2): pr[i1]["is_duplicate"]=True; pr[i1]["idduplicada"]=pr[i2].get(key_map.get("idnoticia"),"")
                    else: pr[i2]["is_duplicate"]=True; pr[i2]["idduplicada"]=pr[i1].get(key_map.get("idnoticia"),"")
    return pr


# ======================================
# Procesamiento del Dossier
# ======================================
def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]; nk = [norm_key(h) for h in headers]
    km = {n: n for n in nk}
    km.update({"titulo":norm_key("Titulo"),"resumen":norm_key("Resumen - Aclaracion"),
               "menciones":norm_key("Menciones - Empresa"),"medio":norm_key("Medio"),
               "tonoiai":norm_key("Tono IA"),"tema":norm_key("Tema"),"subtema":norm_key("Subtema"),
               "idnoticia":norm_key("ID Noticia"),"idduplicada":norm_key("ID duplicada"),
               "tipodemedio":norm_key("Tipo de Medio"),"hora":norm_key("Hora"),
               "link_nota":norm_key("Link Nota"),"link_streaming":norm_key("Link (Streaming - Imagen)"),
               "region":norm_key("Region")})
    rows, sr = [], []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({nk[i]: c for i, c in enumerate(row) if i < len(nk)})
    for rc in rows:
        base = {}
        for k, v in rc.items():
            if k in [km["link_nota"], km["link_streaming"]]: base[k] = extract_link(v)
            else: base[k] = v.value
        if km.get("tipodemedio") in base: base[km["tipodemedio"]] = normalizar_tipo_medio(base.get(km["tipodemedio"]))
        ml = [m.strip() for m in str(base.get(km["menciones"], "")).split(";") if m.strip()]
        for m in ml or [None]:
            new = deepcopy(base)
            if m: new[km["menciones"]] = m
            sr.append(new)
    for idx, row in enumerate(sr): row.update({"original_index": idx, "is_duplicate": False})
    pr = detectar_duplicados_avanzado(sr, km)
    for row in pr:
        if row["is_duplicate"]: row.update({km["tonoiai"]:"Duplicada", km["tema"]:"Duplicada", km["subtema"]:"Duplicada"})
    return pr, km


def fix_links_by_media_type(row, km):
    tk, lnk, lsk = km.get("tipodemedio"), km.get("link_nota"), km.get("link_streaming")
    if not (tk and lnk and lsk): return
    tipo = row.get(tk, "")
    ln = row.get(lnk) or {"value":"","url":None}; ls = row.get(lsk) or {"value":"","url":None}
    hu = lambda x: isinstance(x, dict) and bool(x.get("url"))
    if tipo in ["Radio","Televisión"]: row[lsk] = {"value":"","url":None}
    elif tipo == "Internet": row[lnk], row[lsk] = ls, ln
    elif tipo in ["Prensa","Revista"]:
        if not hu(ln) and hu(ls): row[lnk] = ls
        row[lsk] = {"value":"","url":None}


# ======================================
# Generación de Excel
# ======================================
def generate_output_excel(apr, km):
    wb = Workbook(); ws = wb.active; ws.title = "Resultado"
    fo = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Region","Seccion - Programa","Titulo",
          "Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Audiencia",
          "Tier","Tono","Tono IA","Tema","Subtema","Link Nota","Resumen - Aclaracion",
          "Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    nc = {"ID Noticia","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia"}
    ws.append(fo)
    ls = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    if "Hyperlink_Custom" not in wb.style_names: wb.add_named_style(ls)
    for rd in apr:
        tk = km.get("titulo")
        if tk and tk in rd: rd[tk] = clean_title_for_output(rd.get(tk))
        rk = km.get("resumen")
        if rk and rk in rd: rd[rk] = corregir_texto(rd.get(rk))
        rta, lta = [], {}
        for ci, h in enumerate(fo, 1):
            nkh = norm_key(h); dk = km.get(nkh, nkh); val = rd.get(dk); cv = None
            if h in nc:
                try: cv = float(val) if val is not None and str(val).strip() != "" else None
                except: cv = str(val) if val is not None else None
            elif isinstance(val, dict) and "url" in val:
                cv = val.get("value","Link")
                if val.get("url"): lta[ci] = val["url"]
            elif val is not None: cv = str(val)
            rta.append(cv)
        ws.append(rta)
        for ci, url in lta.items():
            cell = ws.cell(row=ws.max_row, column=ci); cell.hyperlink = url; cell.style = "Hyperlink_Custom"
    output = io.BytesIO(); wb.save(output)
    return output.getvalue()


# ======================================
# Proceso principal
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file,
                                  brand_name, brand_aliases,
                                  tono_pkl_file, tema_pkl_file, analysis_mode):
    st.session_state['tokens_input'] = 0; st.session_state['tokens_output'] = 0; st.session_state['tokens_embedding'] = 0
    start_time = time.time()
    if "API" in analysis_mode: _setup_openai_key()

    with st.status("**Paso 1 de 5** — Limpieza y detección de duplicados", expanded=True) as s:
        apr, km = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="✓ Paso 1 — Limpieza completada", state="complete")

    with st.status("**Paso 2 de 5** — Mapeos y normalización", expanded=True) as s:
        dfr = pd.read_excel(region_file)
        rm = {str(k).lower().strip(): v for k, v in pd.Series(dfr.iloc[:,1].values, index=dfr.iloc[:,0]).to_dict().items()}
        dfi = pd.read_excel(internet_file)
        im = {str(k).lower().strip(): v for k, v in pd.Series(dfi.iloc[:,1].values, index=dfi.iloc[:,0]).to_dict().items()}
        for row in apr:
            omk = str(row.get(km.get("medio"),"")).lower().strip()
            row[km.get("region")] = rm.get(omk, "N/A")
            if omk in im: row[km.get("medio")] = im[omk]; row[km.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, km)
        s.update(label="✓ Paso 2 — Mapeos aplicados", state="complete")

    gc.collect()
    rta = [r for r in apr if not r.get("is_duplicate")]

    if rta:
        df = pd.DataFrame(rta)
        df["resumen_api"] = df[km["titulo"]].fillna("").astype(str) + ". " + df[km["resumen"]].fillna("").astype(str)

        with st.status("**Paso 3 de 5** — Análisis de tono", expanded=True) as s:
            pb = st.progress(0)
            if ("PKL" in analysis_mode) and tono_pkl_file:
                rt = analizar_tono_con_pkl(df["resumen_api"].tolist(), tono_pkl_file)
                if rt is None: st.stop()
                pb.progress(1.0, "Tono (pkl) completado")
            elif "API" in analysis_mode:
                ct = ClasificadorTonoV8(brand_name, brand_aliases)
                rt = await ct.procesar_lote_async(df["resumen_api"], pb, df[km["resumen"]], df[km["titulo"]])
            else:
                rt = [{"tono":"N/A"}]*len(rta); pb.progress(1.0)
            df[km["tonoiai"]] = [r["tono"] for r in rt]
            s.update(label="✓ Paso 3 — Tono analizado", state="complete")

        with st.status("**Paso 4 de 5** — Temas y subtemas", expanded=True) as s:
            pb = st.progress(0)
            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A"]*len(rta); temas_p = ["N/A"]*len(rta)
            else:
                cs = ClasificadorSubtemaV9(brand_name, brand_aliases)
                subtemas = cs.procesar_lote(df["resumen_api"], pb, df[km["resumen"]], df[km["titulo"]])
                temas_p = consolidar_subtemas_en_temas(subtemas, df["resumen_api"].tolist(), pb, marca=brand_name, aliases=brand_aliases)
            df[km["subtema"]] = subtemas
            if ("PKL" in analysis_mode) and tema_pkl_file:
                tp = analizar_temas_con_pkl(df["resumen_api"].tolist(), tema_pkl_file)
                if tp:
                    tc = unificar_temas_por_similitud(tp, subtemas, df["resumen_api"].tolist())
                    df[km["tema"]] = tc
            else:
                tc = unificar_temas_por_similitud(temas_p, subtemas, df["resumen_api"].tolist())
                df[km["tema"]] = tc
            s.update(label="✓ Paso 4 — Clasificación completada", state="complete")

        rm2 = df.set_index("original_index").to_dict("index")
        for row in apr:
            if not row.get("is_duplicate"): row.update(rm2.get(row["original_index"], {}))

    gc.collect()
    ci = (st.session_state['tokens_input']/1e6)*PRICE_INPUT_1M
    co = (st.session_state['tokens_output']/1e6)*PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding']/1e6)*PRICE_EMBEDDING_1M
    cost_str = f"${ci+co+ce:.4f}"

    with st.status("**Paso 5 de 5** — Generando informe", expanded=True) as s:
        dur = f"{time.time()-start_time:.0f}s"
        st.session_state["output_data"] = generate_output_excel(apr, km)
        st.session_state["output_filename"] = f"Informe_{brand_name.replace(' ','_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        st.session_state.update({"brand_name":brand_name, "brand_aliases":brand_aliases,
                                  "total_rows":len(apr), "unique_rows":len(rta),
                                  "duplicates":len(apr)-len(rta), "process_duration":dur, "process_cost":cost_str})
        s.update(label="✓ Paso 5 — Proceso completado", state="complete")


# ======================================
# Análisis Rápido
# ======================================
async def run_quick_analysis_async(df, title_col, summary_col, brand_name, aliases):
    st.session_state['tokens_input']=0; st.session_state['tokens_output']=0; st.session_state['tokens_embedding']=0
    df['texto_analisis'] = df[title_col].fillna('').astype(str)+". "+df[summary_col].fillna('').astype(str)
    with st.status("**Paso 1 de 2** — Analizando tono", expanded=True) as s:
        pb = st.progress(0, "Iniciando…")
        ct = ClasificadorTonoV8(brand_name, aliases)
        rt = await ct.procesar_lote_async(df["texto_analisis"], pb, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Tono IA'] = [r["tono"] for r in rt]
        s.update(label="✓ Paso 1 — Tono analizado", state="complete")
    with st.status("**Paso 2 de 2** — Temas y subtemas", expanded=True) as s:
        pb = st.progress(0, "Generando subtemas…")
        cs = ClasificadorSubtemaV9(brand_name, aliases)
        subtemas = cs.procesar_lote(df["texto_analisis"], pb, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Subtema'] = subtemas
        pt = st.progress(0, "Consolidando temas…")
        temas = consolidar_subtemas_en_temas(subtemas, df["texto_analisis"].tolist(), pt, marca=brand_name, aliases=aliases)
        tc = unificar_temas_por_similitud(temas, subtemas, df["texto_analisis"].tolist())
        df['Tema'] = tc
        s.update(label="✓ Paso 2 — Clasificación finalizada", state="complete")
    df.drop(columns=['texto_analisis'], inplace=True)
    ci = (st.session_state['tokens_input']/1e6)*PRICE_INPUT_1M
    co = (st.session_state['tokens_output']/1e6)*PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding']/1e6)*PRICE_EMBEDDING_1M
    st.session_state['quick_cost'] = f"${ci+co+ce:.4f}"
    return df


def generate_quick_analysis_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as w: df.to_excel(w, index=False, sheet_name='Analisis')
    return output.getvalue()


def render_quick_analysis_tab():
    render_section_header("Análisis Rápido", "Suba un archivo y obtenga tono, tema y subtema con IA")
    if 'quick_analysis_result' in st.session_state:
        render_completion_card("Análisis completado", f"Costo estimado: {st.session_state.get('quick_cost','$0.00')} USD")
        st.dataframe(st.session_state.quick_analysis_result.head(10), use_container_width=True)
        ed = generate_quick_analysis_excel(st.session_state.quick_analysis_result)
        st.download_button(label="Descargar Resultados", data=ed, file_name="Analisis_Rapido.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True, type="primary")
        if st.button("Nuevo Análisis", use_container_width=True):
            for k in ['quick_analysis_result','quick_analysis_df','quick_file_name','quick_cost']:
                if k in st.session_state: del st.session_state[k]
            st.rerun()
        return
    if 'quick_analysis_df' not in st.session_state:
        qf = st.file_uploader("Suba su archivo Excel", type=["xlsx"], label_visibility="collapsed", key="quick_uploader")
        if qf:
            with st.spinner("Leyendo archivo…"):
                try: st.session_state.quick_analysis_df = pd.read_excel(qf); st.session_state.quick_file_name = qf.name; st.rerun()
                except Exception as e: st.error(f"Error: {e}"); st.stop()
    else:
        render_info_box(f"Archivo <strong>{st.session_state.quick_file_name}</strong> cargado correctamente.")
        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df; cols = df.columns.tolist()
            c1, c2 = st.columns(2)
            title_col = c1.selectbox("Columna de título", options=cols, index=0)
            si = 1 if len(cols)>1 else 0
            summary_col = c2.selectbox("Columna de resumen", options=cols, index=si)
            st.markdown("---")
            render_section_header("Configuración de Marca")
            pkl_fb = st.file_uploader("Archivo .pkl para detectar marca (opcional)", type=["pkl"], key="quick_pkl_brand")
            db = ""
            if pkl_fb:
                det = extraer_marca_de_pkl(pkl_fb)
                if det: db = det; st.success(f"Marca detectada: **{det}**")
            brand_name = st.text_input("Marca principal", value=db, placeholder="Ej: Siemens")
            bat = st.text_area("Alias (separados por ;)", placeholder="Ej: Siemens Healthineers; Siemens Energy", height=80)
            if st.form_submit_button("Iniciar Análisis", use_container_width=True, type="primary"):
                if not brand_name: st.error("Falta el nombre de la marca.")
                else:
                    _setup_openai_key()
                    aliases = [a.strip() for a in bat.split(";") if a.strip()]
                    with st.spinner("Analizando…"):
                        st.session_state.quick_analysis_result = asyncio.run(
                            run_quick_analysis_async(df.copy(), title_col, summary_col, brand_name, aliases))
                    st.rerun()
        if st.button("← Cargar otro archivo"):
            for k in ['quick_analysis_df','quick_file_name','quick_analysis_result','quick_cost']:
                if k in st.session_state: del st.session_state[k]
            st.rerun()


# ======================================
# Main
# ======================================
def main():
    load_custom_css()
    if not check_password(): return
    render_masthead()
    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("input_form"):
                render_section_header("Archivos de Entrada", "Suba los tres archivos requeridos para el análisis")
                c1, c2, c3 = st.columns(3)
                dossier_file = c1.file_uploader("Dossier (.xlsx)", type=["xlsx"])
                region_file = c2.file_uploader("Región (.xlsx)", type=["xlsx"])
                internet_file = c3.file_uploader("Internet (.xlsx)", type=["xlsx"])
                render_section_header("Configuración de Marca")
                brand_name = st.text_input("Marca principal", placeholder="Ej: Bancolombia", key="main_brand_name")
                brand_aliases_text = st.text_area("Alias (separados por ;)", placeholder="Ej: Ban; Juan Carlos Mora", height=80, key="main_brand_aliases")
                render_section_header("Modo de Análisis")
                analysis_mode = st.radio("Seleccione el modo:", options=["Híbrido (PKL + API)","Solo Modelos PKL","API de OpenAI"], index=0, key="analysis_mode_radio", horizontal=True)
                tpf, tmpf = None, None
                if "PKL" in analysis_mode:
                    cc1, cc2 = st.columns(2)
                    tpf = cc1.file_uploader("sentimiento.pkl", type=["pkl"])
                    tmpf = cc2.file_uploader("tema.pkl", type=["pkl"])
                if st.form_submit_button("Iniciar Análisis", use_container_width=True, type="primary"):
                    if not all([dossier_file, region_file, internet_file, brand_name.strip()]):
                        st.error("Faltan datos obligatorios.")
                    else:
                        _setup_openai_key()
                        aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases, tpf, tmpf, analysis_mode))
                        st.rerun()
        else:
            render_section_header("Informe Generado", "Los resultados están listos para su descarga")
            render_metrics([
                {"value": st.session_state.total_rows, "label": "Total de noticias", "accent": ""},
                {"value": st.session_state.unique_rows, "label": "Noticias únicas", "accent": "c-green"},
                {"value": st.session_state.duplicates, "label": "Duplicadas", "accent": "c-amber"},
                {"value": st.session_state.process_duration, "label": "Duración", "accent": "c-blue"},
                {"value": f"{st.session_state.get('process_cost','$0.00')} USD", "label": "Costo estimado", "accent": "c-red"},
            ])
            render_completion_card("Análisis completado exitosamente",
                                    f"Marca: {st.session_state.brand_name} · {st.session_state.unique_rows} noticias procesadas")
            st.download_button("Descargar Informe", data=st.session_state.output_data,
                               file_name=st.session_state.output_filename,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True, type="primary")
            if st.button("Nuevo Análisis", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear(); st.session_state.password_correct = pwd; st.rerun()

    with tab2: render_quick_analysis_tab()
    render_footer()


if __name__ == "__main__":
    main()
