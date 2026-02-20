# ======================================
# app.py - Sistema de Análisis de Noticias con IA v8.5
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

# Modelos
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

# Configuración de rendimiento y umbrales
CONCURRENT_REQUESTS = 50
SIMILARITY_THRESHOLD_TONO = 0.88
SIMILARITY_THRESHOLD_TITULOS = 0.93
MAX_CONTEXT_WINDOW = 200
MIN_CONTEXT_WINDOW = 80

# Configuración de agrupación
NUM_TEMAS_PRINCIPALES = 20
UMBRAL_FUSION_SUBTEMAS = 0.78
UMBRAL_FUSION_TEMAS = 0.80

# Precios (Por 1 millón de tokens)
PRICE_INPUT_1M = 0.10
PRICE_OUTPUT_1M = 0.40
PRICE_EMBEDDING_1M = 0.02

# Inicializar contadores de tokens
for _counter_key in ['tokens_input', 'tokens_output', 'tokens_embedding']:
    if _counter_key not in st.session_state:
        st.session_state[_counter_key] = 0

# Listas Geográficas
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
# Estilos CSS - Editorial Moderno 2026
# ======================================
def load_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Playfair+Display:wght@700;800;900&display=swap');

    /* ===== GLOBAL RESET ===== */
    .stApp {
        background-color: #FAFAF8 !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }

    /* ===== HEADER EDITORIAL ===== */
    .editorial-masthead {
        text-align: center;
        padding: 3rem 2rem 2rem 2rem;
        border-bottom: 3px double #111;
        margin-bottom: 2rem;
    }
    .editorial-masthead .edition-line {
        font-family: 'Inter', sans-serif;
        font-size: 0.7rem;
        font-weight: 500;
        letter-spacing: 4px;
        text-transform: uppercase;
        color: #888;
        margin-bottom: 0.8rem;
    }
    .editorial-masthead .logo-title {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 3.2rem;
        font-weight: 900;
        color: #111;
        letter-spacing: -1px;
        line-height: 1.1;
        margin: 0;
    }
    .editorial-masthead .logo-title span.accent {
        color: #C4372B;
    }
    .editorial-masthead .tagline {
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem;
        font-weight: 400;
        color: #666;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-top: 0.6rem;
    }
    .editorial-masthead .divider-line {
        width: 60px;
        height: 2px;
        background: #C4372B;
        margin: 1rem auto 0 auto;
    }

    /* ===== SECTION HEADERS ===== */
    .section-header {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 1.6rem;
        font-weight: 800;
        color: #111;
        border-bottom: 1px solid #ddd;
        padding-bottom: 0.5rem;
        margin: 2rem 0 1.5rem 0;
        letter-spacing: -0.5px;
    }
    .section-subheader {
        font-family: 'Inter', sans-serif;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 3px;
        text-transform: uppercase;
        color: #999;
        margin-bottom: 1rem;
    }

    /* ===== METRIC CARDS ===== */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 1rem;
        margin: 1.5rem 0;
    }
    .metric-card-v2 {
        background: #fff;
        border: 1px solid #E8E8E4;
        padding: 1.4rem 1rem;
        text-align: center;
        transition: all 0.2s ease;
    }
    .metric-card-v2:hover {
        border-color: #111;
        transform: translateY(-1px);
    }
    .metric-card-v2 .metric-num {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 2.2rem;
        font-weight: 800;
        color: #111;
        line-height: 1;
    }
    .metric-card-v2 .metric-num.accent-green { color: #2D7D46; }
    .metric-card-v2 .metric-num.accent-amber { color: #B8860B; }
    .metric-card-v2 .metric-num.accent-blue { color: #2563EB; }
    .metric-card-v2 .metric-num.accent-red { color: #C4372B; }
    .metric-card-v2 .metric-lbl {
        font-family: 'Inter', sans-serif;
        font-size: 0.65rem;
        font-weight: 600;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #999;
        margin-top: 0.5rem;
    }

    /* ===== COMPLETION CARD ===== */
    .completion-card {
        background: #fff;
        border: 1px solid #E8E8E4;
        border-left: 4px solid #2D7D46;
        padding: 2rem;
        margin: 1.5rem 0;
    }
    .completion-card .completion-title {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 1.4rem;
        font-weight: 800;
        color: #111;
        margin-bottom: 0.3rem;
    }
    .completion-card .completion-sub {
        font-size: 0.85rem;
        color: #666;
    }

    /* ===== BUTTONS ===== */
    .stButton > button {
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 1px !important;
        text-transform: uppercase !important;
        font-size: 0.75rem !important;
        border-radius: 0 !important;
        padding: 0.8rem 2rem !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button[kind="primary"] {
        background-color: #111 !important;
        color: #fff !important;
        border: 2px solid #111 !important;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #C4372B !important;
        border-color: #C4372B !important;
    }
    .stButton > button[kind="secondary"] {
        background-color: #fff !important;
        color: #111 !important;
        border: 2px solid #111 !important;
    }
    .stButton > button[kind="secondary"]:hover {
        background-color: #111 !important;
        color: #fff !important;
    }

    /* ===== FORM STYLING ===== */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        font-family: 'Inter', sans-serif !important;
        border-radius: 0 !important;
        border: 1px solid #ddd !important;
        font-size: 0.9rem !important;
    }
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #111 !important;
        box-shadow: none !important;
    }
    .stSelectbox > div > div {
        border-radius: 0 !important;
    }

    /* ===== TABS ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 2px solid #E8E8E4;
    }
    .stTabs [data-baseweb="tab"] {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 0.75rem;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #999;
        padding: 0.8rem 1.5rem;
        border-radius: 0;
        border-bottom: 2px solid transparent;
        margin-bottom: -2px;
    }
    .stTabs [aria-selected="true"] {
        color: #111 !important;
        border-bottom: 2px solid #C4372B !important;
        background: transparent !important;
    }

    /* ===== FILE UPLOADER ===== */
    .stFileUploader > div {
        border-radius: 0 !important;
        border: 1px dashed #ccc !important;
    }
    .stFileUploader > div:hover {
        border-color: #111 !important;
    }

    /* ===== STATUS/PROGRESS ===== */
    .stProgress > div > div > div > div {
        background-color: #111 !important;
    }

    /* ===== RADIO ===== */
    .stRadio > div {
        gap: 0.5rem;
    }

    /* ===== ALERTS ===== */
    .stAlert {
        border-radius: 0 !important;
        font-family: 'Inter', sans-serif !important;
    }

    /* ===== INFO BOX CUSTOM ===== */
    .info-box {
        background: #F5F5F0;
        border-left: 3px solid #111;
        padding: 1rem 1.2rem;
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
        border-top: 1px solid #E8E8E4;
        margin-top: 3rem;
    }
    .editorial-footer p {
        font-family: 'Inter', sans-serif;
        font-size: 0.7rem;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #bbb;
    }

    /* ===== HIDE STREAMLIT DEFAULTS ===== */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* ===== DATAFRAME ===== */
    .stDataFrame {
        border-radius: 0 !important;
    }
    </style>
    """, unsafe_allow_html=True)


# ======================================
# Componentes UI Editoriales
# ======================================
def render_masthead():
    today = datetime.datetime.now()
    date_str = today.strftime("%B %d, %Y").upper()
    st.markdown(f"""
    <div class="editorial-masthead">
        <div class="edition-line">◆ {date_str} ◆</div>
        <h1 class="logo-title">News<span class="accent">Analyzer</span> AI</h1>
        <div class="tagline">Inteligencia artificial aplicada al análisis de medios</div>
        <div class="divider-line"></div>
    </div>
    """, unsafe_allow_html=True)


def render_section_header(title: str, subtitle: str = ""):
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="section-subheader">{subtitle}</div>', unsafe_allow_html=True)


def render_metrics(metrics: List[Dict]):
    cards_html = ""
    for m in metrics:
        accent = m.get("accent", "")
        cards_html += f"""
        <div class="metric-card-v2">
            <div class="metric-num {accent}">{m['value']}</div>
            <div class="metric-lbl">{m['label']}</div>
        </div>
        """
    st.markdown(f'<div class="metric-grid">{cards_html}</div>', unsafe_allow_html=True)


def render_completion_card(title: str, subtitle: str):
    st.markdown(f"""
    <div class="completion-card">
        <div class="completion-title">✓ {title}</div>
        <div class="completion-sub">{subtitle}</div>
    </div>
    """, unsafe_allow_html=True)


def render_info_box(text: str):
    st.markdown(f'<div class="info-box">{text}</div>', unsafe_allow_html=True)


def render_footer():
    st.markdown("""
    <div class="editorial-footer">
        <p>v8.5.0 ◆ NewsAnalyzer AI ◆ Realizado por Johnathan Cortés</p>
    </div>
    """, unsafe_allow_html=True)


# ======================================
# Wrapper unificado para OpenAI (v0.x y v1.x)
# ======================================
def _setup_openai_key():
    api_key = st.secrets.get("OPENAI_API_KEY", "")
    if not api_key:
        st.error("❌ OPENAI_API_KEY no encontrada en secrets.")
        st.stop()
    if OPENAI_NEW_API:
        return api_key
    else:
        openai.api_key = api_key
        try:
            openai.aiosession.set(None)
        except Exception:
            pass
        return api_key


def _count_usage(resp):
    if resp is None:
        return
    if OPENAI_NEW_API:
        usage = getattr(resp, 'usage', None)
        if usage:
            st.session_state['tokens_input'] += getattr(usage, 'prompt_tokens', 0)
            st.session_state['tokens_output'] += getattr(usage, 'completion_tokens', 0)
    else:
        usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
        if usage:
            pt = usage.get('prompt_tokens', 0) if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
            ct = usage.get('completion_tokens', 0) if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
            st.session_state['tokens_input'] += pt
            st.session_state['tokens_output'] += ct


def _count_embedding_usage(resp):
    if resp is None:
        return
    if OPENAI_NEW_API:
        usage = getattr(resp, 'usage', None)
        if usage:
            st.session_state['tokens_embedding'] += getattr(usage, 'total_tokens', 0)
    else:
        usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
        if usage:
            total = usage.get('total_tokens', 0) if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
            st.session_state['tokens_embedding'] += total


def call_with_retries(func, *args, max_retries=3, **kwargs):
    delay = 1
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)
            delay *= 2


async def acall_with_retries(func, *args, max_retries=3, **kwargs):
    delay = 1
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            await asyncio.sleep(delay)
            delay *= 2


def _create_embedding_sync(input_texts: List[str]):
    if OPENAI_NEW_API:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        return client.embeddings.create(input=input_texts, model=OPENAI_MODEL_EMBEDDING)
    else:
        return openai.Embedding.create(input=input_texts, model=OPENAI_MODEL_EMBEDDING)


def _create_chat_sync(messages, max_tokens=50, temperature=0.0, response_format=None):
    kwargs = {"model": OPENAI_MODEL_CLASIFICACION, "messages": messages,
              "max_tokens": max_tokens, "temperature": temperature}
    if response_format:
        kwargs["response_format"] = response_format
    if OPENAI_NEW_API:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        return client.chat.completions.create(**kwargs)
    else:
        return openai.ChatCompletion.create(**kwargs)


async def _create_chat_async(messages, max_tokens=50, temperature=0.0, response_format=None):
    kwargs = {"model": OPENAI_MODEL_CLASIFICACION, "messages": messages,
              "max_tokens": max_tokens, "temperature": temperature}
    if response_format:
        kwargs["response_format"] = response_format
    if OPENAI_NEW_API:
        client = AsyncOpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        return await client.chat.completions.create(**kwargs)
    else:
        return await openai.ChatCompletion.acreate(**kwargs)


def _get_chat_content(resp) -> str:
    if OPENAI_NEW_API:
        return resp.choices[0].message.content.strip()
    else:
        msg = resp.choices[0].message
        return (msg.content if hasattr(msg, 'content') else resp["choices"][0]["message"]["content"]).strip()


def _get_embedding_data(resp):
    if OPENAI_NEW_API:
        return resp.data
    else:
        return resp["data"] if isinstance(resp, dict) else resp.data


# ======================================
# Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True

    render_masthead()
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        render_section_header("Acceso Seguro", "Ingrese sus credenciales")
        with st.form("password_form"):
            password = st.text_input("Contraseña:", type="password", label_visibility="collapsed",
                                     placeholder="Ingrese su contraseña")
            if st.form_submit_button("INGRESAR", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta")
    return False


def norm_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))


def string_norm_label(s: str) -> str:
    if not s:
        return ""
    s = unidecode(s.lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(t for t in s.split() if t not in STOPWORDS_ES)


def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str):
        return ""
    tmp = re.split(r"\s*[:|\-|]\s*", title, 1)
    cleaned = tmp[0] if tmp else title
    return re.sub(r"\W+", " ", cleaned).lower().strip()


def clean_title_for_output(title: Any) -> str:
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()


def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str):
        return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match:
        text = text[match.start():]
    if text and not text.endswith("..."):
        text = text.rstrip(".") + "..."
    return text


def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str):
        return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {
        "fm": "Radio", "am": "Radio", "radio": "Radio",
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión",
        "television": "Televisión", "televisión": "Televisión",
        "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa",
        "revista": "Revista", "revistas": "Revista",
        "online": "Internet", "internet": "Internet",
        "digital": "Internet", "web": "Internet"
    }
    default_value = str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro"
    return mapping.get(t, default_value)


def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match:
            return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}


# ======================================
# Limpieza de temas
# ======================================
def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    tema = re.sub(r'[.,:;!?]+$', '', tema)
    if not tema:
        return "Sin tema"
    tema = tema[0].upper() + tema[1:]
    invalid_trailing = {"en", "de", "del", "la", "el", "y", "o", "con",
                        "sin", "por", "para", "sobre", "al", "los", "las",
                        "un", "una", "su", "sus"}
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_trailing:
        palabras.pop()
    if len(palabras) > 5:
        palabras = palabras[:5]
    tema = " ".join(palabras)
    return tema if tema else "Sin tema"


def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema:
        return "Sin tema"
    tema_lower = tema.lower()
    all_brand_names = [marca.lower()] + [a.lower() for a in aliases if a]
    for brand_name in all_brand_names:
        tema_lower = re.sub(rf'\b{re.escape(brand_name)}\b', '', tema_lower, flags=re.IGNORECASE)
        tema_lower = re.sub(rf'\b{re.escape(unidecode(brand_name))}\b', '', tema_lower, flags=re.IGNORECASE)
    for ciudad in CIUDADES_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(ciudad)}\b', '', tema_lower, flags=re.IGNORECASE)
    for gentilicio in GENTILICIOS_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(gentilicio)}\b', '', tema_lower, flags=re.IGNORECASE)
    frases_geo = ["en colombia", "de colombia", "del pais", "en el pais", "nacional", "territorio nacional"]
    for frase in frases_geo:
        tema_lower = re.sub(rf'\b{re.escape(frase)}\b', '', tema_lower, flags=re.IGNORECASE)
    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
    if not palabras:
        return "Sin tema"
    tema_limpio = " ".join(palabras)
    tema_limpio = tema_limpio[0].upper() + tema_limpio[1:]
    return limpiar_tema(tema_limpio)


# ======================================
# Embeddings con deduplicación
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos:
        return []
    texto_to_idx: Dict[str, List[int]] = defaultdict(list)
    for i, t in enumerate(textos):
        key = (t or "")[:2000]
        texto_to_idx[key].append(i)
    unique_texts = list(texto_to_idx.keys())
    unique_embeddings: Dict[str, Optional[List[float]]] = {}
    for i in range(0, len(unique_texts), batch_size):
        batch = unique_texts[i:i + batch_size]
        batch_clean = [t if t.strip() else " " for t in batch]
        try:
            resp = call_with_retries(_create_embedding_sync, batch_clean)
            _count_embedding_usage(resp)
            emb_data = _get_embedding_data(resp)
            for j, ed in enumerate(emb_data):
                embedding = ed.embedding if hasattr(ed, 'embedding') else ed["embedding"]
                unique_embeddings[batch[j]] = embedding
        except Exception:
            for j, texto in enumerate(batch):
                try:
                    resp = _create_embedding_sync([texto if texto.strip() else " "])
                    _count_embedding_usage(resp)
                    emb_data = _get_embedding_data(resp)
                    embedding = emb_data[0].embedding if hasattr(emb_data[0], 'embedding') else emb_data[0]["embedding"]
                    unique_embeddings[texto] = embedding
                except Exception:
                    unique_embeddings[texto] = None
    resultados = [None] * len(textos)
    for text_key, indices in texto_to_idx.items():
        emb = unique_embeddings.get(text_key)
        for idx in indices:
            resultados[idx] = emb
    return resultados


# ======================================
# DSU robusto
# ======================================
class DSU:
    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, i: int) -> int:
        while self.parent[i] != i:
            self.parent[i] = self.parent[self.parent[i]]
            i = self.parent[i]
        return i

    def union(self, i: int, j: int):
        ri, rj = self.find(i), self.find(j)
        if ri == rj:
            return
        if self.rank[ri] < self.rank[rj]:
            ri, rj = rj, ri
        self.parent[rj] = ri
        if self.rank[ri] == self.rank[rj]:
            self.rank[ri] += 1

    def components(self) -> Dict[int, List[int]]:
        comp = defaultdict(list)
        for i in range(len(self.parent)):
            comp[self.find(i)].append(i)
        return dict(comp)


# ======================================
# Agrupación genérica
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    if not textos or len(textos) < 2:
        return {}
    embs = get_embeddings_batch(textos)
    valid_indices = [i for i, e in enumerate(embs) if e is not None]
    if len(valid_indices) < 2:
        return {}
    emb_matrix = np.array([embs[i] for i in valid_indices])
    dist_matrix = 1 - cosine_similarity(emb_matrix)
    np.fill_diagonal(dist_matrix, 0)
    dist_matrix = np.clip(dist_matrix, 0, 2)
    clustering = AgglomerativeClustering(
        n_clusters=None, distance_threshold=1 - umbral_similitud,
        metric="precomputed", linkage="average"
    ).fit(dist_matrix)
    grupos = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        grupos[label].append(valid_indices[i])
    return {gid: g for gid, g in enumerate(grupos.values()) if len(g) >= 2}


def agrupar_por_titulo_similar(titulos: List[str]) -> Dict[int, List[int]]:
    gid, grupos, used = 0, {}, set()
    norm_titles = [normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(norm_titles)):
        if i in used or not norm_titles[i]:
            continue
        grupo_actual = [i]
        used.add(i)
        for j in range(i + 1, len(norm_titles)):
            if j in used or not norm_titles[j]:
                continue
            if SequenceMatcher(None, norm_titles[i], norm_titles[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                grupo_actual.append(j)
                used.add(j)
        if len(grupo_actual) >= 2:
            grupos[gid] = grupo_actual
            gid += 1
    return grupos


def seleccionar_representante(indices: List[int], textos: List[str]) -> Tuple[int, str]:
    if len(indices) == 1:
        return indices[0], textos[indices[0]]
    subset_textos = [textos[i] for i in indices]
    embs = get_embeddings_batch(subset_textos)
    valid_embs, valid_indices = [], []
    for idx_in_subset, emb in enumerate(embs):
        if emb is not None:
            valid_embs.append(emb)
            valid_indices.append(indices[idx_in_subset])
    if not valid_embs:
        return indices[0], textos[indices[0]]
    M = np.array(valid_embs)
    centro = M.mean(axis=0, keepdims=True)
    sims = cosine_similarity(M, centro).reshape(-1)
    best = int(np.argmax(sims))
    return valid_indices[best], textos[valid_indices[best]]


# ======================================
# Extracción de marca desde PKL
# ======================================
def extraer_marca_de_pkl(pkl_file: io.BytesIO) -> Optional[str]:
    try:
        pkl_file.seek(0)
        pipeline = joblib.load(pkl_file)
        pkl_file.seek(0)
        for attr_name in ['marca', 'brand', 'client', 'cliente', 'target_name', 'brand_name', 'client_name']:
            if hasattr(pipeline, attr_name):
                val = getattr(pipeline, attr_name)
                if isinstance(val, str) and val.strip():
                    return val.strip()
        if hasattr(pipeline, 'steps'):
            for step_name, step_obj in pipeline.steps:
                for attr_name in ['marca', 'brand', 'client', 'cliente']:
                    if hasattr(step_obj, attr_name):
                        val = getattr(step_obj, attr_name)
                        if isinstance(val, str) and val.strip():
                            return val.strip()
        if hasattr(pipeline, 'metadata'):
            meta = pipeline.metadata
            if isinstance(meta, dict):
                for key in ['marca', 'brand', 'client', 'cliente']:
                    if key in meta and isinstance(meta[key], str):
                        return meta[key].strip()
        return None
    except Exception:
        return None


# ======================================
# CLASIFICADOR DE TONO v8
# ======================================
class ClasificadorTonoV8:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex(marca, aliases)

    def _build_brand_regex(self, marca, aliases):
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        if not patterns:
            return re.compile(r"(a^b)")
        return re.compile(r"\b(" + "|".join(patterns) + r")\b", re.IGNORECASE)

    def _extract_brand_contexts(self, texto):
        texto_lower = unidecode(texto.lower())
        matches = list(self.brand_pattern.finditer(texto_lower))
        if not matches:
            return [texto[:400]]
        contextos = []
        oraciones = re.split(r'(?<=[.!?])\s+', texto)
        for match in matches[:5]:
            pos = match.start()
            snippet = texto_lower[max(0, pos - 30):min(len(texto_lower), pos + 80)]
            has_action = bool(re.search(
                r'(lanz|denunci|sancion|innov|crisis|acuerd|alianz|premi|multa|demand|creci|pérdi|gananci)', snippet))
            window = MAX_CONTEXT_WINDOW if has_action else MIN_CONTEXT_WINDOW
            char_count = 0
            relevant = []
            for sent in oraciones:
                s_start, s_end = char_count, char_count + len(sent)
                if s_start <= pos + window and s_end >= max(0, pos - window):
                    relevant.append(sent.strip())
                char_count = s_end + 1
            if relevant:
                contextos.append(" ".join(relevant))
            else:
                start = max(0, pos - window)
                end = min(len(texto), pos + window)
                while end < len(texto) and texto[end] not in '.!?\n':
                    end += 1
                contextos.append(texto[start:end + 1].strip())
        seen = set()
        unique = []
        for c in contextos:
            k = c[:100]
            if k not in seen:
                seen.add(k)
                unique.append(c)
        return unique[:4]

    def _analyze_brand_sentiment_rules(self, contextos):
        pos_score, neg_score = 0, 0
        for contexto in contextos:
            t = unidecode(contexto.lower())
            brand_match = self.brand_pattern.search(t)
            if not brand_match:
                continue
            brand_pos = brand_match.start()
            pre_brand = t[max(0, brand_pos - 40):brand_pos]
            has_neg = bool(re.search(r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente)\b', pre_brand))
            post_brand = t[brand_pos:min(len(t), brand_pos + 150)]
            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(post_brand):
                pos_score += 4
                continue
            for p in POS_COMPILED:
                if p.search(post_brand):
                    neg_score += 1 if has_neg else 0
                    pos_score += 0 if has_neg else 1.5
            for p in NEG_COMPILED:
                if p.search(post_brand):
                    pos_score += 1 if has_neg else 0
                    neg_score += 0 if has_neg else 1.5
            pre_ctx = t[max(0, brand_pos - 100):brand_pos]
            for p in NEG_COMPILED:
                if p.search(pre_ctx):
                    neg_score += 1
            for p in POS_COMPILED:
                if p.search(pre_ctx):
                    pos_score += 1
        if pos_score >= 3 and pos_score > neg_score * 2:
            return "Positivo"
        elif neg_score >= 3 and neg_score > pos_score * 2:
            return "Negativo"
        return None

    async def _llm_classify_tone(self, contextos, semaphore):
        async with semaphore:
            aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
            ctx_text = "\n---\n".join(c[:500] for c in contextos[:3])
            prompt = f"""Eres un analista de reputación corporativa. Analiza el SENTIMIENTO específicamente hacia '{self.marca}' (alias: {aliases_str}).

IMPORTANTE: Clasifica SOLO según cómo afecta la imagen de '{self.marca}', NO el tono general.

CRITERIOS:
- Positivo: logros, lanzamientos, reconocimientos, alianzas, crecimiento, respuesta a crisis, innovación
- Negativo: críticas, sanciones, multas, demandas, pérdidas, fallos, quejas, escándalos
- Neutro: menciones informativas sin juicio claro

EJEMPLOS:
- "X lanzó su nueva plataforma digital" → Positivo
- "X fue multada por la SIC" → Negativo
- "En la reunión participó X" → Neutro
- "Tras la emergencia, X activó su plan" → Positivo
- "Usuarios reportan fallas en la app de X" → Negativo

FRAGMENTOS:
---
{ctx_text}
---

JSON: {{"tono":"Positivo"|"Negativo"|"Neutro"}}"""
            try:
                resp = await acall_with_retries(
                    _create_chat_async, messages=[{"role": "user", "content": prompt}],
                    max_tokens=30, temperature=0.0, response_format={"type": "json_object"})
                _count_usage(resp)
                data = json.loads(_get_chat_content(resp))
                tono = str(data.get("tono", "Neutro")).strip().title()
                return tono if tono in ["Positivo", "Negativo", "Neutro"] else "Neutro"
            except Exception:
                return "Neutro"

    async def _classify_group(self, texto_rep, semaphore):
        contextos = self._extract_brand_contexts(texto_rep)
        tono_reglas = self._analyze_brand_sentiment_rules(contextos)
        if tono_reglas:
            return tono_reglas
        return await self._llm_classify_tone(contextos, semaphore)

    async def procesar_lote_async(self, textos_concat, progress_bar, resumen_puro, titulos_puros):
        textos = textos_concat.tolist()
        n = len(textos)
        progress_bar.progress(0.05, text="Agrupando noticias similares...")
        dsu = DSU(n)
        for _, idxs in agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO).items():
            for j in idxs[1:]:
                dsu.union(idxs[0], j)
        for _, idxs in agrupar_por_titulo_similar(titulos_puros.tolist()).items():
            for j in idxs[1:]:
                dsu.union(idxs[0], j)
        comp = dsu.components()
        representantes = {}
        for cid, idxs in comp.items():
            _, rep_text = seleccionar_representante(idxs, textos)
            representantes[cid] = rep_text
        progress_bar.progress(0.15, text=f"{len(comp)} grupos identificados")
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        cids = list(representantes.keys())
        tasks = [self._classify_group(representantes[cid], semaphore) for cid in cids]
        resultados_por_grupo = {}
        completed = 0
        for cid_idx, coro in enumerate(asyncio.as_completed(tasks)):
            tono = await coro
            resultados_por_grupo[cids[cid_idx]] = tono
            completed += 1
            if completed % 10 == 0 or completed == len(tasks):
                progress_bar.progress(0.15 + 0.80 * completed / len(tasks),
                                      text=f"Analizando tono: {completed}/{len(tasks)}")
        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            tono = resultados_por_grupo.get(cid, "Neutro")
            for i in idxs:
                resultados_finales[i] = {"tono": tono}
        progress_bar.progress(1.0, text="Tono completado")
        return resultados_finales


def analizar_tono_con_pkl(textos, pkl_file):
    try:
        pkl_file.seek(0)
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        TONO_MAP = {1: "Positivo", "1": "Positivo", 0: "Neutro", "0": "Neutro",
                    -1: "Negativo", "-1": "Negativo",
                    "Positivo": "Positivo", "Negativo": "Negativo", "Neutro": "Neutro",
                    "positivo": "Positivo", "negativo": "Negativo", "neutro": "Neutro"}
        return [{"tono": TONO_MAP.get(p, str(p).title())} for p in predicciones]
    except Exception as e:
        st.error(f"Error con pipeline_sentimiento.pkl: {e}")
        return None


# ======================================
# CLASIFICADOR DE SUBTEMAS v8
# ======================================
class ClasificadorSubtemaV8:
    def __init__(self, marca, aliases):
        self.marca = marca
        self.aliases = aliases or []
        self.cache_subtemas = {}

    def _preagrupar_identicos(self, textos, titulos, resumenes):
        def norm_r(t):
            if not t: return ""
            t = unidecode(str(t).lower())
            t = re.sub(r'[^a-z0-9\s]', '', t)
            return ' '.join(t.split()[:40])
        thi, rhi = defaultdict(list), defaultdict(list)
        for i, titulo in enumerate(titulos):
            n = norm_r(titulo)
            if n: thi[hashlib.md5(n.encode()).hexdigest()].append(i)
        for i, resumen in enumerate(resumenes):
            n = norm_r(resumen)
            if n: rhi[hashlib.md5(n[:150].encode()).hexdigest()].append(i)
        grupos, usado, gid = {}, set(), 0
        for indices in thi.values():
            if len(indices) >= 2:
                nuevos = [i for i in indices if i not in usado]
                if len(nuevos) >= 2:
                    grupos[gid] = nuevos; usado.update(nuevos); gid += 1
        for indices in rhi.values():
            nuevos = [i for i in indices if i not in usado]
            if len(nuevos) >= 2:
                grupos[gid] = nuevos; usado.update(nuevos); gid += 1
        return grupos

    def _clustering_por_lotes(self, textos, titulos, indices):
        if len(indices) < 2: return {}
        BS, gf, go = 500, {}, 0
        for bs in range(0, len(indices), BS):
            bi = indices[bs:bs + BS]
            bt = [f"{titulos[i][:150]} {textos[i][:1200]}" for i in bi]
            embs = get_embeddings_batch(bt)
            ve, vi = [], []
            for k, e in enumerate(embs):
                if e is not None: ve.append(e); vi.append(bi[k])
            if len(ve) < 2: continue
            sm = cosine_similarity(np.array(ve))
            dm = np.clip(1 - sm, 0, 2)
            np.fill_diagonal(dm, 0)
            cl = AgglomerativeClustering(n_clusters=None, distance_threshold=0.20,
                                         metric='precomputed', linkage='average').fit(dm)
            g = defaultdict(list)
            for i, l in enumerate(cl.labels_): g[l].append(vi[i])
            for l, idxs in g.items():
                if len(idxs) >= 2: gf[go + l] = idxs
            go += (max(cl.labels_) + 1) if len(cl.labels_) > 0 else 0
        return gf

    def _generar_subtema(self, textos_m, titulos_m):
        tn = sorted(normalize_title_for_comparison(t) for t in titulos_m[:5])
        ck = hashlib.md5("|".join(tn).encode()).hexdigest()
        if ck in self.cache_subtemas: return self.cache_subtemas[ck]
        aw = []
        for t in titulos_m[:5]:
            aw.extend([w for w in string_norm_label(t).split() if w not in STOPWORDS_ES and len(w) > 3])
        kw = [w for w, _ in Counter(aw).most_common(8)]
        td = "\n".join(f"- {t[:120]}" for t in titulos_m[:5])
        prompt = f"""Genera una ETIQUETA TEMÁTICA de 2 a 5 palabras para estas noticias.
TÍTULOS:
{td}
KEYWORDS: {', '.join(kw)}
REGLAS: NO '{self.marca}', NO ciudades, NO verbos, SER ESPECÍFICO, MÁXIMO 5 palabras.
BUENOS: "Expansión Red Sucursales", "Resultados Financieros Trimestrales", "Transformación Digital"
MALOS: "Noticias", "Actividades Varias", "Gestión Empresa"
JSON: {{"subtema": "..."}}"""
        try:
            resp = call_with_retries(_create_chat_sync, messages=[{"role": "user", "content": prompt}],
                                     max_tokens=30, temperature=0.05, response_format={"type": "json_object"})
            _count_usage(resp)
            data = json.loads(_get_chat_content(resp))
            subtema = limpiar_tema_geografico(limpiar_tema(data.get("subtema", "Varios")), self.marca, self.aliases)
            gen = {"sin tema", "varios", "noticias", "actividad", "gestión", "información", "nota", "noticia"}
            if subtema.lower().strip() in gen:
                subtema = " ".join(kw[:3]).title() if kw else "Actividad Corporativa"
            self.cache_subtemas[ck] = subtema
            return subtema
        except Exception:
            subtema = " ".join(kw[:3]).title() if kw else "Actividad Corporativa"
            self.cache_subtemas[ck] = subtema
            return subtema

    def _fusionar_subtemas_similares(self, subtemas, textos):
        df_t = pd.DataFrame({'label': subtemas, 'text': textos})
        ul = list(df_t['label'].unique())
        if len(ul) < 2: return subtemas
        el = get_embeddings_batch(ul)
        te = get_embeddings_batch(textos)
        cc, vl, vle = [], [], []
        for i, label in enumerate(ul):
            if el[i] is None: continue
            idxs = df_t.index[df_t['label'] == label].tolist()[:40]
            vecs = [te[j] for j in idxs if te[j] is not None]
            cc.append(np.mean(vecs, axis=0) if vecs else el[i])
            vl.append(label); vle.append(el[i])
        if len(vl) < 2: return subtemas
        ml, mc = np.array(vle), np.array(cc)
        sl, sc = cosine_similarity(ml), cosine_similarity(mc)
        scomb = 0.4 * sl + 0.6 * sc
        dm = np.clip(1 - scomb, 0, 2)
        np.fill_diagonal(dm, 0)
        cl = AgglomerativeClustering(n_clusters=None, distance_threshold=1 - UMBRAL_FUSION_SUBTEMAS,
                                      metric='precomputed', linkage='average').fit(dm)
        mf = {}
        for cid in set(cl.labels_):
            ci = [i for i, x in enumerate(cl.labels_) if x == cid]
            lic = [vl[i] for i in ci]
            counts = {l: sum(1 for s in subtemas if s == l) for l in lic}
            rep = max(lic, key=lambda x: (counts.get(x, 0), -len(x)))
            for l in lic: mf[l] = rep
        return [mf.get(l, l) for l in subtemas]

    def procesar_lote(self, textos_concat, progress_bar, resumen_puro, titulos_puros):
        textos = textos_concat.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumen_puro.tolist()
        n = len(textos)
        progress_bar.progress(0.05, "Pre-agrupando noticias...")
        gr = self._preagrupar_identicos(textos, titulos, resumenes)
        dsu = DSU(n)
        for idxs in gr.values():
            for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = dsu.components()
        sueltos = [idxs[0] for idxs in comp.values() if len(idxs) == 1]
        progress_bar.progress(0.15, f"{len(gr)} grupos rápidos")
        if len(sueltos) > 1:
            gc2 = self._clustering_por_lotes(textos, titulos, sueltos)
            for idxs in gc2.values():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = dsu.components()
        tg = len(comp)
        progress_bar.progress(0.30, f"Etiquetando {tg} grupos...")
        ms = {}
        for k, (lid, idxs) in enumerate(comp.items()):
            if k % 15 == 0:
                progress_bar.progress(0.30 + 0.40 * k / max(tg, 1), f"Etiquetando: {k}/{tg}")
            subtema = self._generar_subtema([textos[i] for i in idxs], [titulos[i] for i in idxs])
            for i in idxs: ms[i] = subtema
        sb = [ms.get(i, "Varios") for i in range(n)]
        nb = len(set(sb))
        progress_bar.progress(0.75, "Fusionando subtemas similares...")
        sf = self._fusionar_subtemas_similares(sb, textos)
        nf = len(set(sf))
        st.info(f"Subtemas: {nb} → {nf}")
        progress_bar.progress(1.0, "Subtemas listos")
        return sf


# ======================================
# Coherencia Tema-Subtema (MEJORADA)
# ======================================
def unificar_temas_por_subtema(temas: List[str], subtemas: List[str]) -> List[str]:
    """
    Garantiza que noticias con el MISMO subtema tengan el MISMO tema.
    Usa voto mayoritario: el tema más frecuente dentro de cada subtema gana.
    """
    subtema_to_temas = defaultdict(list)
    for i, sub in enumerate(subtemas):
        subtema_to_temas[sub].append(temas[i])

    subtema_to_tema_final = {}
    for sub, tema_list in subtema_to_temas.items():
        # Voto mayoritario
        counter = Counter(tema_list)
        tema_ganador = counter.most_common(1)[0][0]
        subtema_to_tema_final[sub] = tema_ganador

    return [subtema_to_tema_final[sub] for sub in subtemas]


def unificar_temas_por_similitud(
    temas: List[str], subtemas: List[str], textos: List[str]
) -> List[str]:
    """
    Además del voto por subtema idéntico, agrupa subtemas SIMILARES
    bajo el mismo tema usando embeddings de contenido.
    """
    # Paso 1: Primero unificar por subtema idéntico
    temas = unificar_temas_por_subtema(temas, subtemas)

    # Paso 2: Encontrar subtemas únicos y sus temas asignados
    df_work = pd.DataFrame({'tema': temas, 'subtema': subtemas, 'texto': textos})
    unique_subtemas = list(df_work['subtema'].unique())

    if len(unique_subtemas) < 2:
        return temas

    # Paso 3: Calcular centroides de contenido por subtema
    todos_embs = get_embeddings_batch(textos)
    centroides = []
    valid_subs = []

    for sub in unique_subtemas:
        idxs = df_work.index[df_work['subtema'] == sub].tolist()[:30]
        vecs = [todos_embs[j] for j in idxs if todos_embs[j] is not None]
        if vecs:
            centroides.append(np.mean(vecs, axis=0))
            valid_subs.append(sub)

    if len(valid_subs) < 2:
        return temas

    # Paso 4: Agrupar subtemas similares
    matrix = np.array(centroides)
    sim_matrix = cosine_similarity(matrix)
    dist_matrix = np.clip(1 - sim_matrix, 0, 2)
    np.fill_diagonal(dist_matrix, 0)

    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=1 - UMBRAL_FUSION_TEMAS,
        metric='precomputed',
        linkage='average'
    ).fit(dist_matrix)

    # Paso 5: Para cada cluster de subtemas similares, unificar al tema más frecuente
    mapa_sub_to_tema_unificado = {}
    for cluster_id in set(clustering.labels_):
        cluster_subs = [valid_subs[i] for i, l in enumerate(clustering.labels_) if l == cluster_id]

        # Recoger todos los temas asignados a estos subtemas
        all_temas_in_cluster = []
        for sub in cluster_subs:
            rows_sub = df_work[df_work['subtema'] == sub]
            all_temas_in_cluster.extend(rows_sub['tema'].tolist())

        # Voto mayoritario del cluster completo
        if all_temas_in_cluster:
            tema_ganador = Counter(all_temas_in_cluster).most_common(1)[0][0]
        else:
            tema_ganador = cluster_subs[0]

        for sub in cluster_subs:
            mapa_sub_to_tema_unificado[sub] = tema_ganador

    # Aplicar
    return [mapa_sub_to_tema_unificado.get(sub, temas[i]) for i, sub in enumerate(subtemas)]


def consolidar_subtemas_en_temas(subtemas, textos, p_bar, marca="", aliases=None):
    aliases = aliases or []
    p_bar.progress(0.1, text="Analizando estructura de temas...")
    df_temas = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    unique_subtemas = list(df_temas['subtema'].unique())
    if len(unique_subtemas) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "Temas finalizados")
        return subtemas

    embs_labels = get_embeddings_batch(unique_subtemas)
    todos_embs = get_embeddings_batch(textos)
    centroides, valid_subtemas, valid_label_embs = [], [], []
    for i, subt in enumerate(unique_subtemas):
        if embs_labels[i] is None: continue
        idxs = df_temas.index[df_temas['subtema'] == subt].tolist()[:30]
        vecs = [todos_embs[j] for j in idxs if todos_embs[j] is not None]
        centroides.append(np.mean(vecs, axis=0) if vecs else embs_labels[i])
        valid_subtemas.append(subt)
        valid_label_embs.append(embs_labels[i])
    if len(valid_subtemas) < 2:
        p_bar.progress(1.0, "Temas finalizados"); return subtemas

    ml, mc = np.array(valid_label_embs), np.array(centroides)
    sf = 0.35 * cosine_similarity(ml) + 0.65 * cosine_similarity(mc)
    df2 = np.clip(1 - sf, 0, 2)
    np.fill_diagonal(df2, 0)
    nct = min(NUM_TEMAS_PRINCIPALES, len(valid_subtemas))
    cl = AgglomerativeClustering(n_clusters=nct, metric='precomputed', linkage='average').fit(df2)
    cc = defaultdict(list)
    for i, label in enumerate(cl.labels_): cc[label].append(valid_subtemas[i])
    p_bar.progress(0.5, text="Nombrando categorías...")
    mapa_tema_final = {}
    for cid, lista_subt in cc.items():
        ss = ", ".join(lista_subt[:8])
        ts = []
        for subt in lista_subt[:3]:
            rs = df_temas[df_temas['subtema'] == subt].head(2)
            for _, r in rs.iterrows():
                ts.append(str(r['texto']).split('.')[0][:100])
        ctx = "\n".join(f"- {t}" for t in ts[:5])
        prompt = f"""Categoría temática (2-3 palabras) para: {ss}
Noticias ejemplo:
{ctx}
REGLAS: Máx 3 palabras, NO verbos, NO empresas, descriptivo.
BUENOS: "Resultados Financieros", "Expansión Comercial", "Innovación Digital"
Responde SOLO el nombre."""
        try:
            resp = call_with_retries(_create_chat_sync, messages=[{"role": "user", "content": prompt}],
                                     max_tokens=15, temperature=0.1)
            _count_usage(resp)
            nt = limpiar_tema(_get_chat_content(resp).replace('"', '').replace('.', ''))
            if not nt or nt.lower() in {"sin tema", "varios", "noticias"}: nt = lista_subt[0]
        except Exception:
            nt = lista_subt[0]
        if marca:
            nt = limpiar_tema_geografico(nt, marca, aliases)
        for subt in lista_subt: mapa_tema_final[subt] = nt
    temas_finales = [mapa_tema_final.get(subt, subt) for subt in subtemas]
    n_temas = len(set(temas_finales))
    st.info(f"Temas consolidados en {n_temas} categorías")
    p_bar.progress(1.0, "Temas finalizados")
    return temas_finales


def analizar_temas_con_pkl(textos, pkl_file):
    try:
        pkl_file.seek(0)
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error con pipeline_tema.pkl: {e}")
        return None


# ======================================
# Detección de Duplicados
# ======================================
def detectar_duplicados_avanzado(rows, key_map):
    processed_rows = deepcopy(rows)
    seen_online_url, seen_broadcast = {}, {}
    online_title_buckets = defaultdict(list)
    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"): continue
        tipo = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio"))))
        mn = norm_key(row.get(key_map.get("menciones")))
        med = norm_key(row.get(key_map.get("medio")))
        if tipo == "Internet":
            li = row.get(key_map.get("link_nota"), {})
            url = li.get("url") if isinstance(li, dict) else None
            if url and mn:
                key = (url, mn)
                if key in seen_online_url:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_online_url[key]].get(key_map.get("idnoticia"), "")
                    continue
                else: seen_online_url[key] = i
            if med and mn: online_title_buckets[(med, mn)].append(i)
        elif tipo in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mn and med and hora:
                key = (mn, med, hora)
                if key in seen_broadcast:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_broadcast[key]].get(key_map.get("idnoticia"), "")
                else: seen_broadcast[key] = i
    for indices in online_title_buckets.values():
        if len(indices) < 2: continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx1, idx2 = indices[i], indices[j]
                if processed_rows[idx1].get("is_duplicate") or processed_rows[idx2].get("is_duplicate"): continue
                t1 = normalize_title_for_comparison(processed_rows[idx1].get(key_map.get("titulo")))
                t2 = normalize_title_for_comparison(processed_rows[idx2].get(key_map.get("titulo")))
                if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    if len(t1) < len(t2):
                        processed_rows[idx1]["is_duplicate"] = True
                        processed_rows[idx1]["idduplicada"] = processed_rows[idx2].get(key_map.get("idnoticia"), "")
                    else:
                        processed_rows[idx2]["is_duplicate"] = True
                        processed_rows[idx2]["idduplicada"] = processed_rows[idx1].get(key_map.get("idnoticia"), "")
    return processed_rows


# ======================================
# Procesamiento del Dossier
# ======================================
def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({
        "titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"),
        "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"),
        "tonoiai": norm_key("Tono IA"), "tema": norm_key("Tema"),
        "subtema": norm_key("Subtema"), "idnoticia": norm_key("ID Noticia"),
        "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"),
        "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"),
        "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region")
    })
    rows, split_rows = [], []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)})
    for r_cells in rows:
        base = {}
        for k, v in r_cells.items():
            if k in [key_map["link_nota"], key_map["link_streaming"]]:
                base[k] = extract_link(v)
            else: base[k] = v.value
        if key_map.get("tipodemedio") in base:
            base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in m_list or [None]:
            new = deepcopy(base)
            if m: new[key_map["menciones"]] = m
            split_rows.append(new)
    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]:
            row.update({key_map["tonoiai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["subtema"]: "Duplicada"})
    return processed_rows, key_map


def fix_links_by_media_type(row, key_map):
    tkey, ln_key, ls_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key): return
    tipo = row.get(tkey, "")
    ln = row.get(ln_key) or {"value": "", "url": None}
    ls = row.get(ls_key) or {"value": "", "url": None}
    has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
    if tipo in ["Radio", "Televisión"]: row[ls_key] = {"value": "", "url": None}
    elif tipo == "Internet": row[ln_key], row[ls_key] = ls, ln
    elif tipo in ["Prensa", "Revista"]:
        if not has_url(ln) and has_url(ls): row[ln_key] = ls
        row[ls_key] = {"value": "", "url": None}


# ======================================
# Generación de Excel
# ======================================
def generate_output_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"
    final_order = [
        "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Region",
        "Seccion - Programa", "Titulo", "Autor - Conductor", "Nro. Pagina",
        "Dimension", "Duracion - Nro. Caracteres", "CPE", "Audiencia", "Tier",
        "Tono", "Tono IA", "Tema", "Subtema", "Link Nota",
        "Resumen - Aclaracion", "Link (Streaming - Imagen)",
        "Menciones - Empresa", "ID duplicada"
    ]
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension",
                       "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    out_sheet.append(final_order)
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    if "Hyperlink_Custom" not in out_wb.style_names: out_wb.add_named_style(link_style)
    for row_data in all_processed_rows:
        tk = key_map.get("titulo")
        if tk and tk in row_data: row_data[tk] = clean_title_for_output(row_data.get(tk))
        rk = key_map.get("resumen")
        if rk and rk in row_data: row_data[rk] = corregir_texto(row_data.get(rk))
        rta, lta = [], {}
        for ci, header in enumerate(final_order, 1):
            nkh = norm_key(header)
            dk = key_map.get(nkh, nkh)
            val = row_data.get(dk)
            cv = None
            if header in numeric_columns:
                try: cv = float(val) if val is not None and str(val).strip() != "" else None
                except (ValueError, TypeError): cv = str(val) if val is not None else None
            elif isinstance(val, dict) and "url" in val:
                cv = val.get("value", "Link")
                if val.get("url"): lta[ci] = val["url"]
            elif val is not None: cv = str(val)
            rta.append(cv)
        out_sheet.append(rta)
        for ci, url in lta.items():
            cell = out_sheet.cell(row=out_sheet.max_row, column=ci)
            cell.hyperlink = url; cell.style = "Hyperlink_Custom"
    output = io.BytesIO()
    out_wb.save(output)
    return output.getvalue()


# ======================================
# Proceso principal completo
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file,
                                  brand_name, brand_aliases,
                                  tono_pkl_file, tema_pkl_file, analysis_mode):
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0
    start_time = time.time()
    if "API" in analysis_mode:
        _setup_openai_key()

    with st.status("**Paso 1/5** · Limpieza y duplicados", expanded=True) as s:
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="✓ **Paso 1/5** · Limpieza completada", state="complete")

    with st.status("**Paso 2/5** · Mapeos y normalización", expanded=True) as s:
        df_region = pd.read_excel(region_file)
        region_map = {str(k).lower().strip(): v for k, v in
                      pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
        df_internet = pd.read_excel(internet_file)
        internet_map = {str(k).lower().strip(): v for k, v in
                        pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
        for row in all_processed_rows:
            omk = str(row.get(key_map.get("medio"), "")).lower().strip()
            row[key_map.get("region")] = region_map.get(omk, "N/A")
            if omk in internet_map:
                row[key_map.get("medio")] = internet_map[omk]
                row[key_map.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, key_map)
        s.update(label="✓ **Paso 2/5** · Mapeos aplicados", state="complete")

    gc.collect()
    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]

    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        df_temp["resumen_api"] = df_temp[key_map["titulo"]].fillna("").astype(str) + ". " + \
                                  df_temp[key_map["resumen"]].fillna("").astype(str)

        with st.status("**Paso 3/5** · Análisis de tono", expanded=True) as s:
            p_bar = st.progress(0)
            if ("PKL" in analysis_mode) and tono_pkl_file:
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
                p_bar.progress(1.0, "Tono (PKL) completado")
            elif "API" in analysis_mode:
                ct = ClasificadorTonoV8(brand_name, brand_aliases)
                resultados_tono = await ct.procesar_lote_async(
                    df_temp["resumen_api"], p_bar, df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
            else:
                resultados_tono = [{"tono": "N/A"}] * len(rows_to_analyze)
                p_bar.progress(1.0)
            df_temp[key_map["tonoiai"]] = [r["tono"] for r in resultados_tono]
            s.update(label="✓ **Paso 3/5** · Tono analizado", state="complete")

        with st.status("**Paso 4/5** · Tema y subtema", expanded=True) as s:
            p_bar = st.progress(0)
            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A"] * len(rows_to_analyze)
                temas_principales = ["N/A"] * len(rows_to_analyze)
            else:
                cs = ClasificadorSubtemaV8(brand_name, brand_aliases)
                subtemas = cs.procesar_lote(df_temp["resumen_api"], p_bar,
                                            df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
                temas_principales = consolidar_subtemas_en_temas(
                    subtemas, df_temp["resumen_api"].tolist(), p_bar,
                    marca=brand_name, aliases=brand_aliases)

            df_temp[key_map["subtema"]] = subtemas

            # === LÓGICA MEJORADA DE COHERENCIA TEMA-SUBTEMA ===
            if ("PKL" in analysis_mode) and tema_pkl_file:
                temas_pkl = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                if temas_pkl:
                    # Unificar: mismo subtema → mismo tema (voto mayoritario + similitud)
                    temas_coherentes = unificar_temas_por_similitud(
                        temas_pkl, subtemas, df_temp["resumen_api"].tolist()
                    )
                    df_temp[key_map["tema"]] = temas_coherentes
            else:
                # Sin PKL: también unificar por coherencia
                temas_coherentes = unificar_temas_por_similitud(
                    temas_principales, subtemas, df_temp["resumen_api"].tolist()
                )
                df_temp[key_map["tema"]] = temas_coherentes

            s.update(label="✓ **Paso 4/5** · Clasificación completada", state="complete")

        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"):
                row.update(results_map.get(row["original_index"], {}))

    gc.collect()
    ci = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    co = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    cost_str = f"${ci + co + ce:.4f} USD"

    with st.status("**Paso 5/5** · Generando informe", expanded=True) as s:
        dur = f"{time.time() - start_time:.0f}s"
        st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_IA_{brand_name.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name": brand_name, "brand_aliases": brand_aliases,
            "total_rows": len(all_processed_rows), "unique_rows": len(rows_to_analyze),
            "duplicates": len(all_processed_rows) - len(rows_to_analyze),
            "process_duration": dur, "process_cost": cost_str
        })
        s.update(label="✓ **Paso 5/5** · Proceso completado", state="complete")


# ======================================
# Análisis Rápido
# ======================================
async def run_quick_analysis_async(df, title_col, summary_col, brand_name, aliases):
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0
    df['texto_analisis'] = df[title_col].fillna('').astype(str) + ". " + df[summary_col].fillna('').astype(str)

    with st.status("**Paso 1/2** · Analizando tono", expanded=True) as s:
        p_bar = st.progress(0, "Iniciando...")
        ct = ClasificadorTonoV8(brand_name, aliases)
        rt = await ct.procesar_lote_async(df["texto_analisis"], p_bar,
                                           df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Tono IA'] = [r["tono"] for r in rt]
        s.update(label="✓ **Paso 1/2** · Tono analizado", state="complete")

    with st.status("**Paso 2/2** · Tema y subtema", expanded=True) as s:
        p_bar = st.progress(0, "Generando subtemas...")
        cs = ClasificadorSubtemaV8(brand_name, aliases)
        subtemas = cs.procesar_lote(df["texto_analisis"], p_bar,
                                    df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Subtema'] = subtemas
        pt = st.progress(0, "Consolidando temas...")
        temas = consolidar_subtemas_en_temas(subtemas, df["texto_analisis"].tolist(), pt,
                                              marca=brand_name, aliases=aliases)
        # Coherencia tema-subtema
        temas_coherentes = unificar_temas_por_similitud(temas, subtemas, df["texto_analisis"].tolist())
        df['Tema'] = temas_coherentes
        s.update(label="✓ **Paso 2/2** · Clasificación finalizada", state="complete")

    df.drop(columns=['texto_analisis'], inplace=True)
    ci = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    co = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    st.session_state['quick_cost'] = f"${ci + co + ce:.4f} USD"
    return df


def generate_quick_analysis_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Analisis')
    return output.getvalue()


def render_quick_analysis_tab():
    render_section_header("Análisis Rápido", "Suba un archivo y obtenga tono, tema y subtema")

    if 'quick_analysis_result' in st.session_state:
        render_completion_card("Análisis Completado",
                               f"Costo estimado: {st.session_state.get('quick_cost', '$0.00')}")
        st.dataframe(st.session_state.quick_analysis_result.head(10), use_container_width=True)
        ed = generate_quick_analysis_excel(st.session_state.quick_analysis_result)
        st.download_button(label="DESCARGAR RESULTADOS", data=ed,
                           file_name="Analisis_Rapido_IA.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True, type="primary")
        if st.button("NUEVO ANÁLISIS", use_container_width=True):
            for key in ['quick_analysis_result', 'quick_analysis_df', 'quick_file_name', 'quick_cost']:
                if key in st.session_state: del st.session_state[key]
            st.rerun()
        return

    if 'quick_analysis_df' not in st.session_state:
        st.markdown("")
        qf = st.file_uploader("Suba su archivo Excel", type=["xlsx"],
                               label_visibility="collapsed", key="quick_uploader")
        if qf:
            with st.spinner("Leyendo archivo..."):
                try:
                    st.session_state.quick_analysis_df = pd.read_excel(qf)
                    st.session_state.quick_file_name = qf.name; st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}"); st.stop()
    else:
        render_info_box(f"Archivo <strong>{st.session_state.quick_file_name}</strong> cargado correctamente.")
        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df
            columns = df.columns.tolist()
            col1, col2 = st.columns(2)
            title_col = col1.selectbox("Columna Título", options=columns, index=0)
            si = 1 if len(columns) > 1 else 0
            summary_col = col2.selectbox("Columna Resumen", options=columns, index=si)
            st.markdown("---")
            render_section_header("Configuración de Marca", "")
            pkl_for_brand = st.file_uploader("PKL para detectar marca (opcional)", type=["pkl"], key="quick_pkl_brand")
            db = ""
            if pkl_for_brand:
                det = extraer_marca_de_pkl(pkl_for_brand)
                if det: db = det; st.success(f"Marca detectada: **{det}**")
            brand_name = st.text_input("Marca Principal", value=db, placeholder="Ej: Siemens")
            bat = st.text_area("Alias (separados por ;)", placeholder="Ej: Siemens Healthineers; Siemens Energy", height=80)
            if st.form_submit_button("ANALIZAR", use_container_width=True, type="primary"):
                if not brand_name: st.error("Falta nombre de marca.")
                else:
                    _setup_openai_key()
                    aliases = [a.strip() for a in bat.split(";") if a.strip()]
                    with st.spinner("Analizando..."):
                        st.session_state.quick_analysis_result = asyncio.run(
                            run_quick_analysis_async(df.copy(), title_col, summary_col, brand_name, aliases))
                    st.rerun()
        if st.button("← CARGAR OTRO ARCHIVO"):
            for key in ['quick_analysis_df', 'quick_file_name', 'quick_analysis_result', 'quick_cost']:
                if key in st.session_state: del st.session_state[key]
            st.rerun()


# ======================================
# Main UI
# ======================================
def main():
    load_custom_css()
    if not check_password():
        return

    render_masthead()

    tab1, tab2 = st.tabs(["ANÁLISIS COMPLETO", "ANÁLISIS RÁPIDO"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("input_form"):
                render_section_header("Archivos de Entrada", "Suba los tres archivos requeridos")
                col1, col2, col3 = st.columns(3)
                dossier_file = col1.file_uploader("Dossier (.xlsx)", type=["xlsx"])
                region_file = col2.file_uploader("Región (.xlsx)", type=["xlsx"])
                internet_file = col3.file_uploader("Internet (.xlsx)", type=["xlsx"])

                render_section_header("Configuración de Marca", "")
                brand_name = st.text_input("Marca Principal", placeholder="Ej: Bancolombia", key="main_brand_name")
                brand_aliases_text = st.text_area("Alias (sep. por ;)", placeholder="Ej: Ban;Juan Carlos Mora",
                                                   height=80, key="main_brand_aliases")

                render_section_header("Modo de Análisis", "")
                analysis_mode = st.radio("Seleccione:",
                                          options=["Híbrido (PKL + API)", "Solo Modelos PKL", "API de OpenAI"],
                                          index=0, key="analysis_mode_radio", horizontal=True)
                tono_pkl_file, tema_pkl_file = None, None
                if "PKL" in analysis_mode:
                    c1, c2 = st.columns(2)
                    tono_pkl_file = c1.file_uploader("sentimiento.pkl", type=["pkl"])
                    tema_pkl_file = c2.file_uploader("tema.pkl", type=["pkl"])

                if st.form_submit_button("INICIAR ANÁLISIS", use_container_width=True, type="primary"):
                    if not all([dossier_file, region_file, internet_file, brand_name.strip()]):
                        st.error("Faltan datos obligatorios.")
                    else:
                        _setup_openai_key()
                        aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(
                            dossier_file, region_file, internet_file,
                            brand_name, aliases, tono_pkl_file, tema_pkl_file, analysis_mode))
                        st.rerun()
        else:
            render_section_header("Análisis Completado", "Resultados listos para descarga")
            render_metrics([
                {"value": st.session_state.total_rows, "label": "Total Noticias", "accent": ""},
                {"value": st.session_state.unique_rows, "label": "Únicas", "accent": "accent-green"},
                {"value": st.session_state.duplicates, "label": "Duplicadas", "accent": "accent-amber"},
                {"value": st.session_state.process_duration, "label": "Tiempo", "accent": "accent-blue"},
                {"value": st.session_state.get("process_cost", "$0.00"), "label": "Costo Est.", "accent": "accent-red"},
            ])
            render_completion_card(
                "Informe Generado",
                f"Marca: {st.session_state.brand_name} · {st.session_state.unique_rows} noticias analizadas"
            )
            st.download_button("DESCARGAR INFORME",
                               data=st.session_state.output_data,
                               file_name=st.session_state.output_filename,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True, type="primary")
            if st.button("NUEVO ANÁLISIS", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()

    with tab2:
        render_quick_analysis_tab()

    render_footer()


if __name__ == "__main__":
    main()
