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
import openai
import re
import time
from unidecode import unidecode
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer
import json
import asyncio
import hashlib
from typing import List, Dict, Tuple, Optional, Any
import joblib
import gc

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias · IA",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Modelos
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

# Configuración de rendimiento y umbrales
CONCURRENT_REQUESTS = 50
SIMILARITY_THRESHOLD_TONO = 0.92
SIMILARITY_THRESHOLD_TITULOS = 0.95
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 150

# Configuración de agrupación - MEJORADA PARA PRECISIÓN
NUM_TEMAS_PRINCIPALES = 20
# Umbral más estricto para evitar fusiones incorrectas (era 0.85)
UMBRAL_FUSION_CONTENIDO = 0.88
# Umbral mínimo de similitud para agrupar subtemas (nuevo)
UMBRAL_CLUSTERING_SUBTEMAS = 0.78
# Umbral de similitud para clustering inicial de noticias (nuevo)
UMBRAL_CLUSTERING_NOTICIAS = 0.80

# Precios (Por 1 millón de tokens)
PRICE_INPUT_1M = 0.10
PRICE_OUTPUT_1M = 0.40
PRICE_EMBEDDING_1M = 0.02

# Inicializar contadores de tokens de forma segura
if 'tokens_input' not in st.session_state: st.session_state['tokens_input'] = 0
if 'tokens_output' not in st.session_state: st.session_state['tokens_output'] = 0
if 'tokens_embedding' not in st.session_state: st.session_state['tokens_embedding'] = 0
# Cache de embeddings en sesión para evitar re-cálculos
if '_emb_cache' not in st.session_state: st.session_state['_emb_cache'] = {}

# Listas Geográficas (Abreviadas)
CIUDADES_COLOMBIA = { "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo", "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu", "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico", "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila", "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío", "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca", "vaupés", "vaupes", "vichada"}
GENTILICIOS_COLOMBIA = {"bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos", "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos", "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros", "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"}

# ======================================
# Lexicos y patrones
# ======================================
STOPWORDS_ES = set(""" a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada """.split())
POS_VARIANTS = [ r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)", r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)", r"apertur(a|ar|ara|o|an)", r"estren(a|o|ara|an|ando)", r"habilit(a|o|ara|an|ando)", r"disponible", r"mejor(a|o|an|ando)", r"optimiza|amplia|expande", r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?", r"crecimi?ento|aument(a|o|an|ando)", r"gananci(a|as)|utilidad(es)?|benefici(o|os)", r"expansion|crece|crecer", r"inversion|invierte|invertir", r"innova(cion|dor|ndo)|moderniza", r"exito(so|sa)?|logr(o|os|a|an|ando)", r"reconoci(miento|do|da)|premi(o|os|ada)", r"lidera(zgo)?|lider", r"consolida|fortalece", r"oportunidad(es)?|potencial", r"solucion(es)?|resuelve", r"eficien(te|cia)", r"calidad|excelencia", r"satisfaccion|complace", r"confianza|credibilidad", r"sostenible|responsable", r"compromiso|apoya|apoyar", r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)", r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)", r"destaca(r|do|da|ndo)?", r"supera(r|ndo|cion)?", r"record|hito|milestone", r"avanza(r|do|da|ndo)?", r"benefici(a|o|ando|ar|ando)", r"importante(s)?", r"prioridad", r"bienestar", r"garantizar", r"seguridad", r"atencion", r"expres(o|ó|ando)", r"señala(r|do|ando)", r"ratific(a|o|ando|ar)"]
NEG_VARIANTS = [r"demanda|denuncia|sanciona|multa|investiga|critica", r"cae|baja|pierde|crisis|quiebra|default", r"fraude|escandalo|irregularidad", r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga", r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora", r"problema(s|tica|ico)?|dificultad(es)?", r"retras(o|a|ar|ado)", r"perdida(s)?|deficit", r"conflict(o|os)?|disputa(s)?", r"rechaz(a|o|ar|ado)", r"negativ(o|a|os|as)", r"preocupa(cion|nte|do)?", r"alarma(nte)?|alerta", r"riesgo(s)?|amenaza(s)?"]
CRISIS_KEYWORDS = re.compile(r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS = re.compile(r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)
POS_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]

# ======================================
# CSS Moderno - Estilo Claude Light
# ======================================
def load_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ── Reset & Base ── */
    html, body, .stApp {
        font-family: 'Sora', sans-serif;
        background-color: #F7F6F3;
        color: #1A1A1A;
    }
    
    /* ── Main layout ── */
    .main .block-container {
        padding: 2rem 3rem 4rem 3rem;
        max-width: 1200px;
    }

    /* ── Header ── */
    .app-header {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        padding: 2.5rem 0 2rem 0;
        border-bottom: 1.5px solid #E5E3DD;
        margin-bottom: 2.5rem;
    }
    .app-header-eyebrow {
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #CF6641;
        margin-bottom: 0.5rem;
    }
    .app-header-title {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1A1A1A;
        line-height: 1.2;
        margin: 0;
    }
    .app-header-sub {
        margin-top: 0.5rem;
        font-size: 0.95rem;
        color: #6B6860;
        font-weight: 300;
    }

    /* ── Section labels ── */
    .section-label {
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #9E9A94;
        margin-bottom: 0.75rem;
        margin-top: 1.75rem;
        display: block;
    }

    /* ── Cards ── */
    .card {
        background: #FFFFFF;
        border: 1px solid #E5E3DD;
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    .card-accent {
        border-left: 3px solid #CF6641;
    }

    /* ── Metric row ── */
    .metrics-row {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 0.75rem;
        margin: 2rem 0;
    }
    .metric-item {
        background: #FFFFFF;
        border: 1px solid #E5E3DD;
        border-radius: 10px;
        padding: 1.25rem 1rem;
        text-align: center;
    }
    .metric-item-val {
        font-size: 1.6rem;
        font-weight: 700;
        color: #1A1A1A;
        line-height: 1;
        font-family: 'JetBrains Mono', monospace;
    }
    .metric-item-val.green { color: #2B7A3E; }
    .metric-item-val.orange { color: #C0680B; }
    .metric-item-val.blue { color: #2563EB; }
    .metric-item-val.red { color: #B91C1C; }
    .metric-item-lbl {
        font-size: 0.7rem;
        color: #9E9A94;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-top: 0.4rem;
        font-weight: 500;
    }

    /* ── Buttons ── */
    .stButton > button {
        font-family: 'Sora', sans-serif !important;
        font-weight: 500 !important;
        border-radius: 7px !important;
        font-size: 0.9rem !important;
        letter-spacing: 0.01em !important;
        transition: all 0.15s ease !important;
    }
    .stButton > button[kind="primary"] {
        background: #1A1A1A !important;
        color: #FFFFFF !important;
        border: none !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: #333333 !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15) !important;
    }
    .stDownloadButton > button {
        background: #1A1A1A !important;
        color: #FFFFFF !important;
        border-radius: 7px !important;
        font-family: 'Sora', sans-serif !important;
        font-weight: 500 !important;
    }

    /* ── Form inputs ── */
    .stTextInput input, .stTextArea textarea, .stSelectbox select {
        font-family: 'Sora', sans-serif !important;
        border-radius: 7px !important;
        border: 1px solid #E5E3DD !important;
        background: #FAFAF8 !important;
        font-size: 0.9rem !important;
    }
    .stTextInput input:focus, .stTextArea textarea:focus {
        border-color: #CF6641 !important;
        box-shadow: 0 0 0 3px rgba(207, 102, 65, 0.1) !important;
    }

    /* ── File uploader ── */
    [data-testid="stFileUploadDropzone"] {
        border: 1.5px dashed #D9D6CF !important;
        border-radius: 9px !important;
        background: #FAFAF8 !important;
    }
    [data-testid="stFileUploadDropzone"]:hover {
        border-color: #CF6641 !important;
        background: #FFF8F5 !important;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: transparent;
        border-bottom: 1.5px solid #E5E3DD;
    }
    .stTabs [data-baseweb="tab"] {
        font-family: 'Sora', sans-serif;
        font-size: 0.88rem;
        font-weight: 500;
        color: #6B6860;
        padding: 0.7rem 1.5rem;
        border-radius: 0;
        background: transparent;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        color: #1A1A1A;
        border-bottom: 2px solid #CF6641;
        font-weight: 600;
    }

    /* ── Status boxes ── */
    [data-testid="stStatusWidget"] {
        border-radius: 9px !important;
        border: 1px solid #E5E3DD !important;
    }

    /* ── Info / success alerts ── */
    .stAlert {
        border-radius: 8px !important;
        font-family: 'Sora', sans-serif !important;
        font-size: 0.88rem !important;
    }

    /* ── Radio ── */
    .stRadio label {
        font-family: 'Sora', sans-serif !important;
        font-size: 0.9rem !important;
    }

    /* ── Progress bar ── */
    .stProgress > div > div {
        background-color: #CF6641 !important;
        border-radius: 99px !important;
    }
    .stProgress > div {
        border-radius: 99px !important;
        background: #F0EDE8 !important;
    }

    /* ── Divider ── */
    hr {
        border: none;
        border-top: 1px solid #E5E3DD;
        margin: 2rem 0;
    }

    /* ── Footer ── */
    .app-footer {
        text-align: center;
        color: #C3BFB9;
        font-size: 0.75rem;
        margin-top: 3rem;
        padding-top: 1.5rem;
        border-top: 1px solid #E5E3DD;
    }

    /* ── Success banner ── */
    .success-banner {
        background: #F0FBF3;
        border: 1px solid #6FCF97;
        border-radius: 10px;
        padding: 1.25rem 1.5rem;
        margin: 1.5rem 0;
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }

    /* ── Tag chip ── */
    .tag-chip {
        display: inline-block;
        background: #F0EDE8;
        color: #5C5852;
        font-size: 0.72rem;
        font-weight: 500;
        padding: 0.2rem 0.6rem;
        border-radius: 99px;
        letter-spacing: 0.04em;
    }

    /* ── Password screen ── */
    .login-wrap {
        max-width: 400px;
        margin: 4rem auto;
        background: #FFFFFF;
        border: 1px solid #E5E3DD;
        border-radius: 14px;
        padding: 2.5rem;
    }
    .login-title {
        font-size: 1.3rem;
        font-weight: 700;
        color: #1A1A1A;
        margin-bottom: 0.3rem;
    }
    .login-sub {
        font-size: 0.85rem;
        color: #9E9A94;
        margin-bottom: 1.5rem;
    }

    /* Hide streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }
    </style>
    """, unsafe_allow_html=True)


# ======================================
# Autenticacion y Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    st.markdown("""
    <div class="login-wrap">
        <div class="login-title">📰 Acceso al Portal</div>
        <div class="login-sub">Introduce tu contraseña para continuar</div>
    </div>
    """, unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("Contraseña", type="password", placeholder="••••••••")
            if st.form_submit_button("Ingresar →", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True
                    st.success("Acceso autorizado.")
                    time.sleep(1); st.rerun()
                else:
                    st.error("Contraseña incorrecta")
    return False

def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay); delay *= 2

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            await asyncio.sleep(delay); delay *= 2

def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def limpiar_tema(tema: str) -> str:
    if not tema: return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    if tema: tema = tema[0].upper() + tema[1:]
    invalid_words = ["en","de","del","la","el","y","o","con","sin","por","para","sobre"]
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_words: palabras.pop()
    tema = " ".join(palabras)
    if len(tema.split()) > 6: tema = " ".join(tema.split()[:6])
    return tema if tema else "Sin tema"

def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema: return "Sin tema"
    tema_lower = tema.lower()
    all_brand_names = [marca.lower()] + [alias.lower() for alias in aliases if alias]
    for brand_name in all_brand_names:
        tema_lower = re.sub(rf'\b{re.escape(brand_name)}\b', '', tema_lower, flags=re.IGNORECASE)
        tema_lower = re.sub(rf'\b{re.escape(unidecode(brand_name))}\b', '', tema_lower, flags=re.IGNORECASE)
    for ciudad in CIUDADES_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(ciudad)}\b', '', tema_lower, flags=re.IGNORECASE)
    for gentilicio in GENTILICIOS_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(gentilicio)}\b', '', tema_lower, flags=re.IGNORECASE)
    frases_geograficas = ["en colombia", "de colombia", "del pais", "en el pais", "nacional", "colombiano", "colombiana", "colombianos", "colombianas", "territorio nacional"]
    for frase in frases_geograficas:
        tema_lower = re.sub(rf'\b{re.escape(frase)}\b', '', tema_lower, flags=re.IGNORECASE)
    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
    if not palabras: return "Sin tema"
    tema_limpio = " ".join(palabras)
    if tema_limpio: tema_limpio = tema_limpio[0].upper() + tema_limpio[1:]
    return limpiar_tema(tema_limpio)

def string_norm_label(s: str) -> str:
    if not s: return ""
    s = unidecode(s.lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join([t for t in s.split() if t not in STOPWORDS_ES])

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str): return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    cleaned = tmp[0] if tmp else title
    return re.sub(r"\W+", " ", cleaned).lower().strip()

def clean_title_for_output(title: Any) -> str:
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()

def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str): return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match: text = text[match.start():]
    if text and not text.endswith("..."): text = text.rstrip(".") + "..."
    return text

def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {
        "fm": "Radio", "am": "Radio", "radio": "Radio",
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa",
        "revista": "Revista", "revistas": "Revista",
        "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"
    }
    default_value = str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro"
    return mapping.get(t, default_value)


# ======================================
# Cache de embeddings en sesión
# ======================================
def _get_cache_key(text: str) -> str:
    return hashlib.md5(text[:500].encode()).hexdigest()


# ======================================
# Función de Embeddings con caché de sesión
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos: return []
    resultados = [None] * len(textos)
    cache = st.session_state.get('_emb_cache', {})

    # Separar cached vs. no-cached
    indices_to_fetch = []
    for i, t in enumerate(textos):
        key = _get_cache_key(t if t else "")
        if key in cache:
            resultados[i] = cache[key]
        else:
            indices_to_fetch.append(i)

    if not indices_to_fetch:
        return resultados

    # Fetch solo los no-cacheados
    textos_to_fetch = [textos[i] for i in indices_to_fetch]

    for batch_start in range(0, len(textos_to_fetch), batch_size):
        batch_idx_in_fetch = list(range(batch_start, min(batch_start + batch_size, len(textos_to_fetch))))
        batch = [textos_to_fetch[i] for i in batch_idx_in_fetch]
        batch_truncado = [t[:2000] if t else "" for t in batch]
        try:
            resp = call_with_retries(openai.Embedding.create, input=batch_truncado, model=OPENAI_MODEL_EMBEDDING)
            if isinstance(resp, dict):
                usage = resp.get('usage', {})
            else:
                usage = getattr(resp, 'usage', {})
            if usage:
                total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                st.session_state['tokens_embedding'] += total
            for j, emb_data in enumerate(resp["data"]):
                orig_idx = indices_to_fetch[batch_idx_in_fetch[j]]
                emb = emb_data["embedding"]
                resultados[orig_idx] = emb
                cache[_get_cache_key(textos[orig_idx])] = emb
        except Exception:
            for j in batch_idx_in_fetch:
                orig_idx = indices_to_fetch[j]
                try:
                    resp = openai.Embedding.create(input=[textos_to_fetch[j][:2000]], model=OPENAI_MODEL_EMBEDDING)
                    if isinstance(resp, dict):
                        usage = resp.get('usage', {})
                    else:
                        usage = getattr(resp, 'usage', {})
                    if usage:
                        total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                        st.session_state['tokens_embedding'] += total
                    emb = resp["data"][0]["embedding"]
                    resultados[orig_idx] = emb
                    cache[_get_cache_key(textos[orig_idx])] = emb
                except:
                    resultados[orig_idx] = None

    st.session_state['_emb_cache'] = cache
    return resultados


# ======================================
# Agrupación Genérica
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    if not textos: return {}
    embs = get_embeddings_batch(textos)
    valid_indices = [i for i, e in enumerate(embs) if e is not None]
    if len(valid_indices) < 2: return {}
    emb_matrix = np.array([embs[i] for i in valid_indices])
    clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=1 - umbral_similitud, metric="cosine", linkage="average").fit(emb_matrix)
    grupos = defaultdict(list)
    for i, label in enumerate(clustering.labels_): grupos[label].append(valid_indices[i])
    return {gid: g for gid, g in enumerate(grupos.values())}

def agrupar_por_titulo_similar(titulos: List[str]) -> Dict[int, List[int]]:
    gid, grupos, used = 0, {}, set()
    norm_titles = [normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(norm_titles)):
        if i in used or not norm_titles[i]: continue
        grupo_actual = [i]
        used.add(i)
        for j in range(i + 1, len(norm_titles)):
            if j in used or not norm_titles[j]: continue
            if SequenceMatcher(None, norm_titles[i], norm_titles[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                grupo_actual.append(j)
                used.add(j)
        if len(grupo_actual) >= 2: grupos[gid], gid = grupo_actual, gid + 1
    return grupos

def seleccionar_representante(indices: List[int], textos: List[str]) -> Tuple[int, str]:
    subset_textos = [textos[i] for i in indices]
    embs = get_embeddings_batch(subset_textos)
    valid_indices, valid_embs = [], []
    for idx_in_subset, emb in enumerate(embs):
        if emb is not None:
            valid_embs.append(emb)
            valid_indices.append(indices[idx_in_subset])
    if not valid_embs: return indices[0], textos[indices[0]]
    M = np.array(valid_embs)
    centro = M.mean(axis=0, keepdims=True)
    sims = cosine_similarity(M, centro).reshape(-1)
    best_idx_in_valid = int(np.argmax(sims))
    return valid_indices[best_idx_in_valid], textos[valid_indices[best_idx_in_valid]]


# ======================================
# CLASIFICADOR DE TONO
# ======================================
class ClasificadorTonoUltraV3:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex(marca, aliases)

    def _build_brand_regex(self, marca: str, aliases: List[str]) -> str:
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        return r"\b(" + "|".join(patterns) + r")\b" if patterns else r"(a^b)"

    def _extract_brand_context_dynamic(self, texto: str) -> List[str]:
        texto_lower = unidecode(texto.lower())
        contextos = []
        matches = list(re.finditer(self.brand_pattern, texto_lower, re.IGNORECASE))
        if not matches: return [texto[:600]]
        for i, match in enumerate(matches):
            window = 250 if i == 0 else 150
            snippet_preview = texto_lower[max(0, match.start()-50):match.end()+50]
            if any(kw in snippet_preview for kw in ['lanza', 'anuncia', 'crisis', 'denuncia', 'innova']):
                window = 200
            start = max(0, match.start() - window)
            end = min(len(texto), match.end() + window)
            while end < len(texto) and texto[end] not in '.!?': end += 1
            contextos.append(texto[start:end+1].strip())
        return list(dict.fromkeys(contextos))[:4]

    def _analizar_contexto_reglas(self, contextos: List[str]) -> Optional[str]:
        pos_score, neg_score = 0, 0
        for contexto in contextos:
            t = unidecode(contexto.lower())
            tiene_negacion = bool(re.search(r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente)\b.{0,30}' + self.brand_pattern, t, re.IGNORECASE))
            pos_hits = sum(1 for p in POS_PATTERNS if p.search(t))
            neg_hits = sum(1 for p in NEG_PATTERNS if p.search(t))
            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(t):
                pos_score += 3; continue
            if tiene_negacion: pos_score -= pos_hits; neg_score += pos_hits
            else: pos_score += pos_hits; neg_score += neg_hits
        if pos_score >= 3 and pos_score > neg_score * 1.5: return "Positivo"
        elif neg_score >= 3 and neg_score > pos_score * 1.5: return "Negativo"
        return None

    async def _llm_refuerzo_mejorado(self, contextos: List[str]) -> Dict[str, str]:
        aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
        contextos_texto = "\n---\n".join(contextos[:3])
        prompt = f"""Eres un analista de medios experto. Analiza ÚNICAMENTE el sentimiento hacia la marca '{self.marca}' (alias: {aliases_str}) en estos fragmentos.
Positivo: logros, lanzamientos, reconocimientos, respuestas a crisis.
Negativo: críticas, sanciones, pérdidas, escándalos.
Neutro: menciones informativas.
Fragmentos:
---
{contextos_texto}
---
Responde SOLO en JSON: {{"tono":"Positivo|Negativo|Neutro"}}"""
        try:
            resp = await acall_with_retries(openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": prompt}], max_tokens=50, temperature=0.0, response_format={"type": "json_object"})
            if isinstance(resp, dict):
                usage = resp.get('usage', {})
            else:
                usage = getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += pt
                st.session_state['tokens_output'] += ct
            data = json.loads(resp.choices[0].message.content.strip())
            tono = str(data.get("tono", "Neutro")).title()
            return {"tono": tono if tono in ["Positivo","Negativo","Neutro"] else "Neutro"}
        except Exception: return {"tono": "Neutro"}

    async def _clasificar_grupo_async(self, texto_representante: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            contextos = self._extract_brand_context_dynamic(texto_representante)
            tono_reglas = self._analizar_contexto_reglas(contextos)
            if tono_reglas: return {"tono": tono_reglas}
            return await self._llm_refuerzo_mejorado(contextos)

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar, resumen_puro: pd.Series, titulos_puros: pd.Series):
        textos, n = textos_concat.tolist(), len(textos_concat)
        progress_bar.progress(0.05, text="Agrupando noticias para análisis de tono…")
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i]); return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO), agrupar_por_titulo_similar(titulos_puros.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        representantes = {cid: seleccionar_representante(idxs, textos)[1] for cid, idxs in comp.items()}
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [self._clasificar_grupo_async(rep_texto, semaphore) for rep_texto in representantes.values()]
        resultados_brutos = []
        for i, f in enumerate(asyncio.as_completed(tasks)):
            resultados_brutos.append(await f)
            progress_bar.progress(0.1 + 0.85 * (i + 1) / len(tasks), text=f"Analizando tono: {i+1}/{len(tasks)}")
        resultados_por_grupo = {list(representantes.keys())[i]: res for i, res in enumerate(resultados_brutos)}
        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            r = resultados_por_grupo.get(cid, {"tono": "Neutro"})
            for i in idxs: resultados_finales[i] = r
        progress_bar.progress(1.0, text="Análisis de tono completado")
        return resultados_finales

def analizar_tono_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[Dict[str, str]]]:
    try:
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        TONO_MAP = {1: "Positivo", "1": "Positivo", 0: "Neutro", "0": "Neutro", -1: "Negativo", "-1": "Negativo"}
        return [{"tono": TONO_MAP.get(p, str(p).title())} for p in predicciones]
    except Exception as e:
        st.error(f"Error al procesar `pipeline_sentimiento.pkl`: {e}"); return None


# ======================================
# CLASIFICADOR DE SUBTEMAS V4 — ALTA PRECISIÓN
# ======================================
class ClasificadorSubtemaV4:
    """
    Mejoras clave vs V3:
    1. Texto de análisis = título (peso alto) + primeras 300 chars de resumen — más señal específica.
    2. TF-IDF híbrido para pre-filtrar grupos antes de embeddings (más rápido).
    3. Umbral de clustering más estricto configurable (UMBRAL_CLUSTERING_NOTICIAS).
    4. Validación post-clustering: cada noticia se verifica individualmente contra su grupo.
       Si la similitud individual es menor al umbral de confianza, queda como grupo único.
    5. Generación de subtema con contexto enriquecido (keywords TF-IDF + títulos).
    6. Anti-generalización: el prompt penaliza explícitamente etiquetas vagas.
    """

    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.cache_subtemas: Dict[str, str] = {}

    # ── 1. Texto de análisis compuesto ──────────────────────────────────────
    def _build_analysis_text(self, titulo: str, resumen: str) -> str:
        """Título x2 (mayor peso) + primeras 300 chars de resumen."""
        t = str(titulo).strip() if titulo else ""
        r = str(resumen).strip()[:300] if resumen else ""
        return f"{t} {t} {r}".strip()

    # ── 2. TF-IDF pre-agrupación (lexical) ──────────────────────────────────
    def _tfidf_grupos(self, textos: List[str]) -> Dict[int, List[int]]:
        """Agrupa por alta similitud léxica con TF-IDF — muy rápido, sin LLM."""
        if len(textos) < 2:
            return {}
        norm = [string_norm_label(t) for t in textos]
        try:
            vec = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_features=8000)
            matrix = vec.fit_transform(norm)
            sim = (matrix @ matrix.T).toarray()
            np.fill_diagonal(sim, 0)
        except Exception:
            return {}

        used = set()
        grupos: Dict[int, List[int]] = {}
        gid = 0
        for i in range(len(textos)):
            if i in used:
                continue
            # umbral alto para TF-IDF (solo agrupamos los muy similares)
            vecinos = [j for j in range(len(textos)) if j not in used and j != i and sim[i, j] >= 0.60]
            if vecinos:
                grupo = [i] + vecinos
                grupos[gid] = grupo
                used.update(grupo)
                gid += 1
        return grupos

    # ── 3. Clustering semántico estricto ────────────────────────────────────
    def _clustering_semantico(self, textos: List[str], titulos: List[str], indices: List[int]) -> Dict[int, List[int]]:
        """Clustering jerárquico con umbral estricto para evitar sobre-generalización."""
        if len(indices) < 2:
            return {}

        BATCH = 500
        grupos_finales: Dict[int, List[int]] = {}
        offset = 0

        for start in range(0, len(indices), BATCH):
            batch_idxs = indices[start:start + BATCH]
            # Texto compuesto con énfasis en título
            batch_txts = [self._build_analysis_text(titulos[i], textos[i]) for i in batch_idxs]
            embs = get_embeddings_batch(batch_txts)
            valid_embs, final_idxs = [], []
            for k, e in enumerate(embs):
                if e is not None:
                    valid_embs.append(e)
                    final_idxs.append(batch_idxs[k])
            if len(valid_embs) < 2:
                continue

            sim_matrix = cosine_similarity(np.array(valid_embs))
            # Umbral estricto: distance_threshold = 1 - UMBRAL_CLUSTERING_NOTICIAS
            clustering = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=1 - UMBRAL_CLUSTERING_NOTICIAS,
                metric='precomputed',
                linkage='average'
            ).fit(1 - np.clip(sim_matrix, 0, 1))

            grupos = defaultdict(list)
            for i, lbl in enumerate(clustering.labels_):
                grupos[lbl].append(final_idxs[i])

            for lbl, idxs in grupos.items():
                if len(idxs) >= 2:
                    grupos_finales[offset + lbl] = idxs
            offset += len(grupos)

        return grupos_finales

    # ── 4. Validación individual post-clustering ─────────────────────────────
    def _validar_miembros_grupo(
        self, grupo_idxs: List[int], textos: List[str], titulos: List[str], umbral_confianza: float = 0.72
    ) -> Dict[int, List[int]]:
        """
        Para cada grupo, calcula el centroide y verifica que cada miembro
        tenga similitud >= umbral_confianza. Los que no cumplen quedan solos.
        Retorna sub-grupos válidos.
        """
        if len(grupo_idxs) < 2:
            return {0: grupo_idxs}

        batch_txts = [self._build_analysis_text(titulos[i], textos[i]) for i in grupo_idxs]
        embs = get_embeddings_batch(batch_txts)
        valid_pairs = [(grupo_idxs[k], embs[k]) for k in range(len(grupo_idxs)) if embs[k] is not None]

        if len(valid_pairs) < 2:
            return {0: grupo_idxs}

        idxs_v, embs_v = zip(*valid_pairs)
        matrix = np.array(embs_v)
        centroide = matrix.mean(axis=0, keepdims=True)
        sims = cosine_similarity(matrix, centroide).reshape(-1)

        core = [idxs_v[k] for k in range(len(idxs_v)) if sims[k] >= umbral_confianza]
        outliers = [idxs_v[k] for k in range(len(idxs_v)) if sims[k] < umbral_confianza]

        result: Dict[int, List[int]] = {}
        if core:
            result[0] = core
        for i, idx in enumerate(outliers):
            result[i + 1] = [idx]
        return result

    # ── 5. Pre-agrupación rápida por texto idéntico ──────────────────────────
    def _preagrupar_identicos(self, textos, titulos, resumenes):
        n = len(textos)
        grupos: Dict[int, List[int]] = {}
        usado = set()
        gid = 0

        def norm_rapido(texto):
            if not texto: return ""
            t = unidecode(str(texto).lower())
            t = re.sub(r'[^a-z0-9\s]', '', t)
            return ' '.join(t.split()[:40])

        titulos_norm = [norm_rapido(t) for t in titulos]
        resumenes_norm = [norm_rapido(r) for r in resumenes]

        def hash_t(s): return hashlib.md5(s.encode()).hexdigest()

        titulo_idx = defaultdict(list)
        for i, t in enumerate(titulos_norm):
            if t: titulo_idx[hash_t(t)].append(i)

        resumen_idx = defaultdict(list)
        for i, r in enumerate(resumenes_norm):
            if r: resumen_idx[hash_t(r[:100])].append(i)

        for indices in titulo_idx.values():
            if len(indices) >= 2 and not any(i in usado for i in indices):
                grupos[gid] = indices; usado.update(indices); gid += 1

        for indices in resumen_idx.values():
            nuevos = [i for i in indices if i not in usado]
            if len(nuevos) >= 2:
                grupos[gid] = nuevos; usado.update(nuevos); gid += 1

        return grupos

    # ── 6. Generación de etiqueta de subtema ────────────────────────────────
    def _generar_subtema(self, textos_muestra: List[str], titulos_muestra: List[str]) -> str:
        cache_key = hashlib.md5("|".join(sorted(
            [normalize_title_for_comparison(t) for t in titulos_muestra[:3]]
        )).encode()).hexdigest()
        if cache_key in self.cache_subtemas:
            return self.cache_subtemas[cache_key]

        # Keywords TF-IDF sobre el conjunto de títulos
        palabras_titulos = []
        for t in titulos_muestra[:8]:
            palabras_titulos.extend([w for w in string_norm_label(t).split() if len(w) > 3])
        keywords = " | ".join([w for w, _ in Counter(palabras_titulos).most_common(6)])

        titulos_str = "\n".join([f"- {t[:120]}" for t in titulos_muestra[:6]])
        resumenes_str = "\n".join([f"- {r[:150]}" for r in textos_muestra[:3]])

        prompt = f"""Eres un editor de noticias experto. Genera un SUBTEMA periodístico CONCRETO (3-5 palabras) para estas noticias.

TÍTULOS:
{titulos_str}

EXTRACTOS DE RESUMEN:
{resumenes_str}

PALABRAS CLAVE: {keywords}

REGLAS ESTRICTAS:
- El subtema debe describir el EVENTO O ASUNTO ESPECÍFICO que comparten estas noticias
- NO uses la marca '{self.marca}', nombres de ciudades, ni gentilicios
- NO uses verbos vagos ni gerundios como inicio
- NO uses términos genéricos como "Actividad", "Gestión", "Temas", "Varios", "General", "Noticias"
- SÍ usa sustantivos concretos: Ej. "Apertura Sucursal Norte", "Incremento Tarifas Gas", "Premio Innovación Fintech"
- Si el grupo tiene pocas noticias y el tema es muy específico, sé más específico aún

JSON: {{"subtema":"..."}}"""

        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40,
                temperature=0.05,
                response_format={"type": "json_object"}
            )
            if isinstance(resp, dict):
                usage = resp.get('usage', {})
            else:
                usage = getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += pt
                st.session_state['tokens_output'] += ct
            raw = json.loads(resp.choices[0].message.content.strip()).get("subtema", "Actividad Corporativa")
            subtema = limpiar_tema_geografico(limpiar_tema(raw), self.marca, self.aliases)
        except Exception:
            subtema = "Actividad Corporativa"

        self.cache_subtemas[cache_key] = subtema
        return subtema

    # ── 7. Fusión post-proceso por similitud de contenido ───────────────────
    def _fusionar_grupos_por_contenido(self, etiquetas: List[str], textos: List[str]) -> List[str]:
        df_temp = pd.DataFrame({'label': etiquetas, 'text': textos})
        unique_labels = df_temp['label'].unique()
        if len(unique_labels) < 2:
            return etiquetas

        todos_embs = get_embeddings_batch(textos)
        label_centroids: Dict[str, np.ndarray] = {}
        valid_labels = []

        for label in unique_labels:
            indices = df_temp.index[df_temp['label'] == label].tolist()
            vectors = [todos_embs[i] for i in indices[:50] if todos_embs[i] is not None]
            if vectors:
                label_centroids[label] = np.mean(vectors, axis=0)
                valid_labels.append(label)

        if len(valid_labels) < 2:
            return etiquetas

        matrix = np.array([label_centroids[l] for l in valid_labels])
        sim_matrix = cosine_similarity(matrix)
        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=1 - UMBRAL_FUSION_CONTENIDO,
            metric='precomputed',
            linkage='average'
        ).fit(1 - np.clip(sim_matrix, 0, 1))

        mapa_fusion: Dict[str, str] = {}
        for cluster_id in set(clustering.labels_):
            indices_cluster = [i for i, x in enumerate(clustering.labels_) if x == cluster_id]
            labels_in_cluster = [valid_labels[i] for i in indices_cluster]
            counts = Counter([l for l in etiquetas if l in labels_in_cluster])
            representante = max(labels_in_cluster, key=lambda x: (counts[x], -len(x)))
            for lbl in labels_in_cluster:
                mapa_fusion[lbl] = representante

        return [mapa_fusion.get(lbl, lbl) for lbl in etiquetas]

    # ── 8. Pipeline principal ────────────────────────────────────────────────
    def procesar_lote(
        self,
        df_columna_resumen: pd.Series,
        progress_bar,
        resumen_puro: pd.Series,
        titulos_puros: pd.Series
    ) -> List[str]:
        textos = df_columna_resumen.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumen_puro.tolist()
        n = len(textos)

        # Construir textos de análisis compuestos (título x2 + resumen truncado)
        textos_analisis = [self._build_analysis_text(titulos[i], resumenes[i]) for i in range(n)]

        # ── Paso A: Pre-agrupación por hash (idénticos) ──────────────────────
        progress_bar.progress(0.05, "Detectando noticias idénticas…")
        grupos_identicos = self._preagrupar_identicos(textos_analisis, titulos, resumenes)

        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                path = []
                while i != self.p[i]: path.append(i); i = self.p[i]
                for node in path: self.p[node] = i
                return i
            def union(self, i, j): self.p[self.find(j)] = self.find(i)

        dsu = DSU(n)
        for idxs in grupos_identicos.values():
            for j in idxs[1:]: dsu.union(idxs[0], j)

        # ── Paso B: TF-IDF lexical pre-clustering ────────────────────────────
        progress_bar.progress(0.12, "Agrupación léxica TF-IDF…")
        comp_prev = defaultdict(list)
        for i in range(n): comp_prev[dsu.find(i)].append(i)
        indices_sueltos_prev = [i for idxs in comp_prev.values() if len(idxs) == 1 for i in idxs]

        if len(indices_sueltos_prev) > 1:
            textos_sueltos = [textos_analisis[i] for i in indices_sueltos_prev]
            grupos_tfidf = self._tfidf_grupos(textos_sueltos)
            for local_idxs in grupos_tfidf.values():
                real_idxs = [indices_sueltos_prev[li] for li in local_idxs]
                for j in real_idxs[1:]: dsu.union(real_idxs[0], j)

        # ── Paso C: Clustering semántico (umbral estricto) ───────────────────
        progress_bar.progress(0.22, "Clustering semántico estricto…")
        comp_mid = defaultdict(list)
        for i in range(n): comp_mid[dsu.find(i)].append(i)
        indices_sueltos = [i for idxs in comp_mid.values() if len(idxs) == 1 for i in idxs]

        if len(indices_sueltos) > 1:
            grupos_cluster = self._clustering_semantico(textos_analisis, titulos, indices_sueltos)
            for idxs in grupos_cluster.values():
                for j in idxs[1:]: dsu.union(idxs[0], j)

        # ── Paso D: Validación individual (anti-ruido) ───────────────────────
        progress_bar.progress(0.42, "Validando coherencia de grupos…")
        comp_raw = defaultdict(list)
        for i in range(n): comp_raw[dsu.find(i)].append(i)

        # Re-validar grupos semánticos (no los de hash ni TF-IDF exacto)
        comp_validado: Dict[int, List[int]] = {}
        next_id = 0
        for lid, idxs in comp_raw.items():
            if len(idxs) == 1:
                comp_validado[next_id] = idxs; next_id += 1
                continue
            # Sólo validar grupos que no sean idénticos por hash
            sub_grupos = self._validar_miembros_grupo(idxs, textos_analisis, titulos, umbral_confianza=0.72)
            for sg in sub_grupos.values():
                comp_validado[next_id] = sg; next_id += 1

        # ── Paso E: Generación de subtema por grupo ──────────────────────────
        mapa_subtemas: Dict[int, str] = {}
        total_grupos = len(comp_validado)
        progress_bar.progress(0.50, f"Generando etiquetas para {total_grupos} grupos…")

        for k, (lid, idxs) in enumerate(comp_validado.items()):
            if k % 15 == 0:
                progress_bar.progress(0.50 + 0.28 * k / max(total_grupos, 1), f"Etiquetando grupo {k+1}/{total_grupos}…")
            subtema = self._generar_subtema(
                [textos_analisis[i] for i in idxs],
                [titulos[i] for i in idxs]
            )
            for i in idxs: mapa_subtemas[i] = subtema

        subtemas_brutos = [mapa_subtemas.get(i, "Actividad Corporativa") for i in range(n)]

        # ── Paso F: Fusión post-proceso ──────────────────────────────────────
        progress_bar.progress(0.82, "Fusionando subtemas similares…")
        subtemas_fusionados = self._fusionar_grupos_por_contenido(subtemas_brutos, textos_analisis)

        n_before = len(set(subtemas_brutos))
        n_after = len(set(subtemas_fusionados))
        st.info(f"Subtemas: {n_before} → {n_after} tras fusión por similitud de contenido")

        progress_bar.progress(1.0, "Subtemas listos")
        return subtemas_fusionados


# ======================================
# Consolidación de Subtemas → Temas
# ======================================
def consolidar_subtemas_en_temas(subtemas: List[str], textos: List[str], p_bar) -> List[str]:
    p_bar.progress(0.1, text="Analizando estructura de temas…")

    df_temas = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    unique_subtemas = df_temas['subtema'].unique()

    embs_labels = get_embeddings_batch(list(unique_subtemas))
    valid_idxs = [i for i, e in enumerate(embs_labels) if e is not None]

    if not valid_idxs: return subtemas

    valid_subtemas = [unique_subtemas[i] for i in valid_idxs]
    matrix_labels = np.array([embs_labels[i] for i in valid_idxs])

    todos_embs_textos = get_embeddings_batch(textos)
    matrix_content = []

    for subt in valid_subtemas:
        idxs = df_temas.index[df_temas['subtema'] == subt].tolist()[:30]
        vecs = [todos_embs_textos[i] for i in idxs if todos_embs_textos[i] is not None]
        if vecs:
            matrix_content.append(np.mean(vecs, axis=0))
        else:
            idx_orig = list(unique_subtemas).index(subt)
            matrix_content.append(embs_labels[idx_orig])

    matrix_content = np.array(matrix_content)
    sim_labels = cosine_similarity(matrix_labels)
    sim_content = cosine_similarity(matrix_content)
    sim_final = (0.4 * sim_labels) + (0.6 * sim_content)

    n_clusters_target = min(NUM_TEMAS_PRINCIPALES, len(valid_subtemas))
    if n_clusters_target < 2: return subtemas

    clustering = AgglomerativeClustering(
        n_clusters=n_clusters_target,
        metric='precomputed',
        linkage='average'
    ).fit(1 - np.clip(sim_final, 0, 1))

    mapa_tema_final: Dict[str, str] = {}
    clusters_contenidos: Dict[int, List[str]] = defaultdict(list)

    for i, label in enumerate(clustering.labels_):
        clusters_contenidos[label].append(valid_subtemas[i])

    for cid, lista_subtemas in clusters_contenidos.items():
        subtemas_str = ", ".join(lista_subtemas[:10])
        prompt = f"""Genera una CATEGORÍA periodística (2-3 palabras) que agrupe estos subtemas:
{subtemas_str}

REGLAS:
- Usa sustantivos concretos
- No verbos como inicio
- No términos vagos ("Gestión", "Temas", "Varios")
- Ej: "Resultados Financieros", "Sostenibilidad", "Expansión Digital", "Talento Humano"

Solo responde con la categoría, sin JSON ni comillas."""
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=15,
                temperature=0.1
            )
            if isinstance(resp, dict):
                usage = resp.get('usage', {})
            else:
                usage = getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += pt
                st.session_state['tokens_output'] += ct
            nombre_tema = limpiar_tema(resp.choices[0].message.content.strip().replace('"', '').replace('.', ''))
        except Exception:
            nombre_tema = lista_subtemas[0]

        for subt in lista_subtemas:
            mapa_tema_final[subt] = nombre_tema

    temas_finales = [mapa_tema_final.get(subt, subt) for subt in subtemas]
    st.info(f"Temas consolidados en {len(set(temas_finales))} categorías")
    p_bar.progress(1.0, "Temas finalizados")
    return temas_finales


def analizar_temas_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[str]]:
    try:
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error al procesar el `pipeline_tema.pkl`: {e}"); return None


# ======================================
# Lógica de Duplicados y Generación de Excel
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url, seen_broadcast = {}, {}
    online_title_buckets = defaultdict(list)

    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"): continue
        tipo_medio = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio"))))
        mencion_norm = norm_key(row.get(key_map.get("menciones")))
        medio_norm = norm_key(row.get(key_map.get("medio")))

        if tipo_medio == "Internet":
            link_info = row.get(key_map.get("link_nota"), {})
            url = link_info.get("url") if isinstance(link_info, dict) else None
            if url and mencion_norm:
                key = (url, mencion_norm)
                if key in seen_online_url:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_online_url[key]].get(key_map.get("idnoticia"), "")
                    continue
                else: seen_online_url[key] = i
            if medio_norm and mencion_norm: online_title_buckets[(medio_norm, mencion_norm)].append(i)

        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
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
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        if key_map.get("tipodemedio") in base:
            base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in m_list or [None]:
            new = deepcopy(base)
            if m: new[key_map["menciones"]] = m
            split_rows.append(new)

    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]: row.update({key_map["tonoiai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["subtema"]: "Duplicada"})
    return processed_rows, key_map


def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
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


def generate_output_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"
    final_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Region","Seccion - Programa","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Audiencia","Tier","Tono","Tono IA","Tema","Subtema","Link Nota","Resumen - Aclaracion","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    out_sheet.append(final_order)
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    if "Hyperlink_Custom" not in out_wb.style_names: out_wb.add_named_style(link_style)

    for row_data in all_processed_rows:
        titulo_key = key_map.get("titulo")
        if titulo_key and titulo_key in row_data: row_data[titulo_key] = clean_title_for_output(row_data.get(titulo_key))
        resumen_key = key_map.get("resumen")
        if resumen_key and resumen_key in row_data: row_data[resumen_key] = corregir_texto(row_data.get(resumen_key))

        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            nk_header = norm_key(header)
            data_key = key_map.get(nk_header, nk_header)
            val = row_data.get(data_key)
            cell_value = None
            if header in numeric_columns:
                try: cell_value = float(val) if val is not None and str(val).strip() != "" else None
                except (ValueError, TypeError): cell_value = str(val) if val is not None else None
            elif isinstance(val, dict) and "url" in val:
                cell_value = val.get("value", "Link")
                if val.get("url"): links_to_add[col_idx] = val["url"]
            elif val is not None: cell_value = str(val)
            row_to_append.append(cell_value)
        out_sheet.append(row_to_append)
        for col_idx, url in links_to_add.items():
            cell = out_sheet.cell(row=out_sheet.max_row, column=col_idx)
            cell.hyperlink = url
            cell.style = "Hyperlink_Custom"

    output = io.BytesIO()
    out_wb.save(output)
    return output.getvalue()


# ======================================
# Proceso principal asíncrono
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases, tono_pkl_file, tema_pkl_file, analysis_mode):
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0
    st.session_state['_emb_cache'] = {}

    start_time = time.time()
    if "API" in analysis_mode:
        try: openai.api_key = st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
        except Exception: st.error("Error: OPENAI_API_KEY no encontrado."); st.stop()

    with st.status("**Paso 1/5** · Limpieza y duplicados", expanded=True) as s:
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="✓ **Paso 1/5** · Limpieza completada", state="complete")

    with st.status("**Paso 2/5** · Mapeos y normalización", expanded=True) as s:
        df_region = pd.read_excel(region_file)
        region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
        df_internet = pd.read_excel(internet_file)
        internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
        for row in all_processed_rows:
            original_medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()
            row[key_map.get("region")] = region_map.get(original_medio_key, "N/A")
            if original_medio_key in internet_map:
                row[key_map.get("medio")] = internet_map[original_medio_key]
                row[key_map.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, key_map)
        s.update(label="✓ **Paso 2/5** · Mapeos aplicados", state="complete")

    gc.collect()
    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]

    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        df_temp["resumen_api"] = df_temp[key_map["titulo"]].fillna("").astype(str) + ". " + df_temp[key_map["resumen"]].fillna("").astype(str)

        with st.status("**Paso 3/5** · Análisis de tono", expanded=True) as s:
            p_bar = st.progress(0)
            if ("PKL" in analysis_mode) and tono_pkl_file:
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
            elif "API" in analysis_mode:
                clasif_tono = ClasificadorTonoUltraV3(brand_name, brand_aliases)
                resultados_tono = await clasif_tono.procesar_lote_async(df_temp["resumen_api"], p_bar, df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
            else:
                resultados_tono = [{"tono": "N/A"}] * len(rows_to_analyze)
            df_temp[key_map["tonoiai"]] = [res["tono"] for res in resultados_tono]
            s.update(label="✓ **Paso 3/5** · Tono analizado", state="complete")

        with st.status("**Paso 4/5** · Tema y subtema (alta precisión)", expanded=True) as s:
            p_bar = st.progress(0)
            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A"] * len(rows_to_analyze)
                temas_principales = ["N/A"] * len(rows_to_analyze)
            else:
                # Subtemas con ClasificadorSubtemaV4
                clasif_subtemas = ClasificadorSubtemaV4(brand_name, brand_aliases)
                subtemas = clasif_subtemas.procesar_lote(
                    df_temp["resumen_api"], p_bar,
                    df_temp[key_map["resumen"]], df_temp[key_map["titulo"]]
                )
                temas_principales = consolidar_subtemas_en_temas(subtemas, df_temp["resumen_api"].tolist(), p_bar)

            df_temp[key_map["subtema"]] = subtemas

            if ("PKL" in analysis_mode) and tema_pkl_file:
                temas_pkl = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                if temas_pkl: df_temp[key_map["tema"]] = temas_pkl
            else:
                df_temp[key_map["tema"]] = temas_principales

            s.update(label="✓ **Paso 4/5** · Clasificación completada", state="complete")

        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"): row.update(results_map.get(row["original_index"], {}))

    gc.collect()

    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    total_cost = cost_input + cost_output + cost_embedding

    with st.status("**Paso 5/5** · Generando informe", expanded=True) as s:
        duration_str = f"{time.time() - start_time:.0f}s"
        st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_IA_{brand_name.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name": brand_name,
            "brand_aliases": brand_aliases,
            "total_rows": len(all_processed_rows),
            "unique_rows": len(rows_to_analyze),
            "duplicates": len(all_processed_rows) - len(rows_to_analyze),
            "process_duration": duration_str,
            "process_cost": f"${total_cost:.4f}"
        })
        s.update(label="✓ **Paso 5/5** · Proceso completado", state="complete")


# ======================================
# Análisis Rápido
# ======================================
async def run_quick_analysis_async(df: pd.DataFrame, title_col: str, summary_col: str, brand_name: str, aliases: List[str]):
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0
    st.session_state['_emb_cache'] = {}

    df['texto_analisis'] = df[title_col].fillna('').astype(str) + ". " + df[summary_col].fillna('').astype(str)

    with st.status("**Paso 1/2** · Análisis de tono…", expanded=True) as s:
        p_bar = st.progress(0, "Iniciando…")
        clasif_tono = ClasificadorTonoUltraV3(brand_name, aliases)
        resultados_tono = await clasif_tono.procesar_lote_async(df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Tono IA'] = [res["tono"] for res in resultados_tono]
        s.update(label="✓ **Paso 1/2** · Tono analizado", state="complete")

    with st.status("**Paso 2/2** · Tema y subtema…", expanded=True) as s:
        p_bar = st.progress(0, "Generando subtemas…")
        clasif_subtemas = ClasificadorSubtemaV4(brand_name, aliases)
        subtemas = clasif_subtemas.procesar_lote(df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Subtema'] = subtemas
        p_bar.progress(0.5, "Consolidando temas…")
        temas_principales = consolidar_subtemas_en_temas(subtemas, df["texto_analisis"].tolist(), p_bar)
        df['Tema'] = temas_principales
        s.update(label="✓ **Paso 2/2** · Clasificación finalizada", state="complete")

    df.drop(columns=['texto_analisis'], inplace=True)

    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    st.session_state['quick_cost'] = f"${cost_input + cost_output + cost_embedding:.4f}"

    return df


def generate_quick_analysis_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Analisis')
    return output.getvalue()


# ======================================
# Tab: Análisis Rápido
# ======================================
def render_quick_analysis_tab():
    st.markdown('<span class="section-label">Análisis Rápido · OpenAI API</span>', unsafe_allow_html=True)

    if 'quick_analysis_result' in st.session_state:
        st.markdown("""
        <div class="success-banner">
            <span style="font-size:1.2rem">✓</span>
            <div><strong>Análisis completado</strong><br><span style="font-size:0.82rem;color:#4A9B6F">Tono, Subtema y Tema asignados con alta precisión</span></div>
        </div>
        """, unsafe_allow_html=True)

        cost = st.session_state.get('quick_cost', "$0.00")
        col_m1, col_m2 = st.columns(2)
        col_m1.metric("Filas procesadas", len(st.session_state.quick_analysis_result))
        col_m2.metric("Costo estimado", cost)

        st.dataframe(st.session_state.quick_analysis_result.head(10), use_container_width=True)

        excel_data = generate_quick_analysis_excel(st.session_state.quick_analysis_result)
        col_d, col_r = st.columns([2, 1])
        with col_d:
            st.download_button(
                label="Descargar resultados →",
                data=excel_data,
                file_name="Analisis_Rapido_IA.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, type="primary"
            )
        with col_r:
            if st.button("Nuevo análisis", use_container_width=True):
                for key in ['quick_analysis_result', 'quick_analysis_df', 'quick_file_name', 'quick_cost']:
                    if key in st.session_state: del st.session_state[key]
                st.rerun()
        return

    if 'quick_analysis_df' not in st.session_state:
        st.markdown('<span class="section-label">Archivo de entrada</span>', unsafe_allow_html=True)
        quick_file = st.file_uploader(
            "Sube tu archivo Excel con títulos y resúmenes",
            type=["xlsx"], label_visibility="collapsed", key="quick_uploader"
        )
        if quick_file:
            with st.spinner("Leyendo archivo…"):
                try:
                    st.session_state.quick_analysis_df = pd.read_excel(quick_file)
                    st.session_state.quick_file_name = quick_file.name
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}"); st.stop()
    else:
        st.markdown(f"""
        <div class="card card-accent" style="margin-bottom:1.5rem">
            <strong>{st.session_state.quick_file_name}</strong>
            <span class="tag-chip" style="margin-left:0.75rem">{len(st.session_state.quick_analysis_df):,} filas</span>
        </div>
        """, unsafe_allow_html=True)

        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df
            columns = df.columns.tolist()
            st.markdown('<span class="section-label">Columnas</span>', unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            title_col = col1.selectbox("Columna Título", options=columns, index=0)
            summary_index = 1 if len(columns) > 1 else 0
            summary_col = col2.selectbox("Columna Resumen", options=columns, index=summary_index)

            st.markdown('<span class="section-label">Marca</span>', unsafe_allow_html=True)
            brand_name = st.text_input("Nombre de la marca", placeholder="Ej: Siemens")
            brand_aliases_text = st.text_area("Alias (separados por ;)", placeholder="Ej: Siemens Healthineers; SH", height=75)

            submitted = st.form_submit_button("Analizar →", use_container_width=True, type="primary")
            if submitted:
                if not brand_name:
                    st.error("Escribe el nombre de la marca.")
                else:
                    try:
                        openai.api_key = st.secrets["OPENAI_API_KEY"]
                        openai.aiosession.set(None)
                    except Exception:
                        st.error("OPENAI_API_KEY no encontrada."); st.stop()
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    with st.spinner("Analizando…"):
                        st.session_state.quick_analysis_result = asyncio.run(
                            run_quick_analysis_async(df.copy(), title_col, summary_col, brand_name, aliases)
                        )
                    st.rerun()

        if st.button("← Cargar otro archivo"):
            for key in ['quick_analysis_df', 'quick_file_name', 'quick_analysis_result', 'quick_cost']:
                if key in st.session_state: del st.session_state[key]
            st.rerun()


# ======================================
# Tab: Análisis Completo
# ======================================
def render_complete_analysis_tab():
    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown('<span class="section-label">Archivos de entrada</span>', unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            dossier_file = col1.file_uploader("Dossier (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("Región (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("Internet (.xlsx)", type=["xlsx"])

            st.markdown('<span class="section-label">Configuración de marca</span>', unsafe_allow_html=True)
            col_b1, col_b2 = st.columns(2)
            brand_name = col_b1.text_input("Marca principal", placeholder="Ej: Bancolombia")
            brand_aliases_text = col_b2.text_area("Alias (sep. por ;)", placeholder="Ej: Ban; Juan Carlos Mora", height=75)

            st.markdown('<span class="section-label">Modo de análisis</span>', unsafe_allow_html=True)
            analysis_mode = st.radio(
                "Modo:",
                options=["Híbrido (PKL + API)", "Solo Modelos PKL", "API de OpenAI"],
                index=0, horizontal=True
            )

            tono_pkl_file, tema_pkl_file = None, None
            if "PKL" in analysis_mode:
                st.markdown('<span class="section-label">Modelos entrenados</span>', unsafe_allow_html=True)
                c1, c2 = st.columns(2)
                tono_pkl_file = c1.file_uploader("pipeline_sentimiento.pkl", type=["pkl"])
                tema_pkl_file = c2.file_uploader("pipeline_tema.pkl", type=["pkl"])

            st.markdown("")
            submitted = st.form_submit_button("Iniciar análisis →", use_container_width=True, type="primary")

            if submitted:
                if not all([dossier_file, region_file, internet_file, brand_name.strip()]):
                    st.error("Completa todos los campos requeridos.")
                else:
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    asyncio.run(run_full_process_async(
                        dossier_file, region_file, internet_file,
                        brand_name, aliases, tono_pkl_file, tema_pkl_file, analysis_mode
                    ))
                    st.rerun()
    else:
        # ── Resultados ──────────────────────────────────────────────────────
        st.markdown("""
        <div class="success-banner">
            <span style="font-size:1.4rem">✓</span>
            <div><strong>Análisis completado exitosamente</strong><br><span style="font-size:0.82rem;color:#4A9B6F">El informe está listo para descargar</span></div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="metrics-row">
            <div class="metric-item">
                <div class="metric-item-val">{st.session_state.total_rows:,}</div>
                <div class="metric-item-lbl">Total</div>
            </div>
            <div class="metric-item">
                <div class="metric-item-val green">{st.session_state.unique_rows:,}</div>
                <div class="metric-item-lbl">Únicas</div>
            </div>
            <div class="metric-item">
                <div class="metric-item-val orange">{st.session_state.duplicates:,}</div>
                <div class="metric-item-lbl">Duplicadas</div>
            </div>
            <div class="metric-item">
                <div class="metric-item-val blue">{st.session_state.process_duration}</div>
                <div class="metric-item-lbl">Duración</div>
            </div>
            <div class="metric-item">
                <div class="metric-item-val red">{st.session_state.get("process_cost", "$0.00")}</div>
                <div class="metric-item-lbl">Costo USD</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        col_d, col_r = st.columns([2, 1])
        with col_d:
            st.download_button(
                label="Descargar informe →",
                data=st.session_state.output_data,
                file_name=st.session_state.output_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, type="primary"
            )
        with col_r:
            if st.button("Nuevo análisis", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()


# ======================================
# Main
# ======================================
def main():
    load_custom_css()
    if not check_password(): return

    st.markdown("""
    <div class="app-header">
        <div class="app-header-eyebrow">Análisis de Medios · IA</div>
        <div class="app-header-title">Sistema de Clasificación de Noticias</div>
        <div class="app-header-sub">Tono · Subtema · Tema — Clustering de alta precisión con embeddings semánticos</div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])
    with tab1:
        render_complete_analysis_tab()
    with tab2:
        render_quick_analysis_tab()

    st.markdown("""
    <div class="app-footer">
        v8.0.0 · Sistema de Análisis de Noticias con IA · Realizado por Johnathan Cortés ©
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
