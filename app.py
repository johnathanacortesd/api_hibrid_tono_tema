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
import json
import asyncio
import hashlib
from typing import List, Dict, Tuple, Optional, Any
import joblib
import gc
import nest_asyncio
nest_asyncio.apply()

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Analisis de Noticias con IA",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Modelos
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"
OPENAI_MODEL_CLASIFICACION_PRO = "gpt-4.1-mini-2025-04-14"

# Configuracion de rendimiento y umbrales
CONCURRENT_REQUESTS = 50
SIMILARITY_THRESHOLD_TONO = 0.90
SIMILARITY_THRESHOLD_TITULOS = 0.93
WINDOW = 200

# Configuracion de agrupacion
NUM_TEMAS_PRINCIPALES = 20
UMBRAL_FUSION_CONTENIDO = 0.82
UMBRAL_CLUSTER_SUBTEMA = 0.20

# Precios (Por 1 millon de tokens)
PRICE_INPUT_1M = 0.10
PRICE_OUTPUT_1M = 0.40
PRICE_EMBEDDING_1M = 0.02

# Inicializar contadores
for _k in ['tokens_input', 'tokens_output', 'tokens_embedding']:
    if _k not in st.session_state:
        st.session_state[_k] = 0

# Listas Geograficas
CIUDADES_COLOMBIA = {
    "bogota","bogota","medellin","medellin","cali","barranquilla","cartagena",
    "cucuta","cucuta","bucaramanga","pereira","manizales","armenia","ibague","ibague",
    "villavicencio","monteria","monteria","neiva","pasto","valledupar","popayan",
    "popayan","tunja","florencia","sincelejo","riohacha","yopal","santa marta",
    "santamarta","quibdo","quibdo","leticia","mocoa","mitu","mitu",
    "puerto carreno","inirida","inirida","san jose del guaviare","antioquia",
    "atlantico","atlantico","bolivar","bolivar","boyaca","boyaca","caldas",
    "caqueta","caqueta","casanare","cauca","cesar","choco","choco","cordoba",
    "cordoba","cundinamarca","guainia","guainia","guaviare","huila","la guajira",
    "magdalena","meta","narino","narino","norte de santander","putumayo",
    "quindio","quindio","risaralda","san andres","san andres","santander","sucre",
    "tolima","valle del cauca","vaupes","vaupes","vichada"
}
GENTILICIOS_COLOMBIA = {
    "bogotano","bogotanos","bogotana","bogotanas","capitalino","capitalinos",
    "capitalina","capitalinas","antioqueno","antioquenos","antioquena","antioquenas",
    "paisa","paisas","medellense","medellenses","caleno","calenos","calena","calenas",
    "valluno","vallunos","valluna","vallunas","vallecaucano","vallecaucanos",
    "barranquillero","barranquilleros","cartagenero","cartageneros","costeno",
    "costenos","costena","costenas","cucuteno","cucutenos","bumangues","santandereano",
    "santandereanos","boyacense","boyacenses","tolimense","tolimenses","huilense",
    "huilenses","narinense","narinenses","pastuso","pastusas","cordobes","cordobeses",
    "caucano","caucanos","chocoano","chocoanos","casanareno","casanarenos",
    "caqueteno","caquetenos","guajiro","guajiros","llanero","llaneros",
    "amazonense","amazonenses","colombiano","colombianos","colombiana","colombianas"
}

# ======================================
# Lexicos y patrones
# ======================================
STOPWORDS_ES = set("""
a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por
segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le
les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos
esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos
cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha
han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan
estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada
""".split())

POS_VARIANTS = [
    r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?",
    r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto|oferta)",
    r"apertur(a|ar|ara|o|an)", r"estren(a|o|ara|an|ando)",
    r"habilit(a|o|ara|an|ando)", r"mejor(a|o|an|ando|amiento)",
    r"optimiza|amplia|expande",
    r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oo]n(es)?|asociaci[oo]n(es)?|partnership(s)?|fusi[oo]n(es)?|integraci[oo]n(es)?",
    r"crecimi?ento|aument(a|o|an|ando)", r"gananci(a|as)|utilidad(es)?|benefici(o|os)",
    r"expansion|crece|crecer", r"inversion|invierte|invertir",
    r"innova(cion|dor|ndo)|moderniza", r"exito(so|sa)?|logr(o|os|a|an|ando)",
    r"reconoci(miento|do|da)|premi(o|os|ada)|galardon",
    r"lidera(zgo)?|lider", r"consolida|fortalece",
    r"oportunidad(es)?|potencial", r"solucion(es)?|resuelve",
    r"eficien(te|cia)", r"calidad|excelencia", r"satisfaccion|complace",
    r"confianza|credibilidad", r"sostenible|responsable|sustentable",
    r"compromiso|apoya|apoyar",
    r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)",
    r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)",
    r"destaca(r|do|da|ndo)?", r"supera(r|ndo|cion)?",
    r"record|hito|milestone", r"avanza(r|do|da|ndo)?",
    r"bienestar", r"atencion\s+(al\s+)?cliente",
    r"transpar(encia|ente)", r"inclusi[oo]n|diversidad",
    r"empleo(s)?|trabajo(s)?", r"ratific(a|o|ando|ar)",
]

NEG_VARIANTS = [
    r"demanda(d[ao])?|denuncia(d[ao])?",
    r"sanciona(d[ao])?|multa(d[ao])?",
    r"investiga(d[ao])?|imputad[ao]",
    r"critica(d[ao])?|cuestion(a|o|ado)",
    r"cae|baja|pierde|perdida|caida|desplom",
    r"crisis|quiebra|default|insolvencia|bancarrota",
    r"fraude|escandalo|irregularidad|corrupci[oo]n",
    r"fall(a|o|os)|falla(ron)?|interrumpe|suspende",
    r"cierra|clausura|renuncia|huelga|paro",
    r"filtracion|ataque|phishing|hackeo|ciberataque|vulnerabilidad",
    r"incumple|incumplimiento|boicot|queja(s)?|reclamo(s)?",
    r"deteriora|degrada|empeora",
    r"estafa|engano",
    r"desacuerdo|conflicto|disputa",
    r"negativ[oa]|rechaz(a|o|ar|ado)",
    r"preocupa(ci[oo]n|nte|do)?",
    r"alarmante|alerta\s+roja",
    r"riesgo(s)?|amenaza(s)?",
    r"protest(a|o|an|ando)",
    r"escasez|desabastecimiento",
    r"accidente|siniestro|incidente",
    r"perdida(s)?|deficit|quebranto",
    r"vict(ima|imas|imario)",
]

CRISIS_KEYWORDS = re.compile(
    r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oo]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b",
    re.IGNORECASE
)
RESPONSE_VERBS = re.compile(
    r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b",
    re.IGNORECASE
)
NEGATION_PATTERN = re.compile(
    r"\b(no|sin|nunca|jamas|niega|rechaza|desmiente|descarta|evita|previene)\b",
    re.IGNORECASE
)
POS_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]


# ======================================
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,400&family=DM+Mono:wght@400;500&display=swap');

/* ── ROOT TOKENS ── */
:root {
    --ink:        #111111;
    --ink-mid:    #444444;
    --ink-soft:   #777777;
    --ink-faint:  #aaaaaa;
    --paper:      #faf9f7;
    --paper2:     #f3f1ed;
    --paper3:     #ede9e2;
    --rule:       #d8d3cb;
    --rule-dark:  #b8b2a8;
    --gold:       #b5842a;
    --green:      #1a7a4a;
    --red:        #c0392b;
    --blue:       #1a4a7a;
    --radius:     2px;
    --font-serif: 'Libre Baskerville', Georgia, 'Times New Roman', serif;
    --font-sans:  'DM Sans', -apple-system, system-ui, sans-serif;
    --font-mono:  'DM Mono', 'Courier New', monospace;
}

/* ── GLOBAL RESET ── */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stHeader"],
[data-testid="block-container"],
section[data-testid="stMain"] > div,
.main .block-container {
    background-color: var(--paper) !important;
    color: var(--ink) !important;
    font-family: var(--font-sans) !important;
}
.stApp { background: var(--paper) !important; }

/* ── HEADER — masthead style ── */
.main-header {
    background: var(--paper) !important;
    border-top: 4px solid var(--ink);
    border-bottom: 1px solid var(--rule-dark);
    padding: 2.2rem 1rem 1.4rem;
    text-align: center;
    margin-bottom: 0;
}
.main-header h1 {
    font-family: var(--font-serif) !important;
    font-size: 2.8rem !important;
    font-weight: 700 !important;
    color: var(--ink) !important;
    letter-spacing: -0.03em;
    line-height: 1.1;
    margin: 0 0 0.3rem 0 !important;
}
.main-header .kicker {
    font-family: var(--font-mono) !important;
    font-size: 0.62rem;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    color: var(--gold);
    display: block;
    margin-bottom: 0.5rem;
}

.subtitle {
    text-align: center;
    font-family: var(--font-mono) !important;
    font-size: 0.62rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--ink-soft) !important;
    padding: 0.7rem 0 1.8rem;
    border-bottom: 1px solid var(--rule);
    margin-bottom: 2rem;
}

/* ── METRICS ── */
.metric-card {
    background: var(--paper) !important;
    border-top: 3px solid var(--ink);
    border-bottom: 1px solid var(--rule);
    padding: 1.2rem 0.8rem 1rem;
    text-align: left;
}
.metric-value {
    font-family: var(--font-serif) !important;
    font-size: 2rem;
    font-weight: 700;
    color: var(--ink) !important;
    display: block;
    line-height: 1;
    margin-bottom: 0.35rem;
}
.metric-value.green  { color: var(--green) !important; }
.metric-value.yellow { color: var(--gold) !important; }
.metric-value.red    { color: var(--red) !important; }
.metric-value.purple { color: var(--blue) !important; }
.metric-label {
    font-family: var(--font-mono) !important;
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--ink-soft) !important;
}

/* ── SUCCESS / DOWNLOAD CARD ── */
.success-card {
    background: var(--paper2) !important;
    border-left: 3px solid var(--green);
    border-top: 1px solid var(--rule);
    border-bottom: 1px solid var(--rule);
    border-right: 1px solid var(--rule);
    padding: 1.5rem;
    margin: 1.5rem 0;
    border-radius: var(--radius);
}

/* ── TYPOGRAPHY ── */
h1, h2, h3 {
    font-family: var(--font-serif) !important;
    color: var(--ink) !important;
    letter-spacing: -0.02em;
}
h2 { font-size: 1.5rem !important; border-bottom: 1px solid var(--rule); padding-bottom: 0.5rem; }
h3 { font-size: 1.1rem !important; }
p, li, span, div { color: var(--ink) !important; }
label { color: var(--ink-mid) !important; }
strong { color: var(--ink) !important; }

/* ── INPUTS ── */
.stTextInput > div > div,
[data-baseweb="input"],
[data-baseweb="textarea"] {
    background-color: #ffffff !important;
    border: 1px solid var(--rule-dark) !important;
    border-radius: var(--radius) !important;
    color: var(--ink) !important;
}
.stTextInput input,
.stTextArea textarea {
    background: #ffffff !important;
    color: var(--ink) !important;
    font-family: var(--font-sans) !important;
    font-size: 0.9rem !important;
}
.stTextInput input::placeholder,
.stTextArea textarea::placeholder {
    color: var(--ink-faint) !important;
    font-style: italic;
}
.stTextInput input:focus,
.stTextArea textarea:focus {
    border-color: var(--gold) !important;
    box-shadow: 0 0 0 1px var(--gold) !important;
    outline: none !important;
}

/* Labels above inputs */
.stTextInput label, .stTextArea label,
.stSelectbox label, .stFileUploader label,
[data-testid="stWidgetLabel"],
[data-testid="stWidgetLabel"] p {
    font-family: var(--font-mono) !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--ink-mid) !important;
    margin-bottom: 0.3rem !important;
}

/* Selectbox */
[data-baseweb="select"] div {
    background: #ffffff !important;
    border-color: var(--rule-dark) !important;
    color: var(--ink) !important;
}

/* ── RADIO ── */
[data-testid="stRadio"] label,
[data-testid="stRadio"] label p { color: var(--ink-mid) !important; font-size: 0.85rem !important; }
[aria-checked="true"] + div label,
[aria-checked="true"] + div label p { color: var(--ink) !important; font-weight: 600 !important; }

/* ── FILE UPLOADER ── */
[data-testid="stFileUploader"] > div {
    background: var(--paper2) !important;
    border: 1px dashed var(--rule-dark) !important;
    border-radius: var(--radius) !important;
}
[data-testid="stFileUploader"] span,
[data-testid="stFileUploader"] p { color: var(--ink-mid) !important; font-size: 0.82rem !important; }
[data-testid="stFileUploader"] button {
    background: var(--paper) !important;
    color: var(--ink) !important;
    border: 1px solid var(--rule-dark) !important;
    font-size: 0.78rem !important;
}

/* ── FORM CONTAINER ── */
[data-testid="stForm"] {
    background: var(--paper2) !important;
    border: 1px solid var(--rule) !important;
    border-top: 2px solid var(--ink) !important;
    border-radius: var(--radius) !important;
    padding: 1.6rem !important;
}

/* ── BUTTONS ── */
.stButton > button {
    background: var(--paper) !important;
    color: var(--ink) !important;
    border: 1px solid var(--rule-dark) !important;
    border-radius: var(--radius) !important;
    font-family: var(--font-sans) !important;
    font-weight: 500 !important;
    font-size: 0.83rem !important;
    letter-spacing: 0.02em;
    padding: 0.5rem 1.2rem !important;
    transition: all 0.12s ease !important;
}
.stButton > button:hover {
    background: var(--paper3) !important;
    border-color: var(--ink) !important;
}
.stButton > button[kind="primary"],
button[data-testid="baseButton-primary"] {
    background: var(--ink) !important;
    color: var(--paper) !important;
    border-color: var(--ink) !important;
    font-weight: 600 !important;
    letter-spacing: 0.04em;
}
.stButton > button[kind="primary"]:hover,
button[data-testid="baseButton-primary"]:hover {
    background: #2a2a2a !important;
    border-color: #2a2a2a !important;
}

/* ── DOWNLOAD BUTTON ── */
[data-testid="stDownloadButton"] > button {
    background: var(--ink) !important;
    color: var(--paper) !important;
    border-color: var(--ink) !important;
    font-weight: 600 !important;
    width: 100%;
}
[data-testid="stDownloadButton"] > button:hover {
    background: #2a2a2a !important;
}

/* ── TABS ── */
[data-testid="stTabs"] { border-bottom: 2px solid var(--ink) !important; }
button[data-baseweb="tab"] {
    background: transparent !important;
    color: var(--ink-soft) !important;
    font-family: var(--font-mono) !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    border-bottom: 2px solid transparent !important;
    margin-bottom: -2px;
    padding: 0.65rem 1.2rem !important;
    transition: color 0.12s;
}
button[data-baseweb="tab"]:hover { color: var(--ink) !important; }
button[aria-selected="true"][data-baseweb="tab"] {
    color: var(--ink) !important;
    font-weight: 600 !important;
    border-bottom: 2px solid var(--gold) !important;
}
[data-testid="stTabsContent"] { background: transparent !important; padding-top: 1.5rem !important; }

/* ── ALERTS / STATUS ── */
[data-testid="stAlert"] {
    background: var(--paper2) !important;
    border-radius: var(--radius) !important;
    border-left: 3px solid var(--rule-dark) !important;
    color: var(--ink) !important;
}
[data-testid="stAlert"] p { color: var(--ink) !important; font-size: 0.88rem !important; }
[data-testid="stStatusContainer"] {
    background: var(--paper2) !important;
    border: 1px solid var(--rule) !important;
    border-top: 2px solid var(--gold) !important;
    color: var(--ink) !important;
}

/* ── PROGRESS ── */
[data-testid="stProgress"] > div { background: var(--paper3) !important; border-radius: 1px !important; }
[data-testid="stProgress"] > div > div { background: var(--ink) !important; border-radius: 1px !important; }

/* ── SPINNER ── */
[data-testid="stSpinner"] { color: var(--ink-mid) !important; }

/* ── DATAFRAME ── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--rule) !important;
    border-radius: var(--radius) !important;
}
.dvn-scroller { background: #ffffff !important; }

/* ── EXPANDER ── */
[data-testid="stExpander"] {
    background: var(--paper2) !important;
    border: 1px solid var(--rule) !important;
    border-radius: var(--radius) !important;
}
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary p { color: var(--ink) !important; font-family: var(--font-sans) !important; }

/* ── MARKDOWN ── */
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li { color: var(--ink-mid) !important; font-size: 0.9rem !important; line-height: 1.6; }
[data-testid="stMarkdownContainer"] strong { color: var(--ink) !important; }
[data-testid="stMarkdownContainer"] code {
    background: var(--paper3) !important;
    color: var(--ink) !important;
    border: 1px solid var(--rule) !important;
    font-family: var(--font-mono) !important;
    font-size: 0.78rem !important;
    padding: 1px 5px !important;
    border-radius: 2px !important;
}

/* Captions */
[data-testid="stCaptionContainer"] p,
.stCaption { color: var(--ink-soft) !important; font-size: 0.78rem !important; font-style: italic; }

/* ── DIVIDER ── */
hr { border: none !important; border-top: 1px solid var(--rule) !important; margin: 2rem 0 !important; }

/* ── SECTION LABELS (custom class) ── */
.section-rule {
    font-family: var(--font-mono) !important;
    font-size: 0.6rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--ink-soft) !important;
    display: flex;
    align-items: center;
    gap: 0.8rem;
    margin: 1.4rem 0 0.8rem;
}
.section-rule::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--rule);
}

/* ── SCROLLBAR ── */
* { scrollbar-width: thin; scrollbar-color: var(--rule-dark) var(--paper); }
*::-webkit-scrollbar { width: 4px; height: 4px; }
*::-webkit-scrollbar-track { background: var(--paper); }
*::-webkit-scrollbar-thumb { background: var(--rule-dark); border-radius: 1px; }
</style>
""", unsafe_allow_html=True)



# ======================================
# Autenticacion y Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True
    st.markdown(
        '<div class="main-header">'
        '<span class="kicker">Acceso Restringido</span>'
        '<h1>Portal de Análisis</h1>'
        '</div>',
        unsafe_allow_html=True
    )
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("🔑 Contraseña:", type="password")
            if st.form_submit_button("🚀 Ingresar", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True
                    st.success("✅ Acceso autorizado.")
                    st.balloons()
                    time.sleep(1.5)
                    st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
    return False


def call_with_retries(api_func, *args, **kwargs):
    max_retries, delay = 3, 1
    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)
            delay *= 2


async def acall_with_retries(api_func, *args, **kwargs):
    max_retries, delay = 3, 1
    for attempt in range(max_retries):
        try:
            return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            await asyncio.sleep(delay)
            delay *= 2


def norm_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))


def _count_chat_tokens(resp):
    usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
    if usage:
        pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
        ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
        st.session_state['tokens_input'] += (pt or 0)
        st.session_state['tokens_output'] += (ct or 0)


def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    if tema:
        tema = tema[0].upper() + tema[1:]
    # Only strip truly dangling words at the end (conjunctions/articles), not prepositions
    dangling_end = ["y", "o", "con", "para", "sobre", "un", "una", "el", "la"]
    palabras = tema.split()
    while palabras and palabras[-1].lower() in dangling_end:
        palabras.pop()
    tema = " ".join(palabras)
    if len(tema.split()) > 6:
        tema = " ".join(tema.split()[:6])
    return tema if tema else "Sin tema"


def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema:
        return "Sin tema"
    tema_lower = tema.lower()
    all_brand_names = ([marca.lower()] if marca else []) + [a.lower() for a in (aliases or []) if a]
    for bn in all_brand_names:
        tema_lower = re.sub(rf'\b{re.escape(bn)}\b', '', tema_lower, flags=re.IGNORECASE)
        tema_lower = re.sub(rf'\b{re.escape(unidecode(bn))}\b', '', tema_lower, flags=re.IGNORECASE)
    for ciudad in CIUDADES_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(ciudad)}\b', '', tema_lower, flags=re.IGNORECASE)
    for gent in GENTILICIOS_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(gent)}\b', '', tema_lower, flags=re.IGNORECASE)
    for frase in ["en colombia","de colombia","del pais","en el pais","nacional","colombiano","colombiana","colombianos","colombianas","territorio nacional"]:
        tema_lower = re.sub(rf'\b{re.escape(frase)}\b', '', tema_lower, flags=re.IGNORECASE)
    palabras = [p.strip() for p in tema_lower.split() if p.strip() and p.strip() not in STOPWORDS_ES]
    if not palabras:
        return "Sin tema"
    tema_limpio = " ".join(palabras)
    if tema_limpio:
        tema_limpio = tema_limpio[0].upper() + tema_limpio[1:]
    return limpiar_tema(tema_limpio)


def string_norm_label(s: str) -> str:
    if not s:
        return ""
    s = unidecode(s.lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join([t for t in s.split() if t not in STOPWORDS_ES])


def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match:
            return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}


def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str):
        return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
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
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")


# ======================================
# Embeddings
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos:
        return []
    resultados = [None] * len(textos)
    for i in range(0, len(textos), batch_size):
        batch = textos[i:i + batch_size]
        batch_truncado = [t[:2000] if t else "" for t in batch]
        try:
            resp = call_with_retries(openai.Embedding.create, input=batch_truncado, model=OPENAI_MODEL_EMBEDDING)
            usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if usage:
                total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                st.session_state['tokens_embedding'] += (total or 0)
            for j, emb_data in enumerate(resp["data"]):
                resultados[i + j] = emb_data["embedding"]
        except Exception:
            for j, texto in enumerate(batch):
                try:
                    resp = openai.Embedding.create(input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
                    usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
                    if usage:
                        total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                        st.session_state['tokens_embedding'] += (total or 0)
                    resultados[i + j] = resp["data"][0]["embedding"]
                except Exception:
                    resultados[i + j] = None
    return resultados


# ======================================
# Agrupacion Generica
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    if not textos:
        return {}
    embs = get_embeddings_batch(textos)
    valid_indices = [i for i, e in enumerate(embs) if e is not None]
    if len(valid_indices) < 2:
        return {}
    emb_matrix = np.array([embs[i] for i in valid_indices])
    clustering = AgglomerativeClustering(
        n_clusters=None, distance_threshold=1 - umbral_similitud, metric="cosine", linkage="average"
    ).fit(emb_matrix)
    grupos = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        grupos[label].append(valid_indices[i])
    return {gid: g for gid, g in enumerate(grupos.values())}


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
    subset_textos = [textos[i] for i in indices]
    embs = get_embeddings_batch(subset_textos)
    valid_indices, valid_embs = [], []
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
# CLASIFICADOR DE TONO V4
# Mejoras clave:
# - No requiere nombre de marca (opcional)
# - Tono relativo a la marca (no a la noticia global)
# - Propagacion de tono dentro de grupos similares
# - Negacion inteligente que invierte sentido de patrones
# ======================================
class ClasificadorTonoV4:

    def __init__(self, marca: str = "", aliases: List[str] = None):
        self.marca = marca.strip() if marca else ""
        self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex() if self.marca else None
        self._tono_cache: Dict[str, str] = {}

    def _build_brand_regex(self) -> re.Pattern:
        names = [self.marca] + [a for a in self.aliases if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        if not patterns:
            return None
        return re.compile(r"\b(" + "|".join(patterns) + r")\b", re.IGNORECASE)

    def _extract_brand_context(self, texto: str) -> List[str]:
        """
        Extrae fragmentos centrados en menciones de la marca.
        Si no hay marca definida, usa primeras oraciones del texto.
        """
        texto_lower = unidecode(texto.lower())
        if not self.brand_pattern:
            sentences = re.split(r'[.!?]\s+', texto)
            return [" ".join(sentences[:5])[:900]]

        matches = list(self.brand_pattern.finditer(texto_lower))
        if not matches:
            # Marca no mencionada -> tono neutro por defecto
            return []

        contextos = []
        for i, match in enumerate(matches):
            w = WINDOW + 50 if i == 0 else WINDOW
            start = max(0, match.start() - w)
            end = min(len(texto), match.end() + w)
            while end < len(texto) and texto[end] not in '.!?':
                end += 1
            contextos.append(texto[start:end + 1].strip())

        return list(dict.fromkeys(contextos))[:4]

    def _analizar_reglas(self, contextos: List[str]) -> Optional[str]:
        """Reglas con negacion inteligente."""
        if not contextos:
            return "Neutro"

        pos_score = neg_score = 0

        for contexto in contextos:
            t = unidecode(contexto.lower())
            neg_matches = list(NEGATION_PATTERN.finditer(t))

            pos_hits = neg_hits = 0
            for p in POS_PATTERNS:
                for m in p.finditer(t):
                    negado = any(nm.start() < m.start() < nm.start() + 55 for nm in neg_matches)
                    if negado:
                        neg_hits += 1
                    else:
                        pos_hits += 1
            for p in NEG_PATTERNS:
                for m in p.finditer(t):
                    negado = any(nm.start() < m.start() < nm.start() + 55 for nm in neg_matches)
                    if negado:
                        pos_hits += 1
                    else:
                        neg_hits += 1

            # Respuesta institucional a crisis = positivo contextual
            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(t):
                pos_score += 2
                continue

            pos_score += pos_hits
            neg_score += neg_hits

        total = pos_score + neg_score
        if total == 0:
            return None  # Ambiguo -> LLM

        ratio = pos_score / total
        if ratio >= 0.65 and pos_score >= 2:
            return "Positivo"
        if ratio <= 0.35 and neg_score >= 2:
            return "Negativo"
        if total >= 5:
            return "Neutro"
        return None  # Delegar a LLM

    async def _llm_clasificar(self, contextos: List[str]) -> str:
        """LLM especializado en tono RELATIVO A LA MARCA."""
        if not contextos:
            return "Neutro"

        marca_str = f"'{self.marca}'" if self.marca else "la entidad mencionada en el texto"
        aliases_str = ", ".join(self.aliases) if self.aliases else "N/A"
        contextos_texto = "\n---\n".join(contextos[:3])

        prompt = f"""Eres un analista senior de monitoreo de medios. Determina el TONO de cobertura hacia {marca_str} (alias: {aliases_str}).

REGLA FUNDAMENTAL: Analiza UNICAMENTE el sentimiento hacia {marca_str}, NO el tema general.

POSITIVO → logros, lanzamientos, reconocimientos, premios, crecimiento, respuesta efectiva, alianzas beneficiosas, innovacion.
NEGATIVO → criticas directas, sanciones, escandalo, perdidas atribuibles, demandas, fallas operativas, irregularidades.
NEUTRO → menciones informativas, datos sin valoracion, contexto sectorial, cobertura equilibrada, {marca_str} no es sujeto principal.

CASOS ESPECIALES:
- Crisis del sector + {marca_str} como solucion/lider de respuesta → POSITIVO
- {marca_str} no aparece como actor principal → NEUTRO
- Critica al sector pero NO a {marca_str} especificamente → NEUTRO

Fragmentos:
---
{contextos_texto}
---

Responde SOLO JSON: {{"tono":"Positivo|Negativo|Neutro"}}"""

        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            _count_chat_tokens(resp)
            data = json.loads(resp.choices[0].message.content.strip())
            tono = str(data.get("tono", "Neutro")).title()
            return tono if tono in ["Positivo", "Negativo", "Neutro"] else "Neutro"
        except Exception:
            return "Neutro"

    async def _clasificar_item_async(self, texto: str, semaphore: asyncio.Semaphore) -> str:
        cache_key = hashlib.md5(texto[:600].encode()).hexdigest()
        if cache_key in self._tono_cache:
            return self._tono_cache[cache_key]

        async with semaphore:
            contextos = self._extract_brand_context(texto)
            tono = self._analizar_reglas(contextos)
            if tono is None:
                tono = await self._llm_clasificar(contextos)
            self._tono_cache[cache_key] = tono
            return tono

    async def procesar_lote_async(
        self, textos_concat: pd.Series, progress_bar,
        resumen_puro: pd.Series, titulos_puros: pd.Series
    ) -> List[Dict[str, str]]:
        textos = textos_concat.tolist()
        n = len(textos)
        progress_bar.progress(0.05, text="🔄 Construyendo grupos semanticos de tono...")

        # Union-Find
        class DSU:
            def __init__(self, n):
                self.p = list(range(n))
                self.rank = [0] * n

            def find(self, i):
                while self.p[i] != i:
                    self.p[i] = self.p[self.p[i]]
                    i = self.p[i]
                return i

            def union(self, i, j):
                ri, rj = self.find(i), self.find(j)
                if ri == rj: return
                if self.rank[ri] < self.rank[rj]: ri, rj = rj, ri
                self.p[rj] = ri
                if self.rank[ri] == self.rank[rj]: self.rank[ri] += 1

        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO),
                  agrupar_por_titulo_similar(titulos_puros.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)

        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)

        progress_bar.progress(0.15, text=f"📦 {len(comp)} grupos → clasificando representantes...")

        representantes = {cid: seleccionar_representante(idxs, textos)[1] for cid, idxs in comp.items()}
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        resultados_grupos: Dict[int, str] = {}
        total_tareas = len(representantes)
        completados = 0

        # Procesar en orden para poder actualizar barra
        for cid, rep_texto in representantes.items():
            resultados_grupos[cid] = await self._clasificar_item_async(rep_texto, semaphore)
            completados += 1
            if completados % 3 == 0 or completados == total_tareas:
                progress_bar.progress(
                    0.15 + 0.80 * completados / total_tareas,
                    text=f"🎯 Tono: {completados}/{total_tareas} grupos procesados"
                )

        # Propagar tono del representante al grupo completo
        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            tono = resultados_grupos.get(cid, "Neutro")
            for i in idxs:
                resultados_finales[i] = {"tono": tono}

        progress_bar.progress(1.0, text="✅ Tono completado")
        return resultados_finales


def analizar_tono_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[Dict[str, str]]]:
    try:
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        TONO_MAP = {1: "Positivo", "1": "Positivo", 0: "Neutro", "0": "Neutro", -1: "Negativo", "-1": "Negativo"}
        return [{"tono": TONO_MAP.get(p, str(p).title())} for p in predicciones]
    except Exception as e:
        st.error(f"❌ Error al procesar `pipeline_sentimiento.pkl`: {e}")
        return None


# ======================================
# CLASIFICADOR DE SUBTEMAS V4
# Mejoras:
# - No requiere nombre de marca (opcional)
# - Etiquetas 2-5 palabras concretas y especificas
# - Clustering mas granular (menor umbral)
# - Fusion conservadora (sin sobre-generalizacion)
# ======================================
class ClasificadorSubtemaV4:

    def __init__(self, marca: str = "", aliases: List[str] = None):
        self.marca = marca.strip() if marca else ""
        self.aliases = aliases or []
        self._cache: Dict[str, str] = {}

    def _preagrupar_identicos(self, textos, titulos, resumenes):
        n = len(textos)
        grupos, usado, gid = {}, set(), 0

        def norm_r(texto):
            if not texto: return ""
            return ' '.join(re.sub(r'[^a-z0-9\s]', '', unidecode(str(texto).lower())).split()[:30])

        titulos_norm = [norm_r(t) for t in titulos]
        resumenes_norm = [norm_r(r) for r in resumenes]

        titulo_idx = defaultdict(list)
        for i, t in enumerate(titulos_norm):
            if t: titulo_idx[hashlib.md5(t.encode()).hexdigest()].append(i)

        resumen_idx = defaultdict(list)
        for i, r in enumerate(resumenes_norm):
            if r: resumen_idx[hashlib.md5(r[:80].encode()).hexdigest()].append(i)

        for indices in list(titulo_idx.values()) + list(resumen_idx.values()):
            nuevos = [i for i in indices if i not in usado]
            if len(nuevos) >= 2:
                grupos[gid] = nuevos
                usado.update(nuevos)
                gid += 1
        return grupos

    def _clustering_semantico(self, textos, titulos, indices):
        if len(indices) < 2:
            return {}
        BATCH_SIZE = 400
        grupos_finales = {}
        offset = 0

        for bs in range(0, len(indices), BATCH_SIZE):
            batch_idxs = indices[bs:bs + BATCH_SIZE]
            # Titulo con peso mayor + texto truncado
            batch_txts = [f"{str(titulos[i])[:160]} {str(textos[i])[:800]}" for i in batch_idxs]
            embs = get_embeddings_batch(batch_txts)
            valid_embs, final_idxs = [], []
            for k, e in enumerate(embs):
                if e is not None:
                    valid_embs.append(e)
                    final_idxs.append(batch_idxs[k])
            if len(valid_embs) < 2:
                continue

            sim_matrix = cosine_similarity(np.array(valid_embs))
            dist_matrix = np.clip(1 - sim_matrix, 0, 2)

            clustering = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=UMBRAL_CLUSTER_SUBTEMA,
                metric='precomputed',
                linkage='average'
            ).fit(dist_matrix)

            grupos = defaultdict(list)
            for i, lbl in enumerate(clustering.labels_): grupos[lbl].append(final_idxs[i])
            for lbl, idxs in grupos.items():
                if len(idxs) >= 2: grupos_finales[offset + lbl] = idxs
            offset += len(grupos)

        return grupos_finales

    def _generar_etiqueta(self, textos_muestra, titulos_muestra) -> str:
        cache_key = hashlib.md5(
            "|".join(sorted([normalize_title_for_comparison(str(t)) for t in titulos_muestra[:3]])).encode()
        ).hexdigest()
        if cache_key in self._cache:
            return self._cache[cache_key]

        palabras = []
        for t in titulos_muestra[:6]:
            palabras.extend([w for w in string_norm_label(str(t)).split() if w not in STOPWORDS_ES and len(w) > 3])
        keywords = " ".join([w for w, _ in Counter(palabras).most_common(6)])

        marca_regla = f"3. NO usar '{self.marca}' ni sus variantes." if self.marca else "3. NO usar nombres propios de empresas."

        prompt = f"""Eres un editor periodístico senior de un diario de referencia. Tu tarea es escribir un SUBTEMA que describa de forma natural y precisa de qué tratan estas noticias.

TITULOS DE MUESTRA:
{chr(10).join([f'- {str(t)[:100]}' for t in titulos_muestra[:5]])}

PALABRAS CLAVE DEL GRUPO: {keywords}

INSTRUCCIONES DE REDACCIÓN:
- Escribe entre 3 y 6 palabras, como una frase periodística real con sentido completo.
- USA preposiciones y artículos para que suene natural en español: "de", "del", "en", "al", "para", "con", "las", "los", "la", "el", etc.
- BIEN: "Protección del ecosistema de manglares" | MAL: "Protección manglar ecosistema"
- BIEN: "Acceso a energía eléctrica digna" | MAL: "Acceso energía digna"
- BIEN: "Fusión entre bancos del sector cooperativo" | MAL: "Fusión bancaria cooperativa"
- BIEN: "Lanzamiento de nueva aplicación de pagos" | MAL: "Lanzamiento app pagos móvil"
- Sé ESPECÍFICO al tema real: no uses categorías vagas como "Actividad empresarial" o "Noticias del sector".
{marca_regla}
- NO usar nombres de ciudades, departamentos ni países.
- NO empezar con verbo conjugado (no "Lanzó", sí "Lanzamiento de...").

Responde SOLO en JSON: {{"subtema":"..."}}"""

        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            _count_chat_tokens(resp)
            raw = json.loads(resp.choices[0].message.content.strip()).get("subtema", "Varios")
            subtema = limpiar_tema_geografico(limpiar_tema(raw), self.marca, self.aliases)
            if not subtema or subtema == "Sin tema":
                subtema = "Actividad Corporativa"
        except Exception:
            subtema = "Actividad Corporativa"

        self._cache[cache_key] = subtema
        return subtema

    def _fusionar_conservador(self, etiquetas: List[str], textos: List[str]) -> List[str]:
        """Fusion conservadora: evita sobre-agrupacion sin sentido."""
        df_temp = pd.DataFrame({'label': etiquetas, 'text': textos})
        unique_labels = list(df_temp['label'].unique())
        if len(unique_labels) < 2:
            return etiquetas

        todos_embs = get_embeddings_batch(textos)
        label_centroids = {}
        for label in unique_labels:
            indices = df_temp.index[df_temp['label'] == label].tolist()[:40]
            vectors = [todos_embs[i] for i in indices if todos_embs[i] is not None]
            if vectors:
                label_centroids[label] = np.mean(vectors, axis=0)

        valid_labels = [l for l in unique_labels if l in label_centroids]
        if len(valid_labels) < 2:
            return etiquetas

        matrix = np.array([label_centroids[l] for l in valid_labels])
        sim_matrix = cosine_similarity(matrix)
        dist_matrix = np.clip(1 - sim_matrix, 0, 2)

        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=1 - UMBRAL_FUSION_CONTENIDO,
            metric='precomputed',
            linkage='average'
        ).fit(dist_matrix)

        mapa_fusion = {}
        for cluster_id in set(clustering.labels_):
            idxs_cluster = [i for i, x in enumerate(clustering.labels_) if x == cluster_id]
            labels_in_cluster = [valid_labels[i] for i in idxs_cluster]
            counts = Counter([l for l in etiquetas if l in labels_in_cluster])
            representante = max(labels_in_cluster, key=lambda x: (counts[x], -len(x)))
            for lbl in labels_in_cluster:
                mapa_fusion[lbl] = representante

        return [mapa_fusion.get(lbl, lbl) for lbl in etiquetas]

    def procesar_lote(
        self, textos_concat: pd.Series, progress_bar,
        resumen_puro: pd.Series, titulos_puros: pd.Series
    ) -> List[str]:
        textos = textos_concat.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumen_puro.tolist()
        n = len(textos)

        progress_bar.progress(0.08, "⚡ Pre-agrupando noticias identicas...")
        grupos_id = self._preagrupar_identicos(textos, titulos, resumenes)

        class DSU:
            def __init__(self, n):
                self.p = list(range(n))
            def find(self, i):
                path = []
                while i != self.p[i]: path.append(i); i = self.p[i]
                for node in path: self.p[node] = i
                return i
            def union(self, i, j): self.p[self.find(j)] = self.find(i)

        dsu = DSU(n)
        for idxs in grupos_id.values():
            for j in idxs[1:]: dsu.union(idxs[0], j)

        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        indices_sueltos = [i for idxs in comp.values() if len(idxs) == 1 for i in idxs]

        if len(indices_sueltos) > 1:
            progress_bar.progress(0.25, f"🔍 Agrupando {len(indices_sueltos)} noticias unicas por contenido semantico...")
            grupos_cl = self._clustering_semantico(textos, titulos, indices_sueltos)
            for idxs in grupos_cl.values():
                for j in idxs[1:]: dsu.union(idxs[0], j)

        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)

        total_g = len(comp)
        progress_bar.progress(0.35, f"🏷️ Etiquetando {total_g} grupos tematicos...")

        mapa = {}
        for k, (lid, idxs) in enumerate(comp.items()):
            if k % 10 == 0:
                progress_bar.progress(0.35 + 0.40 * k / total_g, f"🏷️ Etiquetando {k + 1}/{total_g}")
            subtema = self._generar_etiqueta([textos[i] for i in idxs], [titulos[i] for i in idxs])
            for i in idxs: mapa[i] = subtema

        subtemas_brutos = [mapa.get(i, "Actividad Corporativa") for i in range(n)]
        n_brutos = len(set(subtemas_brutos))

        progress_bar.progress(0.80, f"🗜️ Fusion conservadora ({n_brutos} subtemas)...")
        subtemas_fusionados = self._fusionar_conservador(subtemas_brutos, textos)
        n_fusionados = len(set(subtemas_fusionados))

        st.info(f"📊 Subtemas: **{n_brutos}** → **{n_fusionados}** tras fusion conservadora")
        progress_bar.progress(1.0, "✅ Subtemas listos")
        return subtemas_fusionados


# ======================================
# CONSOLIDACION DE TEMAS PRINCIPALES
# ======================================
def consolidar_subtemas_en_temas(
    subtemas: List[str], textos: List[str], p_bar,
    marca: str = "", aliases: List[str] = None
) -> List[str]:
    p_bar.progress(0.05, text="📊 Consolidando temas principales...")

    df_t = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    unique_subtemas = list(df_t['subtema'].unique())

    if len(unique_subtemas) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "✅ Temas listos")
        return subtemas

    embs_labels = get_embeddings_batch(unique_subtemas)
    valid_idxs = [i for i, e in enumerate(embs_labels) if e is not None]
    if not valid_idxs:
        return subtemas

    valid_subtemas = [unique_subtemas[i] for i in valid_idxs]
    matrix_labels = np.array([embs_labels[i] for i in valid_idxs])

    todos_embs = get_embeddings_batch(textos)
    matrix_content = []
    for subt in valid_subtemas:
        idxs = df_t.index[df_t['subtema'] == subt].tolist()[:25]
        vecs = [todos_embs[i] for i in idxs if todos_embs[i] is not None]
        if vecs:
            matrix_content.append(np.mean(vecs, axis=0))
        else:
            orig_idx = unique_subtemas.index(subt)
            fb = embs_labels[orig_idx]
            matrix_content.append(fb if fb is not None else np.zeros(len(matrix_labels[0])))

    matrix_content = np.array(matrix_content)
    sim_final = 0.35 * cosine_similarity(matrix_labels) + 0.65 * cosine_similarity(matrix_content)

    n_clusters = min(NUM_TEMAS_PRINCIPALES, len(valid_subtemas))
    if n_clusters < 2:
        return subtemas

    clustering = AgglomerativeClustering(
        n_clusters=n_clusters, metric='precomputed', linkage='average'
    ).fit(np.clip(1 - sim_final, 0, 2))

    clusters = defaultdict(list)
    for i, label in enumerate(clustering.labels_): clusters[label].append(valid_subtemas[i])

    marca_regla = f"NO usar '{marca}'." if marca else ""
    mapa_final = {}

    for cid, lista in clusters.items():
        subtemas_str = ", ".join(lista[:8])
        prompt = f"""Crea una categoria tematica periodistica (2-4 palabras) para estos subtemas:
{subtemas_str}

REGLAS: {marca_regla} NO verbos. NO ciudades. Ser descriptivo y concreto.
EJEMPLOS: "Resultados Financieros", "Sostenibilidad Ambiental", "Innovacion Digital", "Gestion Laboral", "Alianzas Estrategicas".

Responde SOLO el nombre, sin comillas ni puntuacion."""

        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=18,
                temperature=0.1
            )
            _count_chat_tokens(resp)
            nombre = limpiar_tema_geografico(
                limpiar_tema(resp.choices[0].message.content.strip().replace('"', '').replace('.', '')),
                marca, aliases or []
            )
        except Exception:
            nombre = lista[0]

        for subt in lista: mapa_final[subt] = nombre

    temas_finales = [mapa_final.get(s, s) for s in subtemas]
    st.info(f"📉 Temas consolidados en **{len(set(temas_finales))}** categorias")
    p_bar.progress(1.0, "✅ Temas finalizados")
    return temas_finales


def analizar_temas_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[str]]:
    try:
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"❌ Error al procesar `pipeline_tema.pkl`: {e}")
        return None


# ======================================
# Logica de Duplicados y Excel
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
                else:
                    seen_online_url[key] = i
            if medio_norm and mencion_norm:
                online_title_buckets[(medio_norm, mencion_norm)].append(i)

        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
                if key in seen_broadcast:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_broadcast[key]].get(key_map.get("idnoticia"), "")
                else:
                    seen_broadcast[key] = i

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
        base = {
            k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value
            for k, v in r_cells.items()
        }
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
        if row["is_duplicate"]:
            row.update({key_map["tonoiai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["subtema"]: "Duplicada"})
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
    final_order = [
        "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Region",
        "Seccion - Programa", "Titulo", "Autor - Conductor", "Nro. Pagina",
        "Dimension", "Duracion - Nro. Caracteres", "CPE", "Audiencia", "Tier",
        "Tono", "Tono IA", "Tema", "Subtema", "Link Nota",
        "Resumen - Aclaracion", "Link (Streaming - Imagen)", "Menciones - Empresa", "ID duplicada"
    ]
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
# PROCESO PRINCIPAL
# ======================================
async def run_full_process_async(
    dossier_file, region_file, internet_file,
    brand_name, brand_aliases, tono_pkl_file, tema_pkl_file, analysis_mode
):
    for k in ['tokens_input', 'tokens_output', 'tokens_embedding']:
        st.session_state[k] = 0

    start_time = time.time()

    if "API" in analysis_mode or "Hibrido" in analysis_mode or "Híbrido" in analysis_mode:
        try:
            openai.api_key = st.secrets["OPENAI_API_KEY"]
            openai.aiosession.set(None)
        except Exception:
            st.error("❌ Error: OPENAI_API_KEY no encontrado.")
            st.stop()

    with st.status("📋 **Paso 1/5:** Limpieza y deduplicacion", expanded=True) as s:
        wb = load_workbook(dossier_file, data_only=True)
        all_processed_rows, key_map = run_dossier_logic(wb.active)
        s.update(label="✅ **Paso 1/5:** Limpieza completada", state="complete")

    with st.status("🗺️ **Paso 2/5:** Mapeos y Normalizacion", expanded=True) as s:
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
        s.update(label="✅ **Paso 2/5:** Mapeos aplicados", state="complete")

    gc.collect()
    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]

    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        df_temp["resumen_api"] = df_temp[key_map["titulo"]].fillna("").astype(str) + ". " + df_temp[key_map["resumen"]].fillna("").astype(str)

        marca_efectiva = brand_name.strip() if brand_name else ""
        if not marca_efectiva:
            st.info("ℹ️ Sin nombre de marca: el tono se analiza por contexto general del texto.")

        # PASO 3: TONO
        with st.status("🎯 **Paso 3/5:** Analisis de Tono", expanded=True) as s:
            p_bar = st.progress(0)
            if ("PKL" in analysis_mode or "Híbrido" in analysis_mode or "Hibrido" in analysis_mode) and tono_pkl_file:
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
                st.success("✅ Tono clasificado con modelo PKL")
            elif "API" in analysis_mode or "Híbrido" in analysis_mode or "Hibrido" in analysis_mode:
                clasif_tono = ClasificadorTonoV4(marca_efectiva, brand_aliases)
                resultados_tono = await clasif_tono.procesar_lote_async(
                    df_temp["resumen_api"], p_bar,
                    df_temp[key_map["resumen"]], df_temp[key_map["titulo"]]
                )
            else:
                resultados_tono = [{"tono": "N/A"}] * len(rows_to_analyze)

            df_temp[key_map["tonoiai"]] = [res["tono"] for res in resultados_tono]
            s.update(label="✅ **Paso 3/5:** Tono analizado", state="complete")

        # PASO 4: TEMA Y SUBTEMA
        with st.status("🏷️ **Paso 4/5:** Analisis de Tema y Subtema", expanded=True) as s:
            p_bar = st.progress(0)

            if "Solo Modelos PKL" in analysis_mode and not tema_pkl_file:
                subtemas = ["N/A"] * len(rows_to_analyze)
                temas_principales = ["N/A"] * len(rows_to_analyze)
            else:
                clasif_sub = ClasificadorSubtemaV4(marca_efectiva, brand_aliases)
                subtemas = clasif_sub.procesar_lote(
                    df_temp["resumen_api"], p_bar,
                    df_temp[key_map["resumen"]], df_temp[key_map["titulo"]]
                )
                p_bar.progress(0.0, "📊 Consolidando temas principales...")
                temas_principales = consolidar_subtemas_en_temas(
                    subtemas, df_temp["resumen_api"].tolist(), p_bar, marca_efectiva, brand_aliases
                )

            df_temp[key_map["subtema"]] = subtemas

            if ("PKL" in analysis_mode or "Híbrido" in analysis_mode or "Hibrido" in analysis_mode) and tema_pkl_file:
                temas_pkl = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                df_temp[key_map["tema"]] = temas_pkl if temas_pkl else temas_principales
            else:
                df_temp[key_map["tema"]] = temas_principales

            s.update(label="✅ **Paso 4/5:** Clasificacion completada", state="complete")

        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"):
                row.update(results_map.get(row["original_index"], {}))

    gc.collect()

    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    total_cost = cost_input + cost_output + cost_embedding

    with st.status("📊 **Paso 5/5:** Generando informe final", expanded=True) as s:
        duration_str = f"{time.time() - start_time:.0f}s"
        st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = (
            f"Informe_IA_{(brand_name or 'general').replace(' ', '_')}"
            f"_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name": brand_name, "brand_aliases": brand_aliases,
            "total_rows": len(all_processed_rows), "unique_rows": len(rows_to_analyze),
            "duplicates": len(all_processed_rows) - len(rows_to_analyze),
            "process_duration": duration_str,
            "process_cost": f"${total_cost:.4f} USD",
            "tokens_detail": {
                "input": st.session_state['tokens_input'],
                "output": st.session_state['tokens_output'],
                "embedding": st.session_state['tokens_embedding'],
            }
        })
        s.update(label="✅ **Paso 5/5:** Proceso completado", state="complete")


# ======================================
# Analisis Rapido
# ======================================
async def run_quick_analysis_async(
    df: pd.DataFrame, title_col: str, summary_col: str,
    brand_name: str, aliases: List[str]
) -> pd.DataFrame:
    for k in ['tokens_input', 'tokens_output', 'tokens_embedding']:
        st.session_state[k] = 0

    df['texto_analisis'] = df[title_col].fillna('').astype(str) + ". " + df[summary_col].fillna('').astype(str)
    marca_efectiva = brand_name.strip() if brand_name else ""

    with st.status("🎯 **Paso 1/2:** Analizando Tono...", expanded=True) as s:
        p_bar = st.progress(0, "Iniciando analisis de tono contextual...")
        clasif_tono = ClasificadorTonoV4(marca_efectiva, aliases)
        resultados_tono = await clasif_tono.procesar_lote_async(
            df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna('')
        )
        df['Tono IA'] = [res["tono"] for res in resultados_tono]
        s.update(label="✅ **Paso 1/2:** Tono Analizado", state="complete")

    with st.status("🏷️ **Paso 2/2:** Analizando Tema y Subtema...", expanded=True) as s:
        p_bar = st.progress(0, "Generando subtemas...")
        clasif_sub = ClasificadorSubtemaV4(marca_efectiva, aliases)
        subtemas = clasif_sub.procesar_lote(
            df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna('')
        )
        df['Subtema'] = subtemas
        p_bar.progress(0.5, "Consolidando temas principales...")
        df['Tema'] = consolidar_subtemas_en_temas(subtemas, df["texto_analisis"].tolist(), p_bar, marca_efectiva, aliases)
        s.update(label="✅ **Paso 2/2:** Clasificacion finalizada", state="complete")

    df.drop(columns=['texto_analisis'], inplace=True, errors='ignore')
    cost = (
        (st.session_state['tokens_input'] / 1e6) * PRICE_INPUT_1M
        + (st.session_state['tokens_output'] / 1e6) * PRICE_OUTPUT_1M
        + (st.session_state['tokens_embedding'] / 1e6) * PRICE_EMBEDDING_1M
    )
    st.session_state['quick_cost'] = f"${cost:.4f} USD"
    return df


def generate_quick_analysis_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Analisis')
    return output.getvalue()


def render_quick_analysis_tab():
    st.header("⚡ Analisis Rapido con IA")
    st.caption("Sube cualquier Excel con titulos y resumenes para obtener Tono, Tema y Subtema.")

    if 'quick_analysis_result' in st.session_state:
        st.success("🎉 Analisis Rapido Completado")
        cost = st.session_state.get('quick_cost', "$0.00")
        col1, col2 = st.columns([1, 3])
        with col1:
            st.markdown(f'<div class="metric-card"><span class="metric-value red">{cost}</span><div class="metric-label">Costo Estimado</div></div>', unsafe_allow_html=True)
        result_df = st.session_state.quick_analysis_result
        cols_show = [c for c in ['Tono IA', 'Tema', 'Subtema'] if c in result_df.columns]
        st.dataframe(result_df[cols_show].head(15), use_container_width=True)
        excel_data = generate_quick_analysis_excel(result_df)
        st.download_button(
            label="📥 **Descargar Resultados**",
            data=excel_data,
            file_name="Analisis_Rapido_IA.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary"
        )
        if st.button("🔄 Nuevo Analisis"):
            for key in ['quick_analysis_result', 'quick_analysis_df', 'quick_file_name', 'quick_cost']:
                st.session_state.pop(key, None)
            st.rerun()
        return

    if 'quick_analysis_df' not in st.session_state:
        quick_file = st.file_uploader("📂 **Sube tu archivo Excel**", type=["xlsx"], label_visibility="collapsed", key="quick_uploader")
        if quick_file:
            with st.spinner("Leyendo archivo..."):
                try:
                    st.session_state.quick_analysis_df = pd.read_excel(quick_file)
                    st.session_state.quick_file_name = quick_file.name
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {e}")
    else:
        st.success(f"✅ Archivo **'{st.session_state.quick_file_name}'** cargado.")
        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df
            columns = df.columns.tolist()
            col1, col2 = st.columns(2)
            title_col = col1.selectbox("Columna **Titulo**", options=columns, index=0)
            summary_col = col2.selectbox("Columna **Resumen**", options=columns, index=min(1, len(columns) - 1))
            st.divider()
            brand_name = st.text_input("**Marca Principal** *(opcional si usas PKL de tono)*", placeholder="Ej: Bancolombia  |  Dejar vacío si no aplica")
            brand_aliases_text = st.text_area("**Alias** *(separados por ;, opcional)*", placeholder="Ej: Ban;Juan Carlos Mora", height=70)
            if st.form_submit_button("🚀 **Analizar**", use_container_width=True, type="primary"):
                try:
                    openai.api_key = st.secrets["OPENAI_API_KEY"]
                    openai.aiosession.set(None)
                except Exception:
                    st.error("❌ OPENAI_API_KEY no encontrada.")
                    st.stop()
                aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                with st.spinner("🧠 Analizando..."):
                    st.session_state.quick_analysis_result = asyncio.run(
                        run_quick_analysis_async(df.copy(), title_col, summary_col, brand_name, aliases)
                    )
                st.rerun()

        if st.button("⬅️ Cargar otro archivo"):
            for k in ['quick_analysis_df', 'quick_file_name', 'quick_analysis_result', 'quick_cost']:
                st.session_state.pop(k, None)
            st.rerun()


# ======================================
# MAIN
# ======================================
def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown(
        '<div class="main-header">'
        '<span class="kicker">Sistema de Análisis de Medios</span>'
        '<h1>Análisis de Noticias con IA</h1>'
        '</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<div class="subtitle">v8.0 &nbsp;·&nbsp; Tono contextual por marca &nbsp;·&nbsp; '
        'Subtemas narrativos &nbsp;·&nbsp; Propagación grupal &nbsp;·&nbsp; Marca opcional</div>',
        unsafe_allow_html=True
    )

    tab1, tab2 = st.tabs(["📊 Analisis Completo", "⚡ Analisis Rapido"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("input_form"):
                st.markdown("### 📂 Archivos de Entrada")
                col1, col2, col3 = st.columns(3)
                dossier_file = col1.file_uploader("**1. Dossier** (.xlsx)", type=["xlsx"])
                region_file = col2.file_uploader("**2. Region** (.xlsx)", type=["xlsx"])
                internet_file = col3.file_uploader("**3. Internet** (.xlsx)", type=["xlsx"])

                st.markdown("### 🏢 Configuracion de Marca")
                st.caption("💡 Opcional si usas modelo PKL de tono. Si se omite, el tono se clasifica por contexto general del texto.")
                brand_name = st.text_input("**Marca Principal**", placeholder="Ej: Bancolombia  (opcional)", key="main_brand_name")
                brand_aliases_text = st.text_area("**Alias** *(sep. por ;)*", placeholder="Ej: Ban;Juan Carlos Mora", height=65, key="main_brand_aliases")

                st.markdown("### ⚙️ Modo de Analisis")
                analysis_mode = st.radio(
                    "Selecciona modo:",
                    options=["Híbrido (PKL + API)", "Solo API de OpenAI", "Solo Modelos PKL"],
                    index=0, key="analysis_mode_radio", horizontal=True
                )
                if "PKL" in analysis_mode or "Híbrido" in analysis_mode:
                    c1, c2 = st.columns(2)
                    tono_pkl_file = c1.file_uploader("`pipeline_sentimiento.pkl`", type=["pkl"])
                    tema_pkl_file = c2.file_uploader("`pipeline_tema.pkl`", type=["pkl"])
                else:
                    tono_pkl_file = tema_pkl_file = None

                if st.form_submit_button("🚀 **INICIAR ANALISIS**", use_container_width=True, type="primary"):
                    if not all([dossier_file, region_file, internet_file]):
                        st.error("❌ Faltan archivos base (Dossier, Region, Internet).")
                    else:
                        aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(
                            dossier_file, region_file, internet_file,
                            brand_name, aliases, tono_pkl_file, tema_pkl_file, analysis_mode
                        ))
                        st.rerun()
        else:
            st.markdown("## 🎉 Analisis Completado")
            c1, c2, c3, c4, c5 = st.columns(5)
            for col, val, label, cls in [
                (c1, st.session_state.total_rows, "Total", ""),
                (c2, st.session_state.unique_rows, "Únicas", "green"),
                (c3, st.session_state.duplicates, "Duplicados", "yellow"),
                (c4, st.session_state.process_duration, "Tiempo", "purple"),
                (c5, st.session_state.get("process_cost", "$0.00"), "Costo Est.", "red"),
            ]:
                col.markdown(
                    f'<div class="metric-card"><span class="metric-value {cls}">{val}</span>'
                    f'<div class="metric-label">{label}</div></div>',
                    unsafe_allow_html=True
                )

            with st.expander("📈 Detalle de uso de tokens"):
                tok = st.session_state.get("tokens_detail", {})
                st.markdown(
                    f"**Input:** {tok.get('input', 0):,} &nbsp;|&nbsp; "
                    f"**Output:** {tok.get('output', 0):,} &nbsp;|&nbsp; "
                    f"**Embeddings:** {tok.get('embedding', 0):,} tokens"
                )

            st.markdown('<div class="success-card">', unsafe_allow_html=True)
            st.download_button(
                "📥 **DESCARGAR INFORME**",
                data=st.session_state.output_data,
                file_name=st.session_state.output_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, type="primary"
            )
            if st.button("🔄 **Nuevo Analisis**", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        render_quick_analysis_tab()

    st.markdown(
        "<hr><div style='text-align:center;font-family:var(--font-mono,monospace);font-size:0.6rem;"
        "letter-spacing:0.14em;text-transform:uppercase;color:#aaa;padding:1rem 0;'>"
        "v8.0.0 &nbsp;·&nbsp; Realizado por Johnathan Cortés ©️ &nbsp;·&nbsp; "
        "Tono relativo a la marca &nbsp;·&nbsp; Subtemas narrativos &nbsp;·&nbsp; Propagación grupal"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
