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

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias · IA",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

OPENAI_MODEL_EMBEDDING     = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

CONCURRENT_REQUESTS          = 50
SIMILARITY_THRESHOLD_TONO    = 0.92
SIMILARITY_THRESHOLD_TITULOS = 0.93

# ─── Umbrales de clustering ────────────────────────────────────────────────────
UMBRAL_SUBTEMA = 0.82
UMBRAL_TEMA    = 0.76
NUM_TEMAS_MAX  = 20

# ─── Umbral de deduplicación de etiquetas ──────────────────────────────────────
UMBRAL_DEDUP_LABEL = 0.78

# ─── Umbral para fusionar grupos antes de etiquetar ───────────────────────────
UMBRAL_FUSION_INTERGRUPO = 0.84

# ─── Máximo de iteraciones para fusión inter-grupo convergente ─────────────────
MAX_ITER_FUSION = 5

PRICE_INPUT_1M     = 0.10
PRICE_OUTPUT_1M    = 0.40
PRICE_EMBEDDING_1M = 0.02

if 'tokens_input'     not in st.session_state: st.session_state['tokens_input']     = 0
if 'tokens_output'    not in st.session_state: st.session_state['tokens_output']    = 0
if 'tokens_embedding' not in st.session_state: st.session_state['tokens_embedding'] = 0

# ── Geografía ─────────────────────────────────────────────────────────────────
CIUDADES_COLOMBIA = {
    "bogotá","bogota","medellín","medellin","cali","barranquilla","cartagena","cúcuta","cucuta",
    "bucaramanga","pereira","manizales","armenia","ibagué","ibague","villavicencio","montería",
    "monteria","neiva","pasto","valledupar","popayán","popayan","tunja","florencia","sincelejo",
    "riohacha","yopal","santa marta","santamarta","quibdó","quibdo","leticia","mocoa","mitú","mitu",
    "puerto carreño","inírida","inirida","san josé del guaviare","antioquia","atlántico","atlantico",
    "bolívar","bolivar","boyacá","boyaca","caldas","caquetá","caqueta","casanare","cauca","cesar",
    "chocó","choco","córdoba","cordoba","cundinamarca","guainía","guainia","guaviare","huila",
    "la guajira","magdalena","meta","nariño","narino","norte de santander","putumayo","quindío",
    "quindio","risaralda","san andrés","san andres","santander","sucre","tolima","valle del cauca",
    "vaupés","vaupes","vichada",
}
GENTILICIOS_COLOMBIA = {
    "bogotano","bogotanos","bogotana","bogotanas","capitalino","capitalinos","capitalina","capitalinas",
    "antioqueño","antioqueños","antioqueña","antioqueñas","paisa","paisas","medellense","medellenses",
    "caleño","caleños","caleña","caleñas","valluno","vallunos","valluna","vallunas","vallecaucano",
    "vallecaucanos","barranquillero","barranquilleros","cartagenero","cartageneros","costeño","costeños",
    "costeña","costeñas","cucuteño","cucuteños","bumangués","santandereano","santandereanos",
    "boyacense","boyacenses","tolimense","tolimenses","huilense","huilenses","nariñense","nariñenses",
    "pastuso","pastusas","cordobés","cordobeses","cauca","caucano","caucanos","chocoano","chocoanos",
    "casanareño","casanareños","caqueteño","caqueteños","guajiro","guajiros","llanero","llaneros",
    "amazonense","amazonenses","colombiano","colombianos","colombiana","colombianas",
}

STOPWORDS_ES = set("""
a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so
sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro
nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas
que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era
eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba
estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto
cada muy todo toda todos todas ser haber hacer tener poder deber ir dar ver saber querer llegar
pasar encontrar creer decir poner salir volver seguir llevar sentir cambiar
""".split())

POS_VARIANTS = [
    r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)",
    r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)",
    r"apertur(a|ar|ara|o|an)",r"estren(a|o|ara|an|ando)",r"habilit(a|o|ara|an|ando)",
    r"disponible",r"mejor(a|o|an|ando)",r"optimiza|amplia|expande",
    r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?",
    r"crecimi?ento|aument(a|o|an|ando)",r"gananci(a|as)|utilidad(es)?|benefici(o|os)",
    r"expansion|crece|crecer",r"inversion|invierte|invertir",
    r"innova(cion|dor|ndo)|moderniza",r"exito(so|sa)?|logr(o|os|a|an|ando)",
    r"reconoci(miento|do|da)|premi(o|os|ada)",r"lidera(zgo)?|lider",
    r"consolida|fortalece",r"oportunidad(es)?|potencial",r"solucion(es)?|resuelve",
    r"eficien(te|cia)",r"calidad|excelencia",r"satisfaccion|complace",
    r"confianza|credibilidad",r"sostenible|responsable",r"compromiso|apoya|apoyar",
    r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)",r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)",
    r"destaca(r|do|da|ndo)?",r"supera(r|ndo|cion)?",r"record|hito|milestone",
    r"avanza(r|do|da|ndo)?",r"benefici(a|o|ando|ar|ando)",r"importante(s)?",
    r"prioridad",r"bienestar",r"garantizar",r"seguridad",r"atencion",
    r"expres(o|ó|ando)",r"señala(r|do|ando)",r"ratific(a|o|ando|ar)",
]
NEG_VARIANTS = [
    r"demanda|denuncia|sanciona|multa|investiga|critica",
    r"cae|baja|pierde|crisis|quiebra|default",
    r"fraude|escandalo|irregularidad",
    r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga",
    r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora",
    r"problema(s|tica|ico)?|dificultad(es)?",r"retras(o|a|ar|ado)",r"perdida(s)?|deficit",
    r"conflict(o|os)?|disputa(s)?",r"rechaz(a|o|ar|ado)",r"negativ(o|a|os|as)",
    r"preocupa(cion|nte|do)?",r"alarma(nte)?|alerta",r"riesgo(s)?|amenaza(s)?",
]
CRISIS_KEYWORDS  = re.compile(r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS   = re.compile(r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)
POS_PATTERNS     = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS     = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]

# ======================================
# CSS
# ======================================
def load_custom_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700&family=JetBrains+Mono:wght@300;400;500&display=swap');

:root {
    --bg:       #1a1a1e;
    --s1:       #202024;
    --s2:       #28282e;
    --s3:       #323238;
    --border:   #38383f;
    --border2:  #48484f;
    --text:     #ececec;
    --text2:    #a0a0a8;
    --text3:    #606068;
    --accent:   #d4a574;
    --accent2:  #b8865a;
    --green:    #7cb88a;
    --red:      #c87070;
    --blue:     #7a9ec4;
    --r:        10px;
    --r2:       16px;
}

html, body, [data-testid="stApp"] {
    background: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    font-weight: 400;
    -webkit-font-smoothing: antialiased;
}

.app-header {
    padding: 2rem 0 1.5rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    gap: 1rem;
}
.app-header-mark {
    font-family: 'Source Serif 4', serif;
    font-size: 2.4rem;
    color: var(--accent);
    line-height: 1;
    user-select: none;
    opacity: 0.9;
}
.app-header-text {}
.app-header-title {
    font-family: 'Source Serif 4', serif;
    font-size: 1.6rem;
    font-weight: 700;
    color: var(--text);
    line-height: 1.2;
    letter-spacing: -0.02em;
}
.app-header-version {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    color: var(--text3);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-top: 0.2rem;
}

[data-testid="stTabs"] [data-testid="stTabsList"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r) !important;
    padding: 4px !important;
    gap: 4px !important;
}
[data-testid="stTabs"] button[data-baseweb="tab"] {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    color: var(--text2) !important;
    border-radius: 7px !important;
    padding: 0.5rem 1.2rem !important;
    border: none !important;
    background: transparent !important;
    transition: all 0.15s ease !important;
}
[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
    background: var(--s2) !important;
    color: var(--accent) !important;
    border: 1px solid var(--border2) !important;
    font-weight: 600 !important;
}

.metrics-row {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 0.8rem;
    margin: 1.5rem 0;
}
.metric-card {
    background: var(--s1);
    border: 1px solid var(--border);
    border-radius: var(--r2);
    padding: 1.2rem 0.8rem;
    text-align: center;
    transition: border-color 0.15s, transform 0.15s;
}
.metric-card:hover { border-color: var(--border2); transform: translateY(-1px); }
.metric-val {
    font-family: 'Source Serif 4', serif;
    font-size: 1.8rem;
    font-weight: 700;
    line-height: 1;
    margin-bottom: 0.4rem;
}
.metric-lbl {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.62rem;
    color: var(--text3);
    text-transform: uppercase;
    letter-spacing: 0.12em;
}

[data-testid="stForm"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r2) !important;
    padding: 1.8rem !important;
}

.sec-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.64rem;
    color: var(--text3);
    letter-spacing: 0.16em;
    text-transform: uppercase;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
    margin: 1.5rem 0 0.8rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.sec-label::before {
    content: '';
    display: inline-block;
    width: 3px;
    height: 10px;
    background: var(--accent2);
    border-radius: 2px;
}

[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea {
    background: var(--s2) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: var(--r) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 400 !important;
    transition: border-color 0.15s, box-shadow 0.15s !important;
}
[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    border-color: var(--accent2) !important;
    box-shadow: 0 0 0 2px rgba(212,165,116,0.12) !important;
    outline: none !important;
}
label[data-testid="stWidgetLabel"] p {
    color: var(--text2) !important;
    font-size: 0.85rem !important;
}

[data-testid="stFileUploader"] {
    background: var(--s2) !important;
    border: 1px dashed var(--border2) !important;
    border-radius: var(--r) !important;
    transition: border-color 0.15s !important;
}
[data-testid="stFileUploader"]:hover { border-color: var(--accent2) !important; }

.stButton > button,
[data-testid="stDownloadButton"] > button {
    background: var(--s2) !important;
    border: 1px solid var(--border2) !important;
    color: var(--text) !important;
    border-radius: var(--r) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
    transition: all 0.15s ease !important;
    padding: 0.5rem 1.2rem !important;
}
.stButton > button:hover,
[data-testid="stDownloadButton"] > button:hover {
    border-color: var(--accent) !important;
    color: var(--accent) !important;
    background: var(--s1) !important;
}
.stButton > button[kind="primary"],
[data-testid="stDownloadButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, var(--accent2) 0%, var(--accent) 100%) !important;
    border: none !important;
    color: #1a1a1e !important;
    font-weight: 600 !important;
    box-shadow: 0 2px 10px rgba(212,165,116,0.18) !important;
}
.stButton > button[kind="primary"]:hover,
[data-testid="stDownloadButton"] > button[kind="primary"]:hover {
    box-shadow: 0 4px 18px rgba(212,165,116,0.30) !important;
    transform: translateY(-1px) !important;
    color: #1a1a1e !important;
}

[data-testid="stRadio"] label { color: var(--text2) !important; font-size: 0.85rem !important; }
[data-testid="stRadio"] [aria-checked="true"] + div label { color: var(--accent) !important; }

[data-testid="stStatus"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
}

[data-testid="stAlert"] {
    background: var(--s2) !important;
    border: 1px solid var(--border2) !important;
    border-radius: var(--r) !important;
    color: var(--text2) !important;
    font-size: 0.84rem !important;
}

.success-banner {
    background: linear-gradient(135deg, var(--s1) 0%, var(--s2) 100%);
    border: 1px solid var(--green);
    border-left: 3px solid var(--green);
    border-radius: var(--r2);
    padding: 1.4rem 1.6rem;
    margin: 0.8rem 0 1.4rem;
}
.success-title {
    font-family: 'Source Serif 4', serif;
    font-size: 1.3rem;
    color: var(--green);
    margin-bottom: 0.15rem;
}
.success-sub {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: var(--text3);
    letter-spacing: 0.08em;
}

.auth-wrap { max-width: 360px; margin: 10vh auto 0; }
.auth-title {
    font-family: 'Source Serif 4', serif;
    font-size: 2.4rem;
    color: var(--accent);
    text-align: center;
    margin-bottom: 0.2rem;
}
.auth-sub {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: var(--text3);
    text-align: center;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 2rem;
}

[data-testid="stProgressBar"] > div > div { background: var(--accent) !important; border-radius: 4px !important; }
[data-testid="stDataFrame"] { border: 1px solid var(--border) !important; border-radius: var(--r) !important; }

.cluster-info {
    background: var(--s2);
    border: 1px solid var(--border2);
    border-left: 3px solid var(--accent2);
    border-radius: var(--r);
    padding: 0.9rem 1.1rem;
    margin: 0.6rem 0;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    color: var(--text2);
    line-height: 1.7;
}

hr { border-color: var(--border) !important; }

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--accent2); }

.footer {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.62rem;
    color: var(--text3);
    text-align: center;
    padding: 1.5rem 0 0.8rem;
    letter-spacing: 0.1em;
    border-top: 1px solid var(--border);
    margin-top: 2.5rem;
}
</style>
""", unsafe_allow_html=True)


# ======================================
# Caché Global de Embeddings
# ======================================
# MEJORA CRÍTICA: Evita recalcular embeddings que ya se obtuvieron.
# Antes, el mismo texto se embeddía hasta 3 veces (tono, subtema, tema).
# Ahora se calcula una sola vez y se reutiliza.

class EmbeddingCache:
    """Caché global de embeddings por hash de texto para evitar recálculos."""

    def __init__(self):
        self._cache: Dict[str, List[float]] = {}
        self._hits = 0
        self._misses = 0

    def _key(self, text: str) -> str:
        return hashlib.md5(text[:2000].encode('utf-8', errors='ignore')).hexdigest()

    def get(self, text: str) -> Optional[List[float]]:
        k = self._key(text)
        if k in self._cache:
            self._hits += 1
            return self._cache[k]
        self._misses += 1
        return None

    def put(self, text: str, emb: List[float]):
        self._cache[self._key(text)] = emb

    def get_many(self, textos: List[str]) -> Tuple[List[Optional[List[float]]], List[int]]:
        """Retorna embeddings cacheados y lista de índices que faltan."""
        results = [None] * len(textos)
        missing = []
        for i, t in enumerate(textos):
            cached = self.get(t)
            if cached is not None:
                results[i] = cached
            else:
                missing.append(i)
        return results, missing

    def stats(self) -> str:
        total = self._hits + self._misses
        rate = (self._hits / total * 100) if total > 0 else 0
        return f"Cache: {self._hits} hits, {self._misses} misses ({rate:.0f}% hit rate)"

    def clear(self):
        self._cache.clear()
        self._hits = 0
        self._misses = 0


# Instancia global — persiste durante toda la sesión de procesamiento
if '_emb_cache' not in st.session_state:
    st.session_state['_emb_cache'] = EmbeddingCache()

def get_embedding_cache() -> EmbeddingCache:
    return st.session_state['_emb_cache']


# ======================================
# Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True
    st.markdown("""
    <div class="auth-wrap">
        <div class="auth-title">◈</div>
        <div class="auth-sub">Acceso restringido · Sistema IA</div>
    </div>
    """, unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("password_form"):
            password = st.text_input("Contraseña", type="password", placeholder="···")
            if st.form_submit_button("Ingresar", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID"):
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta")
    return False


def call_with_retries(fn, *args, **kwargs):
    delay = 1
    for attempt in range(3):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == 2:
                raise e
            time.sleep(delay)
            delay *= 2


async def acall_with_retries(fn, *args, **kwargs):
    delay = 1
    for attempt in range(3):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            if attempt == 2:
                raise e
            await asyncio.sleep(delay)
            delay *= 2


def norm_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))


def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"\'')
    # Eliminar prefijos comunes que el LLM a veces añade
    for prefix in ["subtema:", "tema:", "categoría:", "categoria:"]:
        if tema.lower().startswith(prefix):
            tema = tema[len(prefix):].strip()
    if tema:
        tema = tema[0].upper() + tema[1:]
    invalid_end = {"en", "de", "del", "la", "el", "y", "o", "con", "sin", "por", "para", "sobre",
                   "los", "las", "un", "una", "al", "su", "sus", "que", "se"}
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_end:
        palabras.pop()
    tema = " ".join(palabras[:7])
    return tema if tema else "Sin tema"


def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema:
        return "Sin tema"
    tl = tema.lower()
    for name in [marca] + [a for a in aliases if a]:
        tl = re.sub(rf'\b{re.escape(unidecode(name.lower()))}\b', '', tl)
    for ciudad in CIUDADES_COLOMBIA:
        tl = re.sub(rf'\b{re.escape(ciudad)}\b', '', tl)
    for gent in GENTILICIOS_COLOMBIA:
        tl = re.sub(rf'\b{re.escape(gent)}\b', '', tl)
    for frase in ["en colombia", "de colombia", "del pais", "en el pais", "nacional",
                   "colombiano", "colombiana", "colombianos", "colombianas", "territorio nacional"]:
        tl = re.sub(rf'\b{re.escape(frase)}\b', '', tl)
    palabras = [p.strip() for p in tl.split() if p.strip()]
    if not palabras:
        return "Sin tema"
    tl = palabras[0].upper() + (" ".join(palabras))[len(palabras[0]):]
    return limpiar_tema(tl)


def string_norm_label(s: str) -> str:
    if not s:
        return ""
    s = unidecode(s.lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(t for t in s.split() if t not in STOPWORDS_ES)


def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        m = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if m:
            return {"value": "Link", "url": m.group(1)}
    return {"value": cell.value, "url": None}


def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str):
        return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    return re.sub(r"\W+", " ", tmp[0]).lower().strip()


def clean_title_for_output(title: Any) -> str:
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()


def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str):
        return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    m = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if m:
        text = text[m.start():]
    if text and not text.endswith("..."):
        text = text.rstrip(".") + "..."
    return text


def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str):
        return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    return {
        "fm": "Radio", "am": "Radio", "radio": "Radio",
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión",
        "television": "Televisión", "televisión": "Televisión",
        "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa",
        "revista": "Revista", "revistas": "Revista",
        "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet",
    }.get(t, str(tipo_raw).strip().title() or "Otro")


# ======================================
# Texto ponderado para embeddings
# ======================================
# MEJORA: El título se repite 2x para que pese más en el embedding.
# Antes era simplemente "titulo. resumen", lo que diluía la señal del título
# en resúmenes largos. Ahora el embedding captura mejor el tema real.

def texto_para_embedding(titulo: str, resumen: str, max_len: int = 1800) -> str:
    """Genera texto optimizado para embedding con título ponderado."""
    t = str(titulo or "").strip()
    r = str(resumen or "").strip()
    # Repetir título 2x para darle más peso semántico
    combined = f"{t}. {t}. {r}"
    return combined[:max_len]


# ======================================
# Deduplicación de etiquetas por similitud de cadena
# ======================================
def dedup_labels(etiquetas: List[str], umbral: float = UMBRAL_DEDUP_LABEL) -> List[str]:
    """
    Fusiona etiquetas cuya representación normalizada tenga similitud >= umbral.
    Elige la etiqueta más frecuente como canónica (no la más corta).
    """
    unique = list(dict.fromkeys(etiquetas))
    if len(unique) <= 1:
        return etiquetas

    normed = [string_norm_label(u) for u in unique]
    n = len(unique)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        if not normed[i]:
            continue
        for j in range(i + 1, n):
            if not normed[j]:
                continue
            if find(i) == find(j):
                continue
            sim = SequenceMatcher(None, normed[i], normed[j]).ratio()
            if sim >= umbral:
                union(i, j)

    # Contar frecuencias para elegir la más común como canónica
    freq = Counter(etiquetas)

    grupos: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        grupos[find(i)].append(i)

    canon: Dict[int, str] = {}
    for root, members in grupos.items():
        candidates = [unique[m] for m in members]
        valid = [c for c in candidates if c not in ("Sin tema", "Varios")]
        if valid:
            # Elegir la más frecuente; en empate, la más corta
            canon[root] = max(valid, key=lambda c: (freq[c], -len(c)))
        else:
            canon[root] = candidates[0]

    label_map: Dict[str, str] = {}
    for i in range(n):
        label_map[unique[i]] = canon[find(i)]

    return [label_map.get(e, e) for e in etiquetas]


# ======================================
# Embeddings con caché
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    """Obtiene embeddings usando caché global. Solo llama a la API para textos no cacheados."""
    if not textos:
        return []

    cache = get_embedding_cache()
    resultados, missing_idxs = cache.get_many(textos)

    if not missing_idxs:
        return resultados

    # Solo enviar a la API los que faltan
    missing_textos = [textos[i][:2000] if textos[i] else "" for i in missing_idxs]

    for i in range(0, len(missing_textos), batch_size):
        batch = missing_textos[i:i + batch_size]
        batch_orig_idxs = missing_idxs[i:i + batch_size]
        try:
            resp = call_with_retries(
                openai.Embedding.create, input=batch, model=OPENAI_MODEL_EMBEDDING
            )
            usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if usage:
                total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                st.session_state['tokens_embedding'] += (total or 0)
            for j, d in enumerate(resp["data"]):
                orig_idx = batch_orig_idxs[j]
                emb = d["embedding"]
                resultados[orig_idx] = emb
                cache.put(textos[orig_idx], emb)
        except:
            for j, t in enumerate(batch):
                orig_idx = batch_orig_idxs[j]
                try:
                    r = openai.Embedding.create(input=[t], model=OPENAI_MODEL_EMBEDDING)
                    emb = r["data"][0]["embedding"]
                    resultados[orig_idx] = emb
                    cache.put(textos[orig_idx], emb)
                except:
                    pass

    return resultados


# ======================================
# DSU (Union-Find)
# ======================================
class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))
        self.rank = [0] * n

    def find(self, i: int) -> int:
        path = []
        while self.p[i] != i:
            path.append(i)
            i = self.p[i]
        for node in path:
            self.p[node] = i
        return i

    def union(self, i: int, j: int):
        ri, rj = self.find(i), self.find(j)
        if ri == rj:
            return
        if self.rank[ri] < self.rank[rj]:
            ri, rj = rj, ri
        self.p[rj] = ri
        if self.rank[ri] == self.rank[rj]:
            self.rank[ri] += 1

    def grupos(self, n: int) -> Dict[int, List[int]]:
        comp: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            comp[self.find(i)].append(i)
        return dict(comp)


# ======================================
# Agrupación de tono
# ======================================
def agrupar_textos_similares(textos: List[str], umbral: float) -> Dict[int, List[int]]:
    if not textos:
        return {}
    embs = get_embeddings_batch(textos)
    valid = [(i, e) for i, e in enumerate(embs) if e is not None]
    if len(valid) < 2:
        return {}
    idxs, M = zip(*valid)
    labels = AgglomerativeClustering(
        n_clusters=None, distance_threshold=1 - umbral, metric="cosine", linkage="average"
    ).fit(np.array(M)).labels_
    g: Dict[int, List[int]] = defaultdict(list)
    for k, lbl in enumerate(labels):
        g[lbl].append(idxs[k])
    return dict(enumerate(g.values()))


def agrupar_por_titulo_similar(titulos: List[str]) -> Dict[int, List[int]]:
    gid, grupos, used = 0, {}, set()
    norm = [normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(norm)):
        if i in used or not norm[i]:
            continue
        grp = [i]
        used.add(i)
        for j in range(i + 1, len(norm)):
            if j in used or not norm[j]:
                continue
            if SequenceMatcher(None, norm[i], norm[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                grp.append(j)
                used.add(j)
        if len(grp) >= 2:
            grupos[gid] = grp
            gid += 1
    return grupos


def seleccionar_representante(indices: List[int], textos: List[str]) -> Tuple[int, str]:
    embs = get_embeddings_batch([textos[i] for i in indices])
    validos = [(indices[k], e) for k, e in enumerate(embs) if e is not None]
    if not validos:
        return indices[0], textos[indices[0]]
    idxs, M = zip(*validos)
    centro = np.mean(M, axis=0, keepdims=True)
    best = int(np.argmax(cosine_similarity(np.array(M), centro)))
    return idxs[best], textos[idxs[best]]


# ======================================
# CLASIFICADOR DE TONO
# ======================================
class ClasificadorTono:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        names = [marca] + [a for a in self.aliases if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        self.brand_re = re.compile(
            r"\b(" + "|".join(patterns) + r")\b" if patterns else r"(a^b)",
            re.IGNORECASE
        )

    def _contextos(self, texto: str) -> List[str]:
        tl = unidecode(texto.lower())
        matches = list(self.brand_re.finditer(tl))
        if not matches:
            return [texto[:600]]
        out = []
        for i, m in enumerate(matches):
            win = 250 if i == 0 else 150
            s = max(0, m.start() - win)
            e = min(len(texto), m.end() + win)
            while e < len(texto) and texto[e] not in '.!?':
                e += 1
            out.append(texto[s:e + 1].strip())
        return list(dict.fromkeys(out))[:4]

    def _reglas(self, contextos: List[str]) -> Optional[str]:
        """
        MEJORA: Requiere más evidencia para clasificar por reglas.
        Antes bastaban 3 hits positivos; ahora se necesita margen más claro
        y se verifica que los hits estén en contexto de la marca.
        """
        pos, neg = 0, 0
        for ctx in contextos:
            t = unidecode(ctx.lower())
            # Verificar que la marca aparece en este contexto
            brand_in_ctx = bool(self.brand_re.search(t))

            neg_present = bool(re.search(
                r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente)\b.{0,30}',
                t, re.IGNORECASE
            ))
            ph = sum(1 for p in POS_PATTERNS if p.search(t))
            nh = sum(1 for p in NEG_PATTERNS if p.search(t))

            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(t):
                pos += 3
                continue
            if neg_present:
                neg += ph
            else:
                # Solo contar si la marca está en contexto
                weight = 1.0 if brand_in_ctx else 0.5
                pos += int(ph * weight)
                neg += int(nh * weight)

        # MEJORA: Umbrales más estrictos para evitar falsos positivos
        if pos >= 4 and pos > neg * 2:
            return "Positivo"
        if neg >= 4 and neg > pos * 2:
            return "Negativo"
        return None

    async def _llm(self, contextos: List[str]) -> Dict[str, str]:
        aliases_str = ", ".join(self.aliases) or "ninguno"
        # MEJORA: Prompt más específico con ejemplos de cada categoría
        prompt = (
            f"Analiza el sentimiento ESPECÍFICO hacia la marca '{self.marca}' "
            f"(alias: {aliases_str}) en los siguientes fragmentos de noticias.\n\n"
            f"CRITERIOS ESTRICTOS:\n"
            f"- Positivo: La noticia presenta a '{self.marca}' favorablemente: logros, lanzamientos, "
            f"premios, crecimiento, alianzas exitosas, responsabilidad social, innovación.\n"
            f"- Negativo: La noticia presenta a '{self.marca}' desfavorablemente: sanciones, multas, "
            f"demandas, fraudes, quejas, pérdidas, crisis, problemas operativos.\n"
            f"- Neutro: Mención informativa sin carga positiva ni negativa clara hacia '{self.marca}'. "
            f"Incluye noticias donde '{self.marca}' es mencionada pero no es protagonista del hecho.\n\n"
            f"FRAGMENTOS:\n---\n{chr(10).join(contextos[:3])}\n---\n\n"
            f'Responde SOLO JSON: {{"tono":"Positivo|Negativo|Neutro"}}'
        )
        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50, temperature=0.0,
                response_format={"type": "json_object"}
            )
            usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += (pt or 0)
                st.session_state['tokens_output'] += (ct or 0)
            tono = str(json.loads(resp.choices[0].message.content).get("tono", "Neutro")).title()
            return {"tono": tono if tono in ("Positivo", "Negativo", "Neutro") else "Neutro"}
        except:
            return {"tono": "Neutro"}

    async def _clasificar_async(self, texto: str, sem: asyncio.Semaphore):
        async with sem:
            ctx = self._contextos(texto)
            r = self._reglas(ctx)
            if r:
                return {"tono": r}
            return await self._llm(ctx)

    async def procesar_lote_async(self, textos: pd.Series, pbar,
                                   resumenes: pd.Series, titulos: pd.Series):
        n = len(textos)
        txts = textos.tolist()
        pbar.progress(0.05, "Agrupando para análisis de tono...")

        # MEJORA: Usar textos ponderados para los embeddings de agrupación de tono
        txts_emb = [
            texto_para_embedding(str(titulos.iloc[i]), str(resumenes.iloc[i]))
            for i in range(n)
        ]

        dsu = DSU(n)
        for g in [agrupar_textos_similares(txts_emb, SIMILARITY_THRESHOLD_TONO),
                  agrupar_por_titulo_similar(titulos.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]:
                    dsu.union(idxs[0], j)

        grupos = dsu.grupos(n)
        reps = {cid: seleccionar_representante(idxs, txts)[1] for cid, idxs in grupos.items()}
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [self._clasificar_async(rep, sem) for rep in reps.values()]
        resultados = []
        for i, f in enumerate(asyncio.as_completed(tasks)):
            resultados.append(await f)
            pbar.progress(0.1 + 0.85 * (i + 1) / len(tasks), f"Analizando tono {i + 1}/{len(tasks)}")
        res_por_grupo = {list(reps.keys())[i]: r for i, r in enumerate(resultados)}
        final = [None] * n
        for cid, idxs in grupos.items():
            r = res_por_grupo.get(cid, {"tono": "Neutro"})
            for i in idxs:
                final[i] = r
        pbar.progress(1.0, "Tono completado")
        return final


def analizar_tono_con_pkl(textos, pkl_file):
    try:
        pipeline = joblib.load(pkl_file)
        TONO_MAP = {1: "Positivo", "1": "Positivo", 0: "Neutro", "0": "Neutro",
                    -1: "Negativo", "-1": "Negativo"}
        return [{"tono": TONO_MAP.get(p, str(p).title())} for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error pkl sentimiento: {e}")
        return None


# ======================================
# CLASIFICADOR DE SUBTEMAS — flujo mejorado
# ======================================
# MEJORAS PRINCIPALES:
# 1. Embeddings ponderados (título 2x) para clustering más preciso
# 2. Clustering semántico sobre TODOS los ítems, no solo sueltos
# 3. Fusión inter-grupo iterativa hasta convergencia
# 4. Etiquetado con contexto de resúmenes (no solo títulos)
# 5. Validación post-etiquetado: si una etiqueta es demasiado genérica, se refina
# 6. Deduplicación de etiquetas por similitud de cadena

class ClasificadorSubtema:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self._cache: Dict[str, str] = {}

    # ── Paso 1: hash exacto ───────────────────────────────────────────────────
    def _paso1_hash_exacto(self, titulos: List[str], resumenes: List[str], dsu: DSU):
        """Noticias con título o inicio de resumen idéntico → mismo grupo."""

        def norm(t: str) -> str:
            t = unidecode(str(t).lower())
            return re.sub(r'[^a-z0-9\s]', '', t).split()

        def tok_40(t):
            return ' '.join(norm(t)[:40])

        def tok_15(t):
            return ' '.join(norm(t)[:15])

        bkt_tit: Dict[str, List[int]] = defaultdict(list)
        bkt_res: Dict[str, List[int]] = defaultdict(list)
        for i, (tit, res) in enumerate(zip(titulos, resumenes)):
            nt, nr = tok_40(tit), tok_15(res)
            if nt:
                bkt_tit[hashlib.md5(nt.encode()).hexdigest()].append(i)
            if nr:
                bkt_res[hashlib.md5(nr.encode()).hexdigest()].append(i)
        for bkt in (bkt_tit, bkt_res):
            for idxs in bkt.values():
                for j in idxs[1:]:
                    dsu.union(idxs[0], j)

    # ── Paso 2: similitud de títulos (SequenceMatcher) ───────────────────────
    def _paso2_titulos_similares(self, titulos: List[str], dsu: DSU):
        norm = [normalize_title_for_comparison(t) for t in titulos]
        n = len(norm)
        for i in range(n):
            if not norm[i]:
                continue
            for j in range(i + 1, n):
                if not norm[j] or dsu.find(i) == dsu.find(j):
                    continue
                if SequenceMatcher(None, norm[i], norm[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    dsu.union(i, j)

    # ── Paso 3: clustering semántico completo ────────────────────────────────
    def _paso3_semantico_completo(self, emb_textos: List[str], dsu: DSU, pbar, p_start: float):
        """
        MEJORA PRINCIPAL: Clustering sobre TODOS los ítems, no solo sueltos.
        Antes los ítems ya agrupados en pasos 1-2 nunca se evaluaban
        semánticamente contra otros grupos, creando fragmentación.
        
        Ahora:
        1. Se obtienen embeddings de todos
        2. Se hace clustering global
        3. Se unen al DSU existente (no reemplaza, AGREGA uniones)
        """
        n = len(emb_textos)
        if n < 2:
            return

        BATCH = 500
        total_batches = max(1, (n + BATCH - 1) // BATCH)

        # Para datasets pequeños, hacer todo de una vez
        if n <= BATCH:
            pbar.progress(p_start, "Clustering semántico global...")
            embs = get_embeddings_batch(emb_textos)
            ok = [(k, e) for k, e in enumerate(embs) if e is not None]
            if len(ok) < 2:
                return
            idxs_ok, M = zip(*ok)
            sim = cosine_similarity(np.array(M))
            labels = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=1 - UMBRAL_SUBTEMA,
                metric='precomputed',
                linkage='average'
            ).fit(1 - sim).labels_
            g: Dict[int, List[int]] = defaultdict(list)
            for k, lbl in enumerate(labels):
                g[lbl].append(idxs_ok[k])
            for cluster in g.values():
                if len(cluster) >= 2:
                    for j in cluster[1:]:
                        dsu.union(cluster[0], j)
            pbar.progress(p_start + 0.18, "Clustering global completado")
            return

        # Para datasets grandes: batch + centroide cross-batch
        all_embs = get_embeddings_batch(emb_textos)

        # Intra-batch clustering
        for b_num, b_start in enumerate(range(0, n, BATCH)):
            batch_idxs = list(range(b_start, min(b_start + BATCH, n)))
            ok = [(idx, all_embs[idx]) for idx in batch_idxs if all_embs[idx] is not None]
            if len(ok) < 2:
                continue
            idxs_ok, M = zip(*ok)
            sim = cosine_similarity(np.array(M))
            labels = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=1 - UMBRAL_SUBTEMA,
                metric='precomputed',
                linkage='average'
            ).fit(1 - sim).labels_
            g: Dict[int, List[int]] = defaultdict(list)
            for k, lbl in enumerate(labels):
                g[lbl].append(idxs_ok[k])
            for cluster in g.values():
                if len(cluster) >= 2:
                    for j in cluster[1:]:
                        dsu.union(cluster[0], j)
            pbar.progress(
                p_start + 0.15 * (b_num + 1) / total_batches,
                f"Clustering lote {b_num + 1}/{total_batches}..."
            )

        # Cross-batch: comparar centroides de grupos de diferentes batches
        pbar.progress(p_start + 0.16, "Unificando lotes...")
        self._fusion_intergrupo_iterativa(emb_textos, all_embs, dsu, pbar, p_start + 0.16)

    # ── Paso 3b: fusión inter-grupo iterativa ────────────────────────────────
    def _fusion_intergrupo_iterativa(self, textos: List[str],
                                      all_embs: List[Optional[List[float]]],
                                      dsu: DSU, pbar, p_start: float):
        """
        MEJORA: Itera hasta convergencia en lugar de una sola pasada.
        Antes se comparaban centroides una vez, pero si A se fusionaba con B,
        y B era similar a C, C no se fusionaba. Ahora itera.
        """
        n = len(textos)

        for iteration in range(MAX_ITER_FUSION):
            grupos = dsu.grupos(n)
            if len(grupos) < 2:
                break

            grupo_ids = list(grupos.keys())
            centroids = []
            valid_gids = []

            for gid in grupo_ids:
                idxs = grupos[gid]
                vecs = [all_embs[i] for i in idxs[:30] if all_embs[i] is not None]
                if vecs:
                    centroids.append(np.mean(vecs, axis=0))
                    valid_gids.append(gid)

            if len(valid_gids) < 2:
                break

            sim = cosine_similarity(np.array(centroids))
            fusiones = 0

            for i in range(len(valid_gids)):
                for j in range(i + 1, len(valid_gids)):
                    if sim[i][j] >= UMBRAL_FUSION_INTERGRUPO:
                        gid_i = valid_gids[i]
                        gid_j = valid_gids[j]
                        rep_i = grupos[gid_i][0]
                        rep_j = grupos[gid_j][0]
                        if dsu.find(rep_i) != dsu.find(rep_j):
                            dsu.union(rep_i, rep_j)
                            fusiones += 1

            pbar.progress(
                min(p_start + 0.04 * (iteration + 1), 0.52),
                f"Fusión iter {iteration + 1}: {fusiones} fusiones"
            )

            if fusiones == 0:
                break  # Convergencia alcanzada

    # ── Paso 4: generar etiqueta para un grupo ────────────────────────────────
    def _generar_etiqueta(self, textos_grp: List[str], titulos_grp: List[str],
                           resumenes_grp: List[str]) -> str:
        """
        MEJORA: Incluye fragmentos de resúmenes en el prompt para dar más
        contexto al LLM. Antes solo veía títulos, que pueden ser ambiguos.
        """
        # Clave de caché robusta
        titulos_norm = sorted(set(normalize_title_for_comparison(t) for t in titulos_grp if t))
        cache_key = hashlib.md5("|".join(titulos_norm[:12]).encode()).hexdigest()
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Extraer keywords
        palabras = []
        for t in titulos_grp[:8]:
            palabras.extend(w for w in string_norm_label(t).split() if len(w) > 3)
        keywords = " · ".join(w for w, _ in Counter(palabras).most_common(8))

        titulos_muestra = list(dict.fromkeys(t[:120] for t in titulos_grp if t))[:6]

        # MEJORA: Incluir fragmentos de resúmenes para contexto
        resumenes_muestra = []
        for r in resumenes_grp[:3]:
            if r and len(str(r)) > 20:
                resumenes_muestra.append(str(r)[:200])

        contexto_resumenes = ""
        if resumenes_muestra:
            contexto_resumenes = (
                "\n\nFRAGMENTOS DE RESÚMENES:\n" +
                "\n".join(f"  · {r}" for r in resumenes_muestra)
            )

        prompt = (
            "Genera un SUBTEMA periodístico en español (3-5 palabras) que describa con precisión "
            "el asunto central y específico de estas noticias.\n\n"
            f"TÍTULOS:\n" + "\n".join(f"  · {t}" for t in titulos_muestra) +
            contexto_resumenes + "\n\n"
            f"PALABRAS CLAVE: {keywords}\n\n"
            "REGLAS OBLIGATORIAS:\n"
            "  1. NO uses el nombre de empresas, marcas, personas ni ciudades\n"
            "  2. NO uses palabras genéricas solas como 'Gestión', 'Actividades', 'Acciones', "
            "'Noticias', 'Información', 'Eventos', 'Varios'\n"
            "  3. El subtema debe describir EL ASUNTO ESPECÍFICO, no el actor\n"
            "  4. Debe ser suficientemente específico para distinguir estas noticias de otras\n"
            "  5. Usa sustantivos y adjetivos descriptivos\n"
            "  6. Ejemplos CORRECTOS: 'Resultados Financieros Trimestrales', "
            "'Programa Becas Universitarias', 'Expansión Red Sucursales', "
            "'Sanción Regulatoria Financiera', 'Transformación Digital Servicios'\n"
            "  7. Ejemplos INCORRECTOS: 'Gestión Empresarial', 'Noticias Corporativas', "
            "'Actividades Varias', 'Eventos Recientes'\n\n"
            'Responde SOLO JSON: {"subtema":"..."}'
        )
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40, temperature=0.0,
                response_format={"type": "json_object"}
            )
            usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += (pt or 0)
                st.session_state['tokens_output'] += (ct or 0)
            raw = json.loads(resp.choices[0].message.content).get("subtema", "Varios")
            etiqueta = limpiar_tema_geografico(limpiar_tema(raw), self.marca, self.aliases)

            # MEJORA: Validar que no sea genérica
            genericas = {"gestión", "gestion", "actividades", "acciones", "noticias",
                         "información", "informacion", "eventos", "varios", "sin tema",
                         "actividad corporativa", "noticias corporativas", "gestión empresarial",
                         "gestion empresarial"}
            if string_norm_label(etiqueta) in genericas or len(etiqueta.split()) < 2:
                # Reintentar con prompt más restrictivo
                etiqueta = self._refinar_etiqueta(titulos_muestra, keywords)

        except:
            etiqueta = self._etiqueta_fallback(titulos_grp)

        self._cache[cache_key] = etiqueta
        return etiqueta

    def _refinar_etiqueta(self, titulos: List[str], keywords: str) -> str:
        """Segundo intento con prompt más específico si la primera etiqueta fue genérica."""
        prompt = (
            "Los siguientes títulos de noticias comparten un tema en común. "
            "Identifica el tema ESPECÍFICO en 3-5 palabras.\n\n"
            f"Títulos: {' | '.join(titulos[:4])}\n"
            f"Keywords: {keywords}\n\n"
            "NO uses palabras genéricas. Sé específico sobre QUÉ PASÓ o DE QUÉ SE HABLA.\n"
            'JSON: {"subtema":"..."}'
        )
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40, temperature=0.1,
                response_format={"type": "json_object"}
            )
            usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += (pt or 0)
                st.session_state['tokens_output'] += (ct or 0)
            raw = json.loads(resp.choices[0].message.content).get("subtema", "Varios")
            return limpiar_tema_geografico(limpiar_tema(raw), self.marca, self.aliases)
        except:
            return self._etiqueta_fallback([])

    def _etiqueta_fallback(self, titulos: List[str]) -> str:
        """Generar etiqueta de fallback basada en keywords de los títulos."""
        if not titulos:
            return "Cobertura Informativa"
        palabras = []
        for t in titulos[:5]:
            for w in string_norm_label(t).split():
                if len(w) > 4:
                    palabras.append(w)
        if palabras:
            top = [w for w, _ in Counter(palabras).most_common(3)]
            return " ".join(w.capitalize() for w in top[:3])
        return "Cobertura Informativa"

    # ── Método principal ──────────────────────────────────────────────────────
    def procesar_lote(self, col_resumen: pd.Series, pbar,
                      resumenes_puros: pd.Series, titulos_puros: pd.Series) -> List[str]:
        textos = col_resumen.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumenes_puros.tolist()
        n = len(textos)

        # MEJORA: Generar textos ponderados para embedding UNA sola vez
        emb_textos = [
            texto_para_embedding(titulos[i], resumenes[i])
            for i in range(n)
        ]

        # ── Fase 1: agrupación determinista ───────────────────────────────────
        pbar.progress(0.05, "Fase 1 · Agrupando noticias idénticas...")
        dsu = DSU(n)
        self._paso1_hash_exacto(titulos, resumenes, dsu)

        pbar.progress(0.12, "Fase 2 · Similitud de títulos...")
        self._paso2_titulos_similares(titulos, dsu)

        # ── Fase 3: clustering semántico COMPLETO ────────────────────────────
        pbar.progress(0.20, "Fase 3 · Clustering semántico global...")
        self._paso3_semantico_completo(emb_textos, dsu, pbar, p_start=0.20)

        # Grupos finales (post-fusión iterativa)
        grupos_finales = dsu.grupos(n)
        n_grupos = len(grupos_finales)

        # ── Fase 4: generar UNA etiqueta por grupo y propagar ─────────────────
        pbar.progress(0.55, f"Fase 4 · Etiquetando {n_grupos} grupos...")
        mapa: Dict[int, str] = {}
        sorted_groups = sorted(grupos_finales.items(), key=lambda x: -len(x[1]))

        for k, (lid, idxs) in enumerate(sorted_groups):
            if k % 10 == 0:
                pbar.progress(
                    0.55 + 0.30 * (k / max(n_grupos, 1)),
                    f"Etiquetando grupo {k + 1}/{n_grupos} ({len(idxs)} noticias)..."
                )
            etiqueta = self._generar_etiqueta(
                [textos[i] for i in idxs],
                [titulos[i] for i in idxs],
                [resumenes[i] for i in idxs]
            )
            for i in idxs:
                mapa[i] = etiqueta

        subtemas = [mapa.get(i, "Varios") for i in range(n)]

        # ── Fase 5: deduplicación de etiquetas similares ────────────────────
        pbar.progress(0.88, "Fase 5 · Deduplicando etiquetas similares...")
        subtemas = dedup_labels(subtemas, UMBRAL_DEDUP_LABEL)

        # ── Fase 6: verificación de consistencia ─────────────────────────────
        # MEJORA: Si dos grupos tienen la misma etiqueta, verificar que sean
        # semánticamente similares. Si no, diferenciar.
        pbar.progress(0.93, "Fase 6 · Verificando consistencia...")
        subtemas = self._verificar_consistencia(subtemas, emb_textos, pbar)

        n_final = len(set(subtemas))
        pbar.progress(1.0, f"Completado: {n_final} subtemas en {n_grupos} grupos")
        st.info(f"Subtemas únicos: **{n_final}** · Grupos semánticos: **{n_grupos}**")
        return subtemas

    def _verificar_consistencia(self, subtemas: List[str], emb_textos: List[str],
                                 pbar) -> List[str]:
        """
        NUEVA FUNCIÓN: Verifica que noticias con la misma etiqueta sean
        realmente similares. Si un subtema agrupa noticias muy diferentes
        (por dedup agresivo), las separa.
        """
        from collections import defaultdict

        # Agrupar por subtema
        por_subtema: Dict[str, List[int]] = defaultdict(list)
        for i, sub in enumerate(subtemas):
            por_subtema[sub].append(i)

        all_embs = get_embeddings_batch(emb_textos)
        resultado = list(subtemas)

        for sub, idxs in por_subtema.items():
            if len(idxs) < 3 or sub in ("Sin tema", "Varios"):
                continue

            # Calcular similitud intra-grupo
            vecs = [all_embs[i] for i in idxs if all_embs[i] is not None]
            if len(vecs) < 3:
                continue

            M = np.array(vecs)
            sim = cosine_similarity(M)
            # Media de similitud (excluyendo diagonal)
            n_v = len(vecs)
            if n_v > 1:
                mask = ~np.eye(n_v, dtype=bool)
                avg_sim = sim[mask].mean()

                # Si la similitud media es muy baja, el grupo es inconsistente
                if avg_sim < 0.65:
                    # Re-clusterizar este grupo
                    labels = AgglomerativeClustering(
                        n_clusters=None,
                        distance_threshold=1 - UMBRAL_SUBTEMA,
                        metric='precomputed',
                        linkage='average'
                    ).fit(1 - sim).labels_

                    if len(set(labels)) > 1:
                        # Hay sub-clusters: añadir sufijo diferenciador
                        for k, lbl in enumerate(labels):
                            valid_idx = [i for i in idxs if all_embs[i] is not None][k]
                            if lbl > 0:  # Solo renombrar los sub-clusters secundarios
                                resultado[valid_idx] = f"{sub} ({lbl + 1})"

        return resultado


# ======================================
# CONSOLIDACIÓN DE TEMAS
# ======================================
def consolidar_temas(subtemas: List[str], textos: List[str], pbar) -> List[str]:
    """
    MEJORA: Usa embeddings de las ETIQUETAS de subtema (no de los textos)
    para clustering de temas. Esto es más preciso porque compara la semántica
    de los subtemas entre sí, no de textos individuales que pueden variar.
    """
    pbar.progress(0.05, "Calculando centroides de subtemas...")
    df = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    unique_subs = list(df['subtema'].unique())

    if len(unique_subs) <= 1:
        pbar.progress(1.0, "Un solo tema")
        return subtemas

    # MEJORA: Combinar embedding de la etiqueta + centroide de textos
    # para una representación más robusta
    all_embs = get_embeddings_batch(textos)

    # Centroide semántico por subtema
    centroids: Dict[str, np.ndarray] = {}
    for sub in unique_subs:
        idxs = df.index[df['subtema'] == sub].tolist()[:40]
        vecs = [all_embs[i] for i in idxs if all_embs[i] is not None]
        if vecs:
            centroids[sub] = np.mean(vecs, axis=0)

    # También embeddear las etiquetas de subtema para comparar por nombre
    label_embs_raw = get_embeddings_batch(unique_subs)
    label_embs = {
        sub: emb for sub, emb in zip(unique_subs, label_embs_raw)
        if emb is not None
    }

    valid_subs = [s for s in unique_subs if s in centroids]
    if len(valid_subs) < 2:
        pbar.progress(1.0, "Sin agrupación posible")
        return subtemas

    pbar.progress(0.40, "Clustering de subtemas en temas...")

    # MEJORA: Combinar similitud de contenido (centroide) y similitud de etiqueta
    M_content = np.array([centroids[s] for s in valid_subs])
    sim_content = cosine_similarity(M_content)

    # Si tenemos embeddings de etiquetas, hacer media ponderada
    has_label_embs = all(s in label_embs for s in valid_subs)
    if has_label_embs:
        M_labels = np.array([label_embs[s] for s in valid_subs])
        sim_labels = cosine_similarity(M_labels)
        # Peso: 60% contenido, 40% etiqueta
        sim = 0.6 * sim_content + 0.4 * sim_labels
    else:
        sim = sim_content

    # Clustering con distance_threshold
    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=1 - UMBRAL_TEMA,
        metric='precomputed',
        linkage='average'
    ).fit(1 - sim)

    n_temas_obtenidos = len(set(clustering.labels_))

    # Si excede el máximo, re-clusterizar con tope
    if n_temas_obtenidos > NUM_TEMAS_MAX:
        clustering = AgglomerativeClustering(
            n_clusters=NUM_TEMAS_MAX,
            metric='precomputed',
            linkage='average'
        ).fit(1 - sim)

    clusters_subs: Dict[int, List[str]] = defaultdict(list)
    for i, lbl in enumerate(clustering.labels_):
        clusters_subs[lbl].append(valid_subs[i])

    # Para subtemas que no se pudieron clusterizar
    unclustered = [s for s in unique_subs if s not in valid_subs]

    mapa_tema: Dict[str, str] = {}
    total_clusters = len(clusters_subs)

    for k, (cid, lista_subs) in enumerate(clusters_subs.items()):
        pbar.progress(0.50 + 0.40 * (k / max(total_clusters, 1)),
                      f"Generando tema {k + 1}/{total_clusters}...")

        # Si solo hay un subtema en el cluster, el tema puede derivarse directamente
        if len(lista_subs) == 1:
            # Generalizar ligeramente el subtema
            nombre = self._generalizar_subtema(lista_subs[0])
        else:
            titulos_muestra = lista_subs[:10]
            prompt = (
                "Genera UNA categoría temática general en español (2-4 palabras) que agrupe "
                "coherentemente estos subtemas periodísticos bajo un mismo paraguas.\n\n"
                f"SUBTEMAS A AGRUPAR:\n" +
                "\n".join(f"  · {s}" for s in titulos_muestra) + "\n\n"
                "REGLAS:\n"
                "  - Sin nombres de empresas, marcas ni ciudades\n"
                "  - Sin verbos ni artículos iniciales\n"
                "  - Usa sustantivos descriptivos: 'Resultados Financieros', 'Sostenibilidad', "
                "'Innovación Tecnológica', 'Responsabilidad Social', 'Regulación Financiera'\n"
                "  - La categoría debe ser más general que los subtemas pero NO tan vaga "
                "como 'Noticias' o 'Información'\n"
                "  - Responde SOLO el nombre del tema, sin comillas ni explicaciones"
            )
            try:
                resp = call_with_retries(
                    openai.ChatCompletion.create,
                    model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=15, temperature=0.0
                )
                usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
                if usage:
                    pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage,
                                                                                             'prompt_tokens', 0)
                    ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage,
                                                                                                 'completion_tokens',
                                                                                                 0)
                    st.session_state['tokens_input'] += (pt or 0)
                    st.session_state['tokens_output'] += (ct or 0)
                nombre = limpiar_tema(resp.choices[0].message.content.strip().replace('"', '').replace('.', ''))
            except:
                nombre = lista_subs[0]

        for sub in lista_subs:
            mapa_tema[sub] = nombre

    # Asignar tema a subtemas no clusterizados
    for sub in unclustered:
        mapa_tema[sub] = sub

    temas_final = [mapa_tema.get(sub, sub) for sub in subtemas]

    # Deduplicar etiquetas de temas
    pbar.progress(0.92, "Deduplicando etiquetas de temas...")
    temas_final = dedup_labels(temas_final, UMBRAL_DEDUP_LABEL)

    n_temas = len(set(temas_final))
    st.info(f"Temas consolidados: **{n_temas}** (máximo configurado: {NUM_TEMAS_MAX})")
    pbar.progress(1.0, "Temas finalizados")
    return temas_final


def _generalizar_subtema(subtema: str) -> str:
    """Generaliza un subtema individual a nivel de tema."""
    # Quitar adjetivos específicos, mantener sustantivos
    palabras = subtema.split()
    if len(palabras) <= 2:
        return subtema
    # Tomar las primeras 2-3 palabras como tema general
    return " ".join(palabras[:min(3, len(palabras))])


# Hacer accesible como función de módulo para consolidar_temas
consolidar_temas.__globals__['_generalizar_subtema'] = _generalizar_subtema


def analizar_temas_con_pkl(textos, pkl_file):
    try:
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error pkl temas: {e}")
        return None


# ======================================
# Duplicados y Excel
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed = deepcopy(rows)
    seen_url, seen_bcast = {}, {}
    title_buckets: Dict[tuple, List[int]] = defaultdict(list)
    for i, row in enumerate(processed):
        if row.get("is_duplicate"):
            continue
        tipo = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio", ""))))
        mencion = norm_key(row.get(key_map.get("menciones", "")))
        medio = norm_key(row.get(key_map.get("medio", "")))
        if tipo == "Internet":
            li = row.get(key_map.get("link_nota", {})) or {}
            url = li.get("url") if isinstance(li, dict) else None
            if url and mencion:
                k = (url, mencion)
                if k in seen_url:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed[seen_url[k]].get(key_map.get("idnoticia", ""), "")
                    continue
                seen_url[k] = i
            if medio and mencion:
                title_buckets[(medio, mencion)].append(i)
        elif tipo in ("Radio", "Televisión"):
            hora = str(row.get(key_map.get("hora", ""), "")).strip()
            if mencion and medio and hora:
                k = (mencion, medio, hora)
                if k in seen_bcast:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed[seen_bcast[k]].get(key_map.get("idnoticia", ""), "")
                else:
                    seen_bcast[k] = i
    for idxs in title_buckets.values():
        if len(idxs) < 2:
            continue
        for i in range(len(idxs)):
            for j in range(i + 1, len(idxs)):
                a, b = idxs[i], idxs[j]
                if processed[a].get("is_duplicate") or processed[b].get("is_duplicate"):
                    continue
                ta = normalize_title_for_comparison(processed[a].get(key_map.get("titulo", "")))
                tb = normalize_title_for_comparison(processed[b].get(key_map.get("titulo", "")))
                if ta and tb and SequenceMatcher(None, ta, tb).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    if len(ta) < len(tb):
                        processed[a]["is_duplicate"] = True
                        processed[a]["idduplicada"] = processed[b].get(key_map.get("idnoticia", ""), "")
                    else:
                        processed[b]["is_duplicate"] = True
                        processed[b]["idduplicada"] = processed[a].get(key_map.get("idnoticia", ""), "")
    return processed


def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({
        "titulo": norm_key("Titulo"),
        "resumen": norm_key("Resumen - Aclaracion"),
        "menciones": norm_key("Menciones - Empresa"),
        "medio": norm_key("Medio"),
        "tonoiai": norm_key("Tono IA"),
        "tema": norm_key("Tema"),
        "subtema": norm_key("Subtema"),
        "idnoticia": norm_key("ID Noticia"),
        "idduplicada": norm_key("ID duplicada"),
        "tipodemedio": norm_key("Tipo de Medio"),
        "hora": norm_key("Hora"),
        "link_nota": norm_key("Link Nota"),
        "link_streaming": norm_key("Link (Streaming - Imagen)"),
        "region": norm_key("Region"),
    })
    rows, split_rows = [], []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row):
            continue
        rows.append({norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)})
    for r_cells in rows:
        base = {
            k: (extract_link(v) if k in (key_map["link_nota"], key_map["link_streaming"]) else v.value)
            for k, v in r_cells.items()
        }
        if key_map.get("tipodemedio") in base:
            base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        ml = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in ml or [None]:
            nr = deepcopy(base)
            if m:
                nr[key_map["menciones"]] = m
            split_rows.append(nr)
    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})
    processed = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed:
        if row["is_duplicate"]:
            row.update({
                key_map["tonoiai"]: "Duplicada",
                key_map["tema"]: "Duplicada",
                key_map["subtema"]: "Duplicada"
            })
    return processed, key_map


def fix_links_by_media_type(row, key_map):
    tkey = key_map.get("tipodemedio")
    ln_key = key_map.get("link_nota")
    ls_key = key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key):
        return
    tipo = row.get(tkey, "")
    ln = row.get(ln_key) or {"value": "", "url": None}
    ls = row.get(ls_key) or {"value": "", "url": None}
    hurl = lambda x: isinstance(x, dict) and bool(x.get("url"))
    if tipo in ("Radio", "Televisión"):
        row[ls_key] = {"value": "", "url": None}
    elif tipo == "Internet":
        row[ln_key], row[ls_key] = ls, ln
    elif tipo in ("Prensa", "Revista"):
        if not hurl(ln) and hurl(ls):
            row[ln_key] = ls
        row[ls_key] = {"value": "", "url": None}


def generate_output_excel(rows, key_map):
    wb = Workbook()
    ws = wb.active
    ws.title = "Resultado"
    ORDER = ["ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Region",
             "Seccion - Programa",
             "Titulo", "Autor - Conductor", "Nro. Pagina", "Dimension",
             "Duracion - Nro. Caracteres",
             "CPE", "Audiencia", "Tier", "Tono", "Tono IA", "Tema", "Subtema", "Link Nota",
             "Resumen - Aclaracion", "Link (Streaming - Imagen)", "Menciones - Empresa",
             "ID duplicada"]
    NUM = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier",
           "Audiencia"}
    ws.append(ORDER)
    ls = NamedStyle(name="HL", font=Font(color="0000FF", underline="single"))
    if "HL" not in wb.style_names:
        wb.add_named_style(ls)
    for row in rows:
        tk = key_map.get("titulo")
        if tk and tk in row:
            row[tk] = clean_title_for_output(row.get(tk))
        rk = key_map.get("resumen")
        if rk and rk in row:
            row[rk] = corregir_texto(row.get(rk))
        out, links = [], {}
        for ci, h in enumerate(ORDER, 1):
            dk = key_map.get(norm_key(h), norm_key(h))
            val = row.get(dk)
            cv = None
            if h in NUM:
                try:
                    cv = float(val) if val is not None and str(val).strip() != "" else None
                except:
                    cv = str(val) if val is not None else None
            elif isinstance(val, dict) and "url" in val:
                cv = val.get("value", "Link")
                if val.get("url"):
                    links[ci] = val["url"]
            elif val is not None:
                cv = str(val)
            out.append(cv)
        ws.append(out)
        for ci, url in links.items():
            cell = ws.cell(row=ws.max_row, column=ci)
            cell.hyperlink = url
            cell.style = "HL"
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ======================================
# Proceso principal
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name,
                                  brand_aliases, tono_pkl_file, tema_pkl_file, analysis_mode):
    st.session_state.update({'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0})
    # Limpiar caché de embeddings para nuevo procesamiento
    get_embedding_cache().clear()
    t0 = time.time()

    if "API" in analysis_mode:
        try:
            openai.api_key = st.secrets["OPENAI_API_KEY"]
            openai.aiosession.set(None)
        except:
            st.error("OPENAI_API_KEY no encontrado.")
            st.stop()

    with st.status("Paso 1 · Limpieza y duplicados", expanded=True) as s:
        rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="Paso 1 completado · Limpieza", state="complete")

    with st.status("Paso 2 · Mapeos", expanded=True) as s:
        df_r = pd.read_excel(region_file)
        rmap = {str(k).lower().strip(): v for k, v in
                pd.Series(df_r.iloc[:, 1].values, index=df_r.iloc[:, 0]).to_dict().items()}
        df_i = pd.read_excel(internet_file)
        imap = {str(k).lower().strip(): v for k, v in
                pd.Series(df_i.iloc[:, 1].values, index=df_i.iloc[:, 0]).to_dict().items()}
        for row in rows:
            mk = str(row.get(key_map.get("medio", ""), "")).lower().strip()
            row[key_map.get("region")] = rmap.get(mk, "N/A")
            if mk in imap:
                row[key_map.get("medio")] = imap[mk]
                row[key_map.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, key_map)
        s.update(label="Paso 2 completado · Mapeos aplicados", state="complete")

    gc.collect()
    to_analyze = [r for r in rows if not r.get("is_duplicate")]

    if to_analyze:
        df = pd.DataFrame(to_analyze)

        # MEJORA: Usar texto ponderado con título repetido
        df["_txt"] = df.apply(
            lambda r: texto_para_embedding(
                str(r.get(key_map["titulo"], "")),
                str(r.get(key_map["resumen"], ""))
            ),
            axis=1
        )

        # MEJORA: Pre-calcular embeddings para reutilización
        with st.status("Pre-calculando embeddings...", expanded=True) as s:
            _ = get_embeddings_batch(df["_txt"].tolist())
            cache_stats = get_embedding_cache().stats()
            s.update(label=f"Embeddings calculados · {cache_stats}", state="complete")

        with st.status("Paso 3 · Tono", expanded=True) as s:
            pb = st.progress(0)
            if "PKL" in analysis_mode and tono_pkl_file:
                res = analizar_tono_con_pkl(df["_txt"].tolist(), tono_pkl_file)
                if res is None:
                    st.stop()
            elif "API" in analysis_mode:
                res = await ClasificadorTono(brand_name, brand_aliases).procesar_lote_async(
                    df["_txt"], pb, df[key_map["resumen"]], df[key_map["titulo"]]
                )
            else:
                res = [{"tono": "N/A"}] * len(to_analyze)
            df[key_map["tonoiai"]] = [r["tono"] for r in res]
            s.update(label="Paso 3 completado · Tono analizado", state="complete")

        with st.status("Paso 4 · Tema y Subtema", expanded=True) as s:
            pb = st.progress(0)
            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A"] * len(to_analyze)
                temas = ["N/A"] * len(to_analyze)
            else:
                subtemas = ClasificadorSubtema(brand_name, brand_aliases).procesar_lote(
                    df["_txt"], pb, df[key_map["resumen"]], df[key_map["titulo"]]
                )
                temas = consolidar_temas(subtemas, df["_txt"].tolist(), pb)

            df[key_map["subtema"]] = subtemas
            if "PKL" in analysis_mode and tema_pkl_file:
                tp = analizar_temas_con_pkl(df["_txt"].tolist(), tema_pkl_file)
                if tp:
                    df[key_map["tema"]] = tp
            else:
                df[key_map["tema"]] = temas
            s.update(label="Paso 4 completado · Clasificación", state="complete")

        rmap2 = df.set_index("original_index").to_dict("index")
        for row in rows:
            if not row.get("is_duplicate"):
                row.update(rmap2.get(row["original_index"], {}))

    gc.collect()
    ci = (st.session_state['tokens_input'] / 1e6) * PRICE_INPUT_1M
    co = (st.session_state['tokens_output'] / 1e6) * PRICE_OUTPUT_1M
    cem = (st.session_state['tokens_embedding'] / 1e6) * PRICE_EMBEDDING_1M

    # Log cache stats
    cache_stats = get_embedding_cache().stats()

    with st.status("Paso 5 · Generando informe", expanded=True) as s:
        st.session_state["output_data"] = generate_output_excel(rows, key_map)
        st.session_state["output_filename"] = (
            f"Informe_IA_{brand_name.replace(' ', '_')}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name": brand_name,
            "brand_aliases": brand_aliases,
            "total_rows": len(rows),
            "unique_rows": len(to_analyze),
            "duplicates": len(rows) - len(to_analyze),
            "process_duration": f"{time.time() - t0:.0f}s",
            "process_cost": f"${ci + co + cem:.4f} USD",
            "cache_stats": cache_stats,
        })
        s.update(label=f"Proceso completado · {cache_stats}", state="complete")


# ======================================
# Análisis Rápido
# ======================================
async def run_quick_analysis_async(df, title_col, summary_col, brand_name, aliases):
    st.session_state.update({'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0})
    get_embedding_cache().clear()

    # MEJORA: Texto ponderado
    df['_txt'] = df.apply(
        lambda r: texto_para_embedding(
            str(r.get(title_col, "")),
            str(r.get(summary_col, ""))
        ),
        axis=1
    )

    # Pre-calcular embeddings
    with st.status("Pre-calculando embeddings...", expanded=True) as s:
        _ = get_embeddings_batch(df['_txt'].tolist())
        s.update(label=f"Embeddings listos · {get_embedding_cache().stats()}", state="complete")

    with st.status("Paso 1/2 · Tono...", expanded=True) as s:
        pb = st.progress(0)
        res = await ClasificadorTono(brand_name, aliases).procesar_lote_async(
            df["_txt"], pb, df[summary_col].fillna(''), df[title_col].fillna('')
        )
        df['Tono IA'] = [r["tono"] for r in res]
        s.update(label="Paso 1/2 completado · Tono analizado", state="complete")
    with st.status("Paso 2/2 · Tema y Subtema...", expanded=True) as s:
        pb = st.progress(0)
        subtemas = ClasificadorSubtema(brand_name, aliases).procesar_lote(
            df["_txt"], pb, df[summary_col].fillna(''), df[title_col].fillna('')
        )
        df['Subtema'] = subtemas
        temas = consolidar_temas(subtemas, df["_txt"].tolist(), pb)
        df['Tema'] = temas
        s.update(label="Paso 2/2 completado · Clasificación", state="complete")
    df.drop(columns=['_txt'], inplace=True)
    ci = (st.session_state['tokens_input'] / 1e6) * PRICE_INPUT_1M
    co = (st.session_state['tokens_output'] / 1e6) * PRICE_OUTPUT_1M
    cem = (st.session_state['tokens_embedding'] / 1e6) * PRICE_EMBEDDING_1M
    st.session_state['quick_cost'] = f"${ci + co + cem:.4f} USD"
    return df


def gen_quick_excel(df) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Analisis')
    return buf.getvalue()


def render_quick_tab():
    st.markdown('<div class="sec-label">Análisis rápido</div>', unsafe_allow_html=True)
    if 'quick_result' in st.session_state:
        st.markdown(
            '<div class="success-banner"><div class="success-title">Análisis completado</div>'
            '<div class="success-sub">Los resultados están listos para descargar</div></div>',
            unsafe_allow_html=True
        )
        st.metric("Costo estimado", st.session_state.get('quick_cost', "$0.00"))
        st.dataframe(st.session_state.quick_result.head(10), use_container_width=True)
        st.download_button(
            "Descargar resultados",
            data=gen_quick_excel(st.session_state.quick_result),
            file_name="Analisis_Rapido_IA.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary"
        )
        if st.button("Nuevo análisis"):
            for k in ('quick_result', 'quick_df', 'quick_name', 'quick_cost'):
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()
        return
    if 'quick_df' not in st.session_state:
        st.markdown("Sube un archivo Excel con columnas de título y resumen.")
        f = st.file_uploader("Archivo Excel", type=["xlsx"], label_visibility="collapsed", key="qu")
        if f:
            try:
                st.session_state.quick_df = pd.read_excel(f)
                st.session_state.quick_name = f.name
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
                st.stop()
    else:
        st.success(f"Archivo **{st.session_state.quick_name}** listo")
        with st.form("qf"):
            cols = st.session_state.quick_df.columns.tolist()
            c1, c2 = st.columns(2)
            tc = c1.selectbox("Columna Título", cols, 0)
            sc = c2.selectbox("Columna Resumen", cols, 1 if len(cols) > 1 else 0)
            st.write("---")
            bn = st.text_input("Marca principal", placeholder="Ej: Bancolombia")
            bat = st.text_area("Alias (sep. ;)", placeholder="Ej: Grupo Bancolombia;Ban",
                               height=70)
            if st.form_submit_button("Analizar", use_container_width=True, type="primary"):
                if not bn:
                    st.error("Indica la marca.")
                else:
                    try:
                        openai.api_key = st.secrets["OPENAI_API_KEY"]
                        openai.aiosession.set(None)
                    except:
                        st.error("OPENAI_API_KEY no encontrada.")
                        st.stop()
                    aliases = [a.strip() for a in bat.split(";") if a.strip()]
                    with st.spinner("Analizando..."):
                        st.session_state.quick_result = asyncio.run(
                            run_quick_analysis_async(
                                st.session_state.quick_df.copy(), tc, sc, bn, aliases
                            )
                        )
                    st.rerun()
        if st.button("Otro archivo"):
            for k in ('quick_df', 'quick_name', 'quick_result', 'quick_cost'):
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()


# ======================================
# Main
# ======================================
def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown("""
    <div class="app-header">
        <div class="app-header-mark">◈</div>
        <div class="app-header-text">
            <div class="app-header-title">Sistema de Análisis de Noticias</div>
            <div class="app-header-version">v11.0 · embedding-cache · weighted-text · iterative-merge · consistency-check</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("main_form"):
                st.markdown('<div class="sec-label">Archivos de entrada</div>',
                            unsafe_allow_html=True)
                c1, c2, c3 = st.columns(3)
                df_file = c1.file_uploader("Dossier (.xlsx)", type=["xlsx"])
                reg_file = c2.file_uploader("Región (.xlsx)", type=["xlsx"])
                int_file = c3.file_uploader("Internet (.xlsx)", type=["xlsx"])

                st.markdown('<div class="sec-label">Marca</div>', unsafe_allow_html=True)
                bn = st.text_input("Nombre principal", placeholder="Ej: Bancolombia", key="bn")
                bat = st.text_area("Alias (sep. ;)",
                                   placeholder="Ej: Grupo Bancolombia;Ban", height=70, key="ba")

                st.markdown('<div class="sec-label">Modo de análisis</div>',
                            unsafe_allow_html=True)
                mode = st.radio("", ["Híbrido (PKL + API)", "Solo Modelos PKL", "API de OpenAI"],
                                index=0, key="mode")
                tpkl, epkl = None, None
                if "PKL" in mode:
                    p1, p2 = st.columns(2)
                    tpkl = p1.file_uploader("pipeline_sentimiento.pkl", type=["pkl"])
                    epkl = p2.file_uploader("pipeline_tema.pkl", type=["pkl"])

                st.markdown(f"""
                <div class="cluster-info">
                  <b>Parámetros de clustering</b><br>
                  UMBRAL_SUBTEMA = {UMBRAL_SUBTEMA} &nbsp;·&nbsp;
                  UMBRAL_TEMA = {UMBRAL_TEMA} &nbsp;·&nbsp;
                  NUM_TEMAS_MAX = {NUM_TEMAS_MAX}<br>
                  UMBRAL_FUSION_INTERGRUPO = {UMBRAL_FUSION_INTERGRUPO} &nbsp;·&nbsp;
                  UMBRAL_DEDUP_LABEL = {UMBRAL_DEDUP_LABEL} &nbsp;·&nbsp;
                  MAX_ITER_FUSION = {MAX_ITER_FUSION}<br>
                  <span style="color:#606068">
                  Aumenta UMBRAL_SUBTEMA para menos subtemas, más generales |
                  Disminúyelo para más subtemas, más específicos
                  </span>
                </div>
                """, unsafe_allow_html=True)

                if st.form_submit_button("Iniciar análisis", use_container_width=True,
                                          type="primary"):
                    if not all([df_file, reg_file, int_file, bn.strip()]):
                        st.error("Completa todos los campos y archivos.")
                    else:
                        aliases = [a.strip() for a in bat.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(
                            df_file, reg_file, int_file, bn, aliases, tpkl, epkl, mode
                        ))
                        st.rerun()
        else:
            total = st.session_state.total_rows
            uniq = st.session_state.unique_rows
            dups = st.session_state.duplicates
            dur = st.session_state.process_duration
            cost = st.session_state.get("process_cost", "$0.00")

            st.markdown(
                '<div class="success-banner"><div class="success-title">Análisis completado</div>'
                '<div class="success-sub">El informe está listo para descargar</div></div>',
                unsafe_allow_html=True
            )
            st.markdown(f"""
            <div class="metrics-row">
              <div class="metric-card"><div class="metric-val" style="color:var(--text)">{total}</div><div class="metric-lbl">Total filas</div></div>
              <div class="metric-card"><div class="metric-val" style="color:var(--green)">{uniq}</div><div class="metric-lbl">Únicas</div></div>
              <div class="metric-card"><div class="metric-val" style="color:var(--accent)">{dups}</div><div class="metric-lbl">Duplicados</div></div>
              <div class="metric-card"><div class="metric-val" style="color:var(--blue)">{dur}</div><div class="metric-lbl">Tiempo</div></div>
              <div class="metric-card"><div class="metric-val" style="color:var(--red)">{cost}</div><div class="metric-lbl">Costo est.</div></div>
            </div>
            """, unsafe_allow_html=True)

            # Mostrar cache stats si están disponibles
            if 'cache_stats' in st.session_state:
                st.caption(f"📊 {st.session_state['cache_stats']}")

            st.download_button(
                "Descargar informe",
                data=st.session_state.output_data,
                file_name=st.session_state.output_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, type="primary"
            )
            if st.button("Nuevo análisis", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()

    with tab2:
        render_quick_tab()

    st.markdown(
        '<div class="footer">v11.0.0 · Realizado por Johnathan Cortés ©</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
