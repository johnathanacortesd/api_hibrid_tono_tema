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

UMBRAL_SUBTEMA = 0.82
UMBRAL_TEMA    = 0.76
NUM_TEMAS_MAX  = 20

UMBRAL_DEDUP_LABEL = 0.78
UMBRAL_FUSION_INTERGRUPO = 0.84
MAX_ITER_FUSION = 5

PRICE_INPUT_1M     = 0.10
PRICE_OUTPUT_1M    = 0.40
PRICE_EMBEDDING_1M = 0.02

if 'tokens_input'     not in st.session_state: st.session_state['tokens_input']     = 0
if 'tokens_output'    not in st.session_state: st.session_state['tokens_output']    = 0
if 'tokens_embedding' not in st.session_state: st.session_state['tokens_embedding'] = 0

# ── Geografía ─────────────────────────────────────────────────────────────────
CIUDADES_COLOMBIA = {
    "bogotá","bogota","medellín","medellin","cali","barranquilla","cartagena",
    "cúcuta","cucuta","bucaramanga","pereira","manizales","armenia","ibagué",
    "ibague","villavicencio","montería","monteria","neiva","pasto","valledupar",
    "popayán","popayan","tunja","florencia","sincelejo","riohacha","yopal",
    "santa marta","santamarta","quibdó","quibdo","leticia","mocoa","mitú","mitu",
    "puerto carreño","inírida","inirida","san josé del guaviare","antioquia",
    "atlántico","atlantico","bolívar","bolivar","boyacá","boyaca","caldas",
    "caquetá","caqueta","casanare","cauca","cesar","chocó","choco","córdoba",
    "cordoba","cundinamarca","guainía","guainia","guaviare","huila","la guajira",
    "magdalena","meta","nariño","narino","norte de santander","putumayo",
    "quindío","quindio","risaralda","san andrés","san andres","santander",
    "sucre","tolima","valle del cauca","vaupés","vaupes","vichada",
}
GENTILICIOS_COLOMBIA = {
    "bogotano","bogotanos","bogotana","bogotanas","capitalino","capitalinos",
    "capitalina","capitalinas","antioqueño","antioqueños","antioqueña",
    "antioqueñas","paisa","paisas","medellense","medellenses","caleño",
    "caleños","caleña","caleñas","valluno","vallunos","valluna","vallunas",
    "vallecaucano","vallecaucanos","barranquillero","barranquilleros",
    "cartagenero","cartageneros","costeño","costeños","costeña","costeñas",
    "cucuteño","cucuteños","bumangués","santandereano","santandereanos",
    "boyacense","boyacenses","tolimense","tolimenses","huilense","huilenses",
    "nariñense","nariñenses","pastuso","pastusas","cordobés","cordobeses",
    "cauca","caucano","caucanos","chocoano","chocoanos","casanareño",
    "casanareños","caqueteño","caqueteños","guajiro","guajiros","llanero",
    "llaneros","amazonense","amazonenses","colombiano","colombianos",
    "colombiana","colombianas",
}

STOPWORDS_ES = set("""
a ante bajo cabe con contra de desde durante en entre hacia hasta mediante
para por segun sin so sobre tras y o u e la el los las un una unos unas lo
al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este
esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual
cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue
fueron era eran sera seran seria serian he ha han habia habian hay hubo habra
habria estoy esta estan estaba estaban estamos estan estar estare estaria
estuvieron estarian estuvo asi ya mas menos tan tanto cada muy todo toda todos
todas ser haber hacer tener poder deber ir dar ver saber querer llegar pasar
encontrar creer decir poner salir volver seguir llevar sentir cambiar
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
    r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)",
    r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)",
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
    r"problema(s|tica|ico)?|dificultad(es)?",r"retras(o|a|ar|ado)",
    r"perdida(s)?|deficit",
    r"conflict(o|os)?|disputa(s)?",r"rechaz(a|o|ar|ado)",r"negativ(o|a|os|as)",
    r"preocupa(cion|nte|do)?",r"alarma(nte)?|alerta",r"riesgo(s)?|amenaza(s)?",
]
CRISIS_KEYWORDS  = re.compile(
    r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|"
    r"damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE
)
RESPONSE_VERBS   = re.compile(
    r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|"
    r"responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|"
    r"gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE
)
POS_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]


# ======================================
# CSS
# ======================================
def load_custom_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700&family=JetBrains+Mono:wght@300;400;500&display=swap');

:root {
    --bg:#f7f7f5;--s1:#ffffff;--s2:#f0efec;--s3:#e8e7e3;
    --border:#dddbd4;--border2:#c8c5bb;
    --text:#1a1a1a;--text2:#555555;--text3:#999892;
    --accent:#6366f1;--accent2:#4f46e5;--accent-bg:#eef2ff;--accent-bdr:#c7d2fe;
    --green:#16a34a;--green-bg:#f0fdf4;--green-bdr:#bbf7d0;
    --red:#dc2626;--red-bg:#fef2f2;--red-bdr:#fecaca;
    --blue:#2563eb;--blue-bg:#eff6ff;--amber:#d97706;--amber-bg:#fffbeb;
    --r:8px;--r2:12px;--r3:16px;
    --shadow-xs:0 1px 2px rgba(0,0,0,0.04);
    --shadow-sm:0 1px 3px rgba(0,0,0,0.06),0 1px 2px rgba(0,0,0,0.04);
    --shadow-md:0 4px 6px -1px rgba(0,0,0,0.07),0 2px 4px -2px rgba(0,0,0,0.05);
}
html,body,[data-testid="stApp"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;-webkit-font-smoothing:antialiased}
#MainMenu,footer,header{visibility:hidden}.stDeployButton{display:none}
.app-header{background:var(--s1);border:1px solid var(--border);border-radius:var(--r3);padding:1.5rem 2rem;margin-bottom:1.5rem;display:flex;align-items:center;gap:1.2rem;box-shadow:var(--shadow-sm)}
.app-header-icon{width:48px;height:48px;background:linear-gradient(135deg,var(--accent),var(--accent2));border-radius:var(--r2);display:flex;align-items:center;justify-content:center;font-size:1.4rem;color:white;flex-shrink:0;box-shadow:0 2px 8px rgba(99,102,241,0.25)}
.app-header-text{flex:1}
.app-header-title{font-family:'Inter',sans-serif;font-size:1.35rem;font-weight:700;color:var(--text);line-height:1.3;letter-spacing:-0.02em}
.app-header-version{font-family:'JetBrains Mono',monospace;font-size:.65rem;color:var(--text3);letter-spacing:.08em;margin-top:.15rem}
.app-header-badge{background:var(--accent-bg);border:1px solid var(--accent-bdr);color:var(--accent2);font-family:'JetBrains Mono',monospace;font-size:.6rem;font-weight:600;padding:.25rem .6rem;border-radius:20px;letter-spacing:.05em;text-transform:uppercase}
[data-testid="stTabs"] [data-testid="stTabsList"]{background:var(--s1)!important;border:1px solid var(--border)!important;border-radius:var(--r2)!important;padding:4px!important;gap:4px!important;box-shadow:var(--shadow-xs)!important}
[data-testid="stTabs"] button[data-baseweb="tab"]{font-family:'Inter',sans-serif!important;font-size:.82rem!important;font-weight:500!important;color:var(--text2)!important;border-radius:var(--r)!important;padding:.5rem 1.2rem!important;border:none!important;background:transparent!important;transition:all .2s!important}
[data-testid="stTabs"] button[data-baseweb="tab"]:hover{background:var(--s2)!important;color:var(--text)!important}
[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"]{background:var(--accent-bg)!important;color:var(--accent2)!important;border:1px solid var(--accent-bdr)!important;font-weight:600!important}
.metrics-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:.75rem;margin:1.2rem 0}
.metric-card{background:var(--s1);border:1px solid var(--border);border-radius:var(--r2);padding:1.1rem .6rem;text-align:center;transition:all .2s;box-shadow:var(--shadow-xs);position:relative;overflow:hidden}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:var(--r2) var(--r2) 0 0}
.metric-card.m-total::before{background:var(--text2)}.metric-card.m-unique::before{background:var(--green)}.metric-card.m-dup::before{background:var(--amber)}.metric-card.m-time::before{background:var(--blue)}.metric-card.m-cost::before{background:var(--accent)}
.metric-card:hover{border-color:var(--border2);transform:translateY(-1px);box-shadow:var(--shadow-md)}
.metric-val{font-family:'Inter',sans-serif;font-size:1.65rem;font-weight:700;line-height:1;margin-bottom:.4rem}
.metric-lbl{font-family:'JetBrains Mono',monospace;font-size:.6rem;color:var(--text3);text-transform:uppercase;letter-spacing:.1em}
[data-testid="stForm"]{background:var(--s1)!important;border:1px solid var(--border)!important;border-radius:var(--r3)!important;padding:1.8rem!important;box-shadow:var(--shadow-sm)!important}
.sec-label{font-family:'Inter',sans-serif;font-size:.7rem;font-weight:600;color:var(--text3);letter-spacing:.1em;text-transform:uppercase;padding-bottom:.5rem;border-bottom:1px solid var(--border);margin:1.5rem 0 .8rem;display:flex;align-items:center;gap:.5rem}
.sec-label::before{content:'';display:inline-block;width:3px;height:12px;background:var(--accent);border-radius:2px}
[data-testid="stTextInput"] input,[data-testid="stTextArea"] textarea{background:var(--s1)!important;border:1px solid var(--border)!important;color:var(--text)!important;border-radius:var(--r)!important;font-family:'Inter',sans-serif!important;font-size:.88rem!important;transition:border-color .2s,box-shadow .2s!important}
[data-testid="stTextInput"] input:focus,[data-testid="stTextArea"] textarea:focus{border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(99,102,241,0.12)!important;outline:none!important}
[data-testid="stTextInput"] input::placeholder,[data-testid="stTextArea"] textarea::placeholder{color:var(--text3)!important}
label[data-testid="stWidgetLabel"] p{color:var(--text2)!important;font-size:.82rem!important;font-weight:500!important}
[data-testid="stFileUploader"]{background:var(--s1)!important;border:2px dashed var(--border)!important;border-radius:var(--r2)!important;transition:all .2s!important}
[data-testid="stFileUploader"]:hover{border-color:var(--accent)!important;background:var(--accent-bg)!important}
.stButton>button,[data-testid="stDownloadButton"]>button{background:var(--s1)!important;border:1px solid var(--border)!important;color:var(--text)!important;border-radius:var(--r)!important;font-family:'Inter',sans-serif!important;font-weight:500!important;font-size:.85rem!important;transition:all .2s!important;padding:.5rem 1.2rem!important;box-shadow:var(--shadow-xs)!important}
.stButton>button:hover,[data-testid="stDownloadButton"]>button:hover{border-color:var(--accent)!important;color:var(--accent2)!important;background:var(--accent-bg)!important;box-shadow:var(--shadow-sm)!important}
.stButton>button[kind="primary"],[data-testid="stDownloadButton"]>button[kind="primary"]{background:linear-gradient(135deg,var(--accent),var(--accent2))!important;border:none!important;color:#fff!important;font-weight:600!important;box-shadow:0 2px 8px rgba(99,102,241,0.3)!important}
.stButton>button[kind="primary"]:hover,[data-testid="stDownloadButton"]>button[kind="primary"]:hover{box-shadow:0 4px 16px rgba(99,102,241,0.4)!important;transform:translateY(-1px)!important;color:#fff!important}
[data-testid="stRadio"] label{color:var(--text2)!important;font-size:.84rem!important}
[data-testid="stStatus"]{background:var(--s1)!important;border:1px solid var(--border)!important;border-radius:var(--r2)!important;font-family:'JetBrains Mono',monospace!important;font-size:.78rem!important;box-shadow:var(--shadow-xs)!important}
[data-testid="stAlert"]{background:var(--s1)!important;border:1px solid var(--border)!important;border-radius:var(--r)!important;color:var(--text2)!important;font-size:.84rem!important}
.success-banner{background:var(--green-bg);border:1px solid var(--green-bdr);border-left:4px solid var(--green);border-radius:var(--r2);padding:1.2rem 1.5rem;margin:.6rem 0 1.2rem;box-shadow:var(--shadow-xs);display:flex;align-items:center;gap:1rem}
.success-icon{width:36px;height:36px;background:var(--green);border-radius:50%;display:flex;align-items:center;justify-content:center;color:white;font-size:1.1rem;flex-shrink:0}
.success-title{font-family:'Inter',sans-serif;font-size:1rem;font-weight:600;color:var(--green);margin-bottom:.1rem}
.success-sub{font-family:'Inter',sans-serif;font-size:.78rem;color:var(--text2)}
.auth-wrap{max-width:380px;margin:8vh auto 0;text-align:center}
.auth-icon{width:64px;height:64px;background:linear-gradient(135deg,var(--accent),var(--accent2));border-radius:16px;display:inline-flex;align-items:center;justify-content:center;font-size:1.8rem;color:white;margin-bottom:1rem;box-shadow:0 4px 12px rgba(99,102,241,0.3)}
.auth-title{font-family:'Inter',sans-serif;font-size:1.5rem;font-weight:700;color:var(--text);margin-bottom:.3rem}
.auth-sub{font-family:'Inter',sans-serif;font-size:.8rem;color:var(--text3);margin-bottom:2rem}
.cluster-info{background:var(--accent-bg);border:1px solid var(--accent-bdr);border-radius:var(--r2);padding:1rem 1.2rem;margin:.6rem 0;font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--text2);line-height:1.8}
[data-testid="stProgressBar"]>div>div{background:linear-gradient(90deg,var(--accent),var(--accent2))!important;border-radius:4px!important}
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:var(--r)!important;box-shadow:var(--shadow-xs)!important}
hr{border-color:var(--border)!important}
::-webkit-scrollbar{width:6px;height:6px}::-webkit-scrollbar-track{background:var(--s2);border-radius:3px}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}::-webkit-scrollbar-thumb:hover{background:var(--accent)}
.footer{font-family:'JetBrains Mono',monospace;font-size:.6rem;color:var(--text3);text-align:center;padding:1.5rem 0 .8rem;letter-spacing:.08em;border-top:1px solid var(--border);margin-top:2.5rem}
</style>
""", unsafe_allow_html=True)


# ======================================
# Caché Global de Embeddings
# ======================================
class EmbeddingCache:
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
        <div class="auth-icon">◈</div>
        <div class="auth-title">Sistema de Análisis</div>
        <div class="auth-sub">Ingresa tus credenciales para continuar</div>
    </div>""", unsafe_allow_html=True)
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


def capitalizar_etiqueta(tema: str) -> str:
    if not tema or not tema.strip():
        return "Sin tema"
    tema = tema.strip().lower()
    return tema[0].upper() + tema[1:]


def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"\'')
    for prefix in ["subtema:", "tema:", "categoría:", "categoria:", "category:"]:
        if tema.lower().startswith(prefix):
            tema = tema[len(prefix):].strip()
    invalid_end = {"en", "de", "del", "la", "el", "y", "o", "con", "sin",
                   "por", "para", "sobre", "los", "las", "un", "una", "al",
                   "su", "sus", "que", "se"}
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_end:
        palabras.pop()
    tema = " ".join(palabras[:7])
    return capitalizar_etiqueta(tema) if tema else "Sin tema"


def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema:
        return "Sin tema"
    tl = unidecode(tema.lower())
    for name in [marca] + [a for a in aliases if a]:
        tl = re.sub(rf'\b{re.escape(unidecode(name.strip().lower()))}\b', '', tl)
    for ciudad in CIUDADES_COLOMBIA:
        tl = re.sub(rf'\b{re.escape(ciudad)}\b', '', tl)
    for gent in GENTILICIOS_COLOMBIA:
        tl = re.sub(rf'\b{re.escape(gent)}\b', '', tl)
    for frase in ["en colombia", "de colombia", "del pais", "en el pais",
                   "nacional", "colombiano", "colombiana", "colombianos",
                   "colombianas", "territorio nacional"]:
        tl = re.sub(rf'\b{re.escape(frase)}\b', '', tl)
    palabras = [p.strip() for p in tl.split() if p.strip()]
    if not palabras:
        return "Sin tema"
    return limpiar_tema(" ".join(palabras))


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
        "online": "Internet", "internet": "Internet",
        "digital": "Internet", "web": "Internet",
    }.get(t, str(tipo_raw).strip().title() or "Otro")


def texto_para_embedding(titulo: str, resumen: str, max_len: int = 1800) -> str:
    t = str(titulo or "").strip()
    r = str(resumen or "").strip()
    return f"{t}. {t}. {t}. {r}"[:max_len]


# ======================================
# Deduplicación de etiquetas
# ======================================
def dedup_labels(etiquetas: List[str], umbral: float = UMBRAL_DEDUP_LABEL) -> List[str]:
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
            if not normed[j] or find(i) == find(j):
                continue
            if SequenceMatcher(None, normed[i], normed[j]).ratio() >= umbral:
                union(i, j)

    label_embs = get_embeddings_batch(unique)
    valid_pairs = [(i, label_embs[i]) for i in range(n) if label_embs[i] is not None]
    if len(valid_pairs) >= 2:
        vp_idxs, vp_vecs = zip(*valid_pairs)
        sim_mat = cosine_similarity(np.array(vp_vecs))
        for pi in range(len(vp_idxs)):
            for pj in range(pi + 1, len(vp_idxs)):
                if sim_mat[pi][pj] >= umbral + 0.05:
                    ii, jj = vp_idxs[pi], vp_idxs[pj]
                    if find(ii) != find(jj):
                        union(ii, jj)

    freq = Counter(etiquetas)
    grupos: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        grupos[find(i)].append(i)

    canon: Dict[int, str] = {}
    for root, members in grupos.items():
        candidates = [unique[m] for m in members]
        valid = [c for c in candidates if c.lower() not in ("sin tema", "varios")]
        canon[root] = max(valid, key=lambda c: (freq[c], len(c))) if valid else candidates[0]

    label_map = {unique[i]: canon[find(i)] for i in range(n)}
    return [capitalizar_etiqueta(label_map.get(e, e)) for e in etiquetas]


# ======================================
# Embeddings con caché
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos:
        return []
    cache = get_embedding_cache()
    resultados, missing_idxs = cache.get_many(textos)
    if not missing_idxs:
        return resultados

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
# Agrupación para tono
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
        n_clusters=None, distance_threshold=1 - umbral,
        metric="cosine", linkage="average"
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
# Segmentación a nivel de oración
# ======================================
_SENT_SPLIT = re.compile(r'(?<=[.!?;])\s+|(?<=\n)')


def _split_sentences(text: str) -> List[str]:
    """Divide texto en oraciones razonables."""
    parts = _SENT_SPLIT.split(text)
    sents = []
    for p in parts:
        p = p.strip()
        if len(p) > 15:
            sents.append(p)
    return sents if sents else [text[:600]]


# ======================================
# CLASIFICADOR DE TONO — v13 brand-aware
# ======================================
class ClasificadorTono:
    """
    Clasificador que evalúa el sentimiento EXCLUSIVAMENTE hacia la marca
    indicada (nombre + aliases), ignorando el sentimiento hacia competidores
    u otras entidades mencionadas en la misma noticia.
    """

    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca.strip()
        self.aliases = [a.strip() for a in (aliases or []) if a.strip()]
        # Todas las formas de referirse a la marca
        self._all_names = [self.marca] + self.aliases
        # Construir regex para detectar la marca
        patterns = []
        for n in self._all_names:
            p = re.escape(unidecode(n.lower()))
            patterns.append(p)
        self.brand_re = re.compile(
            r"\b(" + "|".join(patterns) + r")\b", re.IGNORECASE
        ) if patterns else re.compile(r"(a^b)")

        # Construir string legible de todos los nombres para prompts
        self._names_str = ", ".join(self._all_names)

    def _extraer_oraciones_marca(self, texto: str) -> List[str]:
        """
        Extrae SOLO las oraciones donde aparece la marca o sus aliases.
        Cada oración incluye la oración previa como contexto.
        Esto evita contaminar el análisis con oraciones sobre competidores.
        """
        oraciones = _split_sentences(texto)
        resultado = []
        for i, sent in enumerate(oraciones):
            sent_norm = unidecode(sent.lower())
            if self.brand_re.search(sent_norm):
                # Incluir oración previa como contexto si existe
                if i > 0:
                    ctx = oraciones[i - 1] + " " + sent
                else:
                    ctx = sent
                resultado.append(ctx.strip())
        # Si la marca no aparece en ninguna oración individual,
        # tomar las primeras 600 chars (mención implícita)
        if not resultado:
            return [texto[:600]]
        # Deduplicar manteniendo orden
        return list(dict.fromkeys(resultado))[:5]

    def _es_sujeto_de_oracion(self, oracion: str) -> bool:
        """
        Heurística: la marca es probablemente el sujeto si aparece
        en la primera mitad de la oración (antes del verbo principal).
        """
        on = unidecode(oracion.lower())
        match = self.brand_re.search(on)
        if not match:
            return False
        pos_marca = match.start()
        # Si la marca está en el primer 60% de la oración, es probable sujeto
        return pos_marca < len(on) * 0.6

    def _analizar_sentimiento_oracion(self, oracion: str) -> Tuple[int, int]:
        """
        Cuenta hits positivos y negativos SOLO si la marca parece ser el sujeto.
        Devuelve (pos_hits, neg_hits).
        """
        on = unidecode(oracion.lower())
        brand_found = self.brand_re.search(on)
        if not brand_found:
            return 0, 0

        # Verificar si hay negación cerca de la marca
        neg_near_brand = bool(re.search(
            r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente|tampoco|ni)\b',
            on[max(0, brand_found.start() - 40):brand_found.end() + 40],
            re.IGNORECASE
        ))

        # Caso especial: crisis donde la marca RESPONDE
        if CRISIS_KEYWORDS.search(on) and RESPONSE_VERBS.search(on):
            if self._es_sujeto_de_oracion(oracion):
                return 3, 0

        ph = sum(1 for p in POS_PATTERNS if p.search(on))
        nh = sum(1 for p in NEG_PATTERNS if p.search(on))

        is_subject = self._es_sujeto_de_oracion(oracion)
        weight = 1.0 if is_subject else 0.3

        if neg_near_brand:
            # Negación invierte: "Tigo no tuvo problemas" → positivo
            return int(nh * weight), int(ph * weight)
        return int(ph * weight), int(nh * weight)

    def _reglas(self, oraciones_marca: List[str]) -> Optional[str]:
        """
        Análisis basado en reglas SOLO sobre oraciones que mencionan la marca.
        """
        total_pos, total_neg = 0, 0
        for sent in oraciones_marca:
            p, n = self._analizar_sentimiento_oracion(sent)
            total_pos += p
            total_neg += n

        # Umbrales más estrictos: necesita señal clara
        if total_pos >= 4 and total_pos > total_neg * 2.5:
            return "Positivo"
        if total_neg >= 4 and total_neg > total_pos * 2.5:
            return "Negativo"
        return None

    async def _llm(self, oraciones_marca: List[str], texto_completo: str) -> Dict[str, str]:
        """
        Prompt LLM diseñado para evaluar SOLO el sentimiento hacia la marca,
        con instrucciones explícitas de ignorar competidores.
        """
        # Enviar oraciones filtradas + contexto general resumido
        fragmentos = "\n".join(f"  → {s[:250]}" for s in oraciones_marca[:4])
        contexto_breve = texto_completo[:300]

        prompt = (
            f"Eres un analista de reputación de marca. Evalúa el sentimiento "
            f"EXCLUSIVAMENTE hacia '{self.marca}' "
            f"(también conocida como: {', '.join(self.aliases) if self.aliases else 'sin aliases'}).\n\n"

            f"REGLAS CRÍTICAS:\n"
            f"1. Evalúa SOLO cómo queda '{self.marca}' en la noticia, NO otras empresas\n"
            f"2. Si la noticia es negativa para un COMPETIDOR pero neutra/positiva para "
            f"'{self.marca}', el tono es Neutro o Positivo\n"
            f"3. Si la noticia es positiva para un COMPETIDOR pero '{self.marca}' solo "
            f"es mencionada sin protagonismo, el tono es Neutro\n"
            f"4. Si '{self.marca}' NO es el sujeto principal de la acción descrita, "
            f"el tono es Neutro\n\n"

            f"CRITERIOS:\n"
            f"• Positivo: '{self.marca}' es presentada favorablemente (logros, premios, "
            f"crecimiento, innovación, alianzas, RSE, lanzamientos)\n"
            f"• Negativo: '{self.marca}' es presentada desfavorablemente (sanciones, "
            f"fraudes, quejas, pérdidas, escándalos, fallas)\n"
            f"• Neutro: Mención informativa sin carga clara, o '{self.marca}' no es "
            f"protagonista del hecho\n\n"

            f"ORACIONES DONDE APARECE '{self.marca}':\n{fragmentos}\n\n"
            f"CONTEXTO GENERAL:\n{contexto_breve}...\n\n"

            f'Responde SOLO JSON: {{"tono":"Positivo|Negativo|Neutro",'
            f'"razon":"explicación breve de por qué"}}'
        )
        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=80, temperature=0.0,
                response_format={"type": "json_object"}
            )
            usage = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += (pt or 0)
                st.session_state['tokens_output'] += (ct or 0)
            parsed = json.loads(resp.choices[0].message.content)
            tono = str(parsed.get("tono", "Neutro")).strip().title()
            return {"tono": tono if tono in ("Positivo", "Negativo", "Neutro") else "Neutro"}
        except:
            return {"tono": "Neutro"}

    async def _clasificar_async(self, texto: str, sem: asyncio.Semaphore):
        async with sem:
            # Paso 1: Extraer SOLO oraciones donde aparece la marca
            oraciones_marca = self._extraer_oraciones_marca(texto)

            # Paso 2: Intentar clasificar con reglas
            resultado_reglas = self._reglas(oraciones_marca)
            if resultado_reglas:
                return {"tono": resultado_reglas}

            # Paso 3: Clasificar con LLM enviando oraciones filtradas
            return await self._llm(oraciones_marca, texto)

    async def procesar_lote_async(self, textos: pd.Series, pbar,
                                   resumenes: pd.Series, titulos: pd.Series):
        n = len(textos)
        txts = textos.tolist()
        pbar.progress(0.05, "Agrupando para análisis de tono...")

        txts_emb = [
            texto_para_embedding(str(titulos.iloc[i]), str(resumenes.iloc[i]))
            for i in range(n)
        ]

        # Agrupar noticias similares para clasificar una sola vez
        dsu = DSU(n)
        for g in [agrupar_textos_similares(txts_emb, SIMILARITY_THRESHOLD_TONO),
                  agrupar_por_titulo_similar(titulos.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]:
                    dsu.union(idxs[0], j)

        grupos = dsu.grupos(n)

        # Para cada grupo, seleccionar el representante más central
        reps = {}
        for cid, idxs in grupos.items():
            _, rep_txt = seleccionar_representante(idxs, txts)
            reps[cid] = rep_txt

        # Clasificar cada representante de grupo
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        cids_list = list(reps.keys())
        tasks = [self._clasificar_async(reps[cid], sem) for cid in cids_list]

        resultados_list = []
        for i, f in enumerate(asyncio.as_completed(tasks)):
            resultados_list.append(await f)
            pbar.progress(
                0.1 + 0.85 * (i + 1) / len(tasks),
                f"Analizando tono {i + 1}/{len(tasks)}"
            )

        res_por_grupo = {cids_list[i]: r for i, r in enumerate(resultados_list)}

        # Propagar resultado del representante a todo el grupo
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
# CLASIFICADOR DE SUBTEMAS
# ======================================
class ClasificadorSubtema:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self._cache: Dict[str, str] = {}

    def _paso1_hash_exacto(self, titulos: List[str], resumenes: List[str], dsu: DSU):
        def norm_tokens(t: str) -> List[str]:
            return re.sub(r'[^a-z0-9\s]', '', unidecode(str(t).lower())).split()

        bkt_tit: Dict[str, List[int]] = defaultdict(list)
        bkt_res: Dict[str, List[int]] = defaultdict(list)
        for i, (tit, res) in enumerate(zip(titulos, resumenes)):
            nt = ' '.join(norm_tokens(tit)[:40])
            nr = ' '.join(norm_tokens(res)[:15])
            if nt:
                bkt_tit[hashlib.md5(nt.encode()).hexdigest()].append(i)
            if nr:
                bkt_res[hashlib.md5(nr.encode()).hexdigest()].append(i)
        for bkt in (bkt_tit, bkt_res):
            for idxs in bkt.values():
                for j in idxs[1:]:
                    dsu.union(idxs[0], j)

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

    def _paso3_semantico_completo(self, emb_textos: List[str],
                                   all_embs: List[Optional[List[float]]],
                                   dsu: DSU, pbar, p_start: float):
        n = len(emb_textos)
        if n < 2:
            return
        BATCH = 500
        if n <= BATCH:
            pbar.progress(p_start, "Clustering semántico global...")
            ok = [(k, e) for k, e in enumerate(all_embs) if e is not None]
            if len(ok) < 2:
                return
            idxs_ok, M = zip(*ok)
            sim = cosine_similarity(np.array(M))
            labels = AgglomerativeClustering(
                n_clusters=None, distance_threshold=1 - UMBRAL_SUBTEMA,
                metric='precomputed', linkage='average'
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

        total_batches = max(1, (n + BATCH - 1) // BATCH)
        for b_num, b_start in enumerate(range(0, n, BATCH)):
            batch_idxs = list(range(b_start, min(b_start + BATCH, n)))
            ok = [(idx, all_embs[idx]) for idx in batch_idxs if all_embs[idx] is not None]
            if len(ok) < 2:
                continue
            idxs_ok, M = zip(*ok)
            sim = cosine_similarity(np.array(M))
            labels = AgglomerativeClustering(
                n_clusters=None, distance_threshold=1 - UMBRAL_SUBTEMA,
                metric='precomputed', linkage='average'
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
        pbar.progress(p_start + 0.16, "Unificando lotes...")
        self._fusion_intergrupo_iterativa(emb_textos, all_embs, dsu, pbar, p_start + 0.16)

    def _fusion_intergrupo_iterativa(self, textos: List[str],
                                      all_embs: List[Optional[List[float]]],
                                      dsu: DSU, pbar, p_start: float):
        n = len(textos)
        for iteration in range(MAX_ITER_FUSION):
            grupos = dsu.grupos(n)
            if len(grupos) < 2:
                break
            centroids, valid_gids = [], []
            for gid, idxs in grupos.items():
                vecs = [all_embs[i] for i in idxs[:50] if all_embs[i] is not None]
                if vecs:
                    centroids.append(np.mean(vecs, axis=0))
                    valid_gids.append(gid)
            if len(valid_gids) < 2:
                break
            sim = cosine_similarity(np.array(centroids))
            pairs = sorted(
                [(sim[i][j], i, j) for i in range(len(valid_gids))
                 for j in range(i + 1, len(valid_gids))
                 if sim[i][j] >= UMBRAL_FUSION_INTERGRUPO],
                reverse=True
            )
            fusiones = 0
            for _, i, j in pairs:
                rep_i = grupos[valid_gids[i]][0]
                rep_j = grupos[valid_gids[j]][0]
                if dsu.find(rep_i) != dsu.find(rep_j):
                    dsu.union(rep_i, rep_j)
                    fusiones += 1
            pbar.progress(
                min(p_start + 0.04 * (iteration + 1), 0.52),
                f"Fusión iter {iteration + 1}: {fusiones} fusiones"
            )
            if fusiones == 0:
                break

    def _generar_etiqueta(self, textos_grp: List[str], titulos_grp: List[str],
                           resumenes_grp: List[str]) -> str:
        titulos_norm = sorted(set(normalize_title_for_comparison(t) for t in titulos_grp if t))
        cache_key = hashlib.md5("|".join(titulos_norm[:12]).encode()).hexdigest()
        if cache_key in self._cache:
            return self._cache[cache_key]

        palabras = []
        for t in titulos_grp[:8]:
            for w in string_norm_label(t).split():
                if len(w) > 3:
                    palabras.append(w)
        keywords = " · ".join(w for w, _ in Counter(palabras).most_common(8))
        titulos_muestra = list(dict.fromkeys(t[:120] for t in titulos_grp if t))[:6]
        resumenes_muestra = [str(r)[:200] for r in resumenes_grp[:3]
                             if r and len(str(r)) > 20]
        ctx_res = ""
        if resumenes_muestra:
            ctx_res = "\n\nFRAGMENTOS DE RESÚMENES:\n" + \
                      "\n".join(f"  · {r}" for r in resumenes_muestra)

        prompt = (
            "Genera un SUBTEMA periodístico en español (3-5 palabras) que describa "
            "con precisión el asunto central y específico de estas noticias.\n\n"
            f"TÍTULOS:\n" + "\n".join(f"  · {t}" for t in titulos_muestra) +
            ctx_res + "\n\n"
            f"PALABRAS CLAVE: {keywords}\n\n"
            "REGLAS OBLIGATORIAS:\n"
            "  1. NO uses el nombre de empresas, marcas, personas ni ciudades\n"
            "  2. NO uses palabras genéricas solas como 'Gestión', 'Actividades', "
            "'Acciones', 'Noticias', 'Información', 'Eventos', 'Varios'\n"
            "  3. El subtema debe describir EL ASUNTO ESPECÍFICO, no el actor\n"
            "  4. Debe ser suficientemente específico para distinguir estas noticias\n"
            "  5. Usa sustantivos y adjetivos descriptivos\n"
            "  6. FORMATO: todo en minúsculas excepto la primera letra\n"
            "  7. Ejemplos CORRECTOS: 'Resultados financieros trimestrales', "
            "'Programa becas universitarias', 'Expansión red sucursales'\n"
            "  8. Ejemplos INCORRECTOS: 'Gestión Empresarial', 'RESULTADOS'\n\n"
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

            genericas = {"gestión", "gestion", "actividades", "acciones", "noticias",
                         "información", "informacion", "eventos", "varios", "sin tema",
                         "actividad corporativa", "noticias corporativas",
                         "gestión empresarial", "gestion empresarial",
                         "cobertura informativa", "gestión integral"}
            if (string_norm_label(etiqueta) in {string_norm_label(g) for g in genericas}
                    or len(etiqueta.split()) < 2):
                etiqueta = self._refinar_etiqueta(titulos_muestra, keywords, resumenes_muestra)
        except:
            etiqueta = self._etiqueta_fallback(titulos_grp)

        etiqueta = capitalizar_etiqueta(etiqueta)
        self._cache[cache_key] = etiqueta
        return etiqueta

    def _refinar_etiqueta(self, titulos, keywords, resumenes=None):
        ctx_extra = ""
        if resumenes:
            ctx_extra = f"\nContexto: {' | '.join(r[:100] for r in resumenes[:2])}"
        prompt = (
            "Los siguientes títulos comparten un tema. Identifica el tema "
            "ESPECÍFICO en 3-5 palabras.\n\n"
            f"Títulos: {' | '.join(titulos[:4])}\n"
            f"Keywords: {keywords}{ctx_extra}\n\n"
            "NO uses palabras genéricas. Sé específico.\n"
            "FORMATO: todo en minúsculas excepto la primera letra.\n"
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

    def _etiqueta_fallback(self, titulos):
        if not titulos:
            return "Cobertura informativa"
        palabras = []
        for t in titulos[:5]:
            for w in string_norm_label(t).split():
                if len(w) > 4:
                    palabras.append(w)
        if palabras:
            top = [w for w, _ in Counter(palabras).most_common(3)]
            return capitalizar_etiqueta(" ".join(top[:3]))
        return "Cobertura informativa"

    def procesar_lote(self, col_resumen: pd.Series, pbar,
                      resumenes_puros: pd.Series, titulos_puros: pd.Series) -> List[str]:
        textos = col_resumen.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumenes_puros.tolist()
        n = len(textos)

        emb_textos = [texto_para_embedding(titulos[i], resumenes[i]) for i in range(n)]

        pbar.progress(0.05, "Fase 1 · Agrupando noticias idénticas...")
        dsu = DSU(n)
        self._paso1_hash_exacto(titulos, resumenes, dsu)

        pbar.progress(0.12, "Fase 2 · Similitud de títulos...")
        self._paso2_titulos_similares(titulos, dsu)

        pbar.progress(0.18, "Calculando embeddings...")
        all_embs = get_embeddings_batch(emb_textos)

        pbar.progress(0.20, "Fase 3 · Clustering semántico global...")
        self._paso3_semantico_completo(emb_textos, all_embs, dsu, pbar, p_start=0.20)

        grupos_finales = dsu.grupos(n)
        n_grupos = len(grupos_finales)

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

        pbar.progress(0.88, "Fase 5 · Deduplicando etiquetas similares...")
        subtemas = dedup_labels(subtemas, UMBRAL_DEDUP_LABEL)

        pbar.progress(0.93, "Fase 6 · Verificando consistencia...")
        subtemas = self._verificar_consistencia(subtemas, all_embs, pbar)

        subtemas = [capitalizar_etiqueta(s) for s in subtemas]

        n_final = len(set(subtemas))
        pbar.progress(1.0, f"Completado: {n_final} subtemas en {n_grupos} grupos")
        st.info(f"Subtemas únicos: **{n_final}** · Grupos semánticos: **{n_grupos}**")
        return subtemas

    def _verificar_consistencia(self, subtemas, all_embs, pbar):
        por_subtema: Dict[str, List[int]] = defaultdict(list)
        for i, sub in enumerate(subtemas):
            por_subtema[sub].append(i)

        resultado = list(subtemas)
        centroids: Dict[str, np.ndarray] = {}
        for sub, idxs in por_subtema.items():
            vecs = [all_embs[i] for i in idxs if all_embs[i] is not None]
            if vecs:
                centroids[sub] = np.mean(vecs, axis=0)

        for sub in [s for s in centroids if len(por_subtema[s]) >= 3]:
            idxs = por_subtema[sub]
            if sub.lower() in ("sin tema", "varios") or len(idxs) < 3:
                continue
            valid_items = [(i, all_embs[i]) for i in idxs if all_embs[i] is not None]
            if len(valid_items) < 3:
                continue
            v_idxs, v_vecs = zip(*valid_items)
            M = np.array(v_vecs)
            centroid = centroids[sub]
            sims = cosine_similarity(M, centroid.reshape(1, -1)).flatten()
            threshold = max(0.60, np.mean(sims) - 2 * np.std(sims))

            for k, (orig_idx, sim_val) in enumerate(zip(v_idxs, sims)):
                if sim_val < threshold:
                    best_sub, best_sim = sub, sim_val
                    emb = all_embs[orig_idx]
                    for other_sub, other_centroid in centroids.items():
                        if other_sub == sub:
                            continue
                        s = cosine_similarity(
                            np.array(emb).reshape(1, -1),
                            other_centroid.reshape(1, -1)
                        )[0][0]
                        if s > best_sim and s > 0.75:
                            best_sim = s
                            best_sub = other_sub
                    if best_sub != sub:
                        resultado[orig_idx] = best_sub
        return resultado


# ======================================
# CONSOLIDACIÓN DE TEMAS
# ======================================
def _generalizar_subtema(subtema: str) -> str:
    palabras = subtema.split()
    return " ".join(palabras[:min(3, len(palabras))]) if len(palabras) > 2 else subtema


def consolidar_temas(subtemas: List[str], textos: List[str], pbar) -> List[str]:
    pbar.progress(0.05, "Calculando centroides de subtemas...")
    df = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    unique_subs = list(df['subtema'].unique())

    if len(unique_subs) <= 1:
        pbar.progress(1.0, "Un solo tema")
        return [capitalizar_etiqueta(s) for s in subtemas]

    all_embs = get_embeddings_batch(textos)
    centroids: Dict[str, np.ndarray] = {}
    for sub in unique_subs:
        idxs = df.index[df['subtema'] == sub].tolist()[:40]
        vecs = [all_embs[i] for i in idxs if all_embs[i] is not None]
        if vecs:
            centroids[sub] = np.mean(vecs, axis=0)

    label_embs_raw = get_embeddings_batch(unique_subs)
    label_embs = {s: e for s, e in zip(unique_subs, label_embs_raw) if e is not None}

    valid_subs = [s for s in unique_subs if s in centroids]
    if len(valid_subs) < 2:
        pbar.progress(1.0, "Sin agrupación posible")
        return [capitalizar_etiqueta(s) for s in subtemas]

    pbar.progress(0.40, "Clustering de subtemas en temas...")
    M_content = np.array([centroids[s] for s in valid_subs])
    sim_content = cosine_similarity(M_content)

    if all(s in label_embs for s in valid_subs):
        M_labels = np.array([label_embs[s] for s in valid_subs])
        sim = 0.6 * sim_content + 0.4 * cosine_similarity(M_labels)
    else:
        sim = sim_content

    clustering = AgglomerativeClustering(
        n_clusters=None, distance_threshold=1 - UMBRAL_TEMA,
        metric='precomputed', linkage='average'
    ).fit(1 - sim)

    if len(set(clustering.labels_)) > NUM_TEMAS_MAX:
        clustering = AgglomerativeClustering(
            n_clusters=NUM_TEMAS_MAX, metric='precomputed', linkage='average'
        ).fit(1 - sim)

    clusters_subs: Dict[int, List[str]] = defaultdict(list)
    for i, lbl in enumerate(clustering.labels_):
        clusters_subs[lbl].append(valid_subs[i])

    unclustered = [s for s in unique_subs if s not in valid_subs]
    mapa_tema: Dict[str, str] = {}
    total_clusters = len(clusters_subs)

    for k, (cid, lista_subs) in enumerate(clusters_subs.items()):
        pbar.progress(0.50 + 0.40 * (k / max(total_clusters, 1)),
                      f"Generando tema {k + 1}/{total_clusters}...")
        if len(lista_subs) == 1:
            nombre = _generalizar_subtema(lista_subs[0])
        else:
            prompt = (
                "Genera UNA categoría temática general en español (2-4 palabras) "
                "que agrupe estos subtemas periodísticos.\n\n"
                f"SUBTEMAS:\n" +
                "\n".join(f"  · {s}" for s in lista_subs[:10]) + "\n\n"
                "REGLAS:\n"
                "  - Sin nombres de empresas, marcas ni ciudades\n"
                "  - Sin verbos ni artículos iniciales\n"
                "  - FORMATO: todo en minúsculas excepto la primera letra\n"
                "  - Usa sustantivos descriptivos\n"
                "  - NO tan vaga como 'Noticias' o 'Información'\n"
                "  - Responde SOLO el nombre del tema"
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
                    pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                    ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                    st.session_state['tokens_input'] += (pt or 0)
                    st.session_state['tokens_output'] += (ct or 0)
                nombre = limpiar_tema(
                    resp.choices[0].message.content.strip().replace('"', '').replace('.', '')
                )
            except:
                nombre = lista_subs[0]

        for sub in lista_subs:
            mapa_tema[sub] = nombre

    for sub in unclustered:
        mapa_tema[sub] = sub

    temas_final = [mapa_tema.get(sub, sub) for sub in subtemas]
    pbar.progress(0.92, "Deduplicando etiquetas de temas...")
    temas_final = dedup_labels(temas_final, UMBRAL_DEDUP_LABEL)
    temas_final = [capitalizar_etiqueta(t) for t in temas_final]

    n_temas = len(set(temas_final))
    st.info(f"Temas consolidados: **{n_temas}** (máximo configurado: {NUM_TEMAS_MAX})")
    pbar.progress(1.0, "Temas finalizados")
    return temas_final


def analizar_temas_con_pkl(textos, pkl_file):
    try:
        pipeline = joblib.load(pkl_file)
        return [capitalizar_etiqueta(str(p)) for p in pipeline.predict(textos)]
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
                    row["idduplicada"] = processed[seen_url[k]].get(
                        key_map.get("idnoticia", ""), "")
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
                    row["idduplicada"] = processed[seen_bcast[k]].get(
                        key_map.get("idnoticia", ""), "")
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
                ta = normalize_title_for_comparison(
                    processed[a].get(key_map.get("titulo", "")))
                tb = normalize_title_for_comparison(
                    processed[b].get(key_map.get("titulo", "")))
                if (ta and tb and
                        SequenceMatcher(None, ta, tb).ratio() >= SIMILARITY_THRESHOLD_TITULOS):
                    if len(ta) < len(tb):
                        processed[a]["is_duplicate"] = True
                        processed[a]["idduplicada"] = processed[b].get(
                            key_map.get("idnoticia", ""), "")
                    else:
                        processed[b]["is_duplicate"] = True
                        processed[b]["idduplicada"] = processed[a].get(
                            key_map.get("idnoticia", ""), "")
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
            k: (extract_link(v) if k in (key_map["link_nota"], key_map["link_streaming"])
                 else v.value)
            for k, v in r_cells.items()
        }
        if key_map.get("tipodemedio") in base:
            base[key_map["tipodemedio"]] = normalizar_tipo_medio(
                base.get(key_map["tipodemedio"]))
        ml = [m.strip() for m in
              str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
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
    ORDER = [
        "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Region",
        "Seccion - Programa", "Titulo", "Autor - Conductor", "Nro. Pagina",
        "Dimension", "Duracion - Nro. Caracteres", "CPE", "Audiencia", "Tier",
        "Tono", "Tono IA", "Tema", "Subtema", "Link Nota",
        "Resumen - Aclaracion", "Link (Streaming - Imagen)",
        "Menciones - Empresa", "ID duplicada"
    ]
    NUM = {"ID Noticia", "Nro. Pagina", "Dimension",
           "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    ws.append(ORDER)
    ls_style = NamedStyle(name="HL", font=Font(color="0000FF", underline="single"))
    if "HL" not in wb.style_names:
        wb.add_named_style(ls_style)
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
async def run_full_process_async(dossier_file, region_file, internet_file,
                                  brand_name, brand_aliases, tono_pkl_file,
                                  tema_pkl_file, analysis_mode):
    st.session_state.update(
        {'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0})
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
        rows, key_map = run_dossier_logic(
            load_workbook(dossier_file, data_only=True).active)
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
        df["_txt"] = df.apply(
            lambda r: texto_para_embedding(
                str(r.get(key_map["titulo"], "")),
                str(r.get(key_map["resumen"], ""))
            ), axis=1)

        with st.status("Pre-calculando embeddings...", expanded=True) as s:
            _ = get_embeddings_batch(df["_txt"].tolist())
            cache_stats = get_embedding_cache().stats()
            s.update(label=f"Embeddings calculados · {cache_stats}",
                     state="complete")

        with st.status("Paso 3 · Tono", expanded=True) as s:
            pb = st.progress(0)
            if "PKL" in analysis_mode and tono_pkl_file:
                res = analizar_tono_con_pkl(df["_txt"].tolist(), tono_pkl_file)
                if res is None:
                    st.stop()
            elif "API" in analysis_mode:
                res = await ClasificadorTono(
                    brand_name, brand_aliases
                ).procesar_lote_async(
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
                subtemas = ClasificadorSubtema(
                    brand_name, brand_aliases
                ).procesar_lote(
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
async def run_quick_analysis_async(df, title_col, summary_col,
                                    brand_name, aliases):
    st.session_state.update(
        {'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0})
    get_embedding_cache().clear()

    df['_txt'] = df.apply(
        lambda r: texto_para_embedding(
            str(r.get(title_col, "")), str(r.get(summary_col, ""))
        ), axis=1)

    with st.status("Pre-calculando embeddings...", expanded=True) as s:
        _ = get_embeddings_batch(df['_txt'].tolist())
        s.update(label=f"Embeddings listos · {get_embedding_cache().stats()}",
                 state="complete")

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
    st.markdown('<div class="sec-label">Análisis rápido</div>',
                unsafe_allow_html=True)
    if 'quick_result' in st.session_state:
        st.markdown(
            '<div class="success-banner">'
            '<div class="success-icon">✓</div>'
            '<div class="success-content">'
            '<div class="success-title">Análisis completado</div>'
            '<div class="success-sub">Los resultados están listos</div>'
            '</div></div>', unsafe_allow_html=True
        )
        st.metric("Costo estimado", st.session_state.get('quick_cost', "$0.00"))
        st.dataframe(st.session_state.quick_result.head(10),
                     use_container_width=True)
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
        f = st.file_uploader("Archivo Excel", type=["xlsx"],
                             label_visibility="collapsed", key="qu")
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
            sc = c2.selectbox("Columna Resumen", cols,
                              1 if len(cols) > 1 else 0)
            st.write("---")
            bn = st.text_input("Marca principal",
                               placeholder="Ej: Bancolombia")
            bat = st.text_area("Alias (sep. ;)",
                               placeholder="Ej: Grupo Bancolombia;Ban",
                               height=70)
            if st.form_submit_button("Analizar", use_container_width=True,
                                      type="primary"):
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
                                st.session_state.quick_df.copy(),
                                tc, sc, bn, aliases
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
        <div class="app-header-icon">◈</div>
        <div class="app-header-text">
            <div class="app-header-title">Sistema de Análisis de Noticias</div>
            <div class="app-header-version">v13.0 · brand-aware tone · sentence-level analysis · semantic-dedup</div>
        </div>
        <div class="app-header-badge">IA Powered</div>
    </div>""", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("main_form"):
                st.markdown(
                    '<div class="sec-label">Archivos de entrada</div>',
                    unsafe_allow_html=True)
                c1, c2, c3 = st.columns(3)
                df_file = c1.file_uploader("Dossier (.xlsx)", type=["xlsx"])
                reg_file = c2.file_uploader("Región (.xlsx)", type=["xlsx"])
                int_file = c3.file_uploader("Internet (.xlsx)", type=["xlsx"])

                st.markdown('<div class="sec-label">Marca</div>',
                            unsafe_allow_html=True)
                bn = st.text_input("Nombre principal",
                                   placeholder="Ej: Bancolombia", key="bn")
                bat = st.text_area(
                    "Alias (sep. ;)",
                    placeholder="Ej: Grupo Bancolombia;Ban", height=70,
                    key="ba")

                st.markdown('<div class="sec-label">Modo de análisis</div>',
                            unsafe_allow_html=True)
                mode = st.radio(
                    "", ["Híbrido (PKL + API)", "Solo Modelos PKL",
                         "API de OpenAI"],
                    index=0, key="mode")
                tpkl, epkl = None, None
                if "PKL" in mode:
                    p1, p2 = st.columns(2)
                    tpkl = p1.file_uploader("pipeline_sentimiento.pkl",
                                            type=["pkl"])
                    epkl = p2.file_uploader("pipeline_tema.pkl", type=["pkl"])

                st.markdown(f"""
                <div class="cluster-info">
                  <b>Parámetros</b><br>
                  UMBRAL_SUBTEMA={UMBRAL_SUBTEMA} · UMBRAL_TEMA={UMBRAL_TEMA} · NUM_TEMAS_MAX={NUM_TEMAS_MAX}<br>
                  UMBRAL_FUSION={UMBRAL_FUSION_INTERGRUPO} · UMBRAL_DEDUP={UMBRAL_DEDUP_LABEL} · MAX_ITER={MAX_ITER_FUSION}
                </div>""", unsafe_allow_html=True)

                if st.form_submit_button("Iniciar análisis",
                                          use_container_width=True,
                                          type="primary"):
                    if not all([df_file, reg_file, int_file, bn.strip()]):
                        st.error("Completa todos los campos y archivos.")
                    else:
                        aliases = [a.strip() for a in bat.split(";")
                                   if a.strip()]
                        asyncio.run(run_full_process_async(
                            df_file, reg_file, int_file, bn, aliases,
                            tpkl, epkl, mode
                        ))
                        st.rerun()
        else:
            total = st.session_state.total_rows
            uniq = st.session_state.unique_rows
            dups = st.session_state.duplicates
            dur = st.session_state.process_duration
            cost = st.session_state.get("process_cost", "$0.00")

            st.markdown(
                '<div class="success-banner">'
                '<div class="success-icon">✓</div>'
                '<div class="success-content">'
                '<div class="success-title">Análisis completado</div>'
                '<div class="success-sub">El informe está listo</div>'
                '</div></div>', unsafe_allow_html=True
            )
            st.markdown(f"""
            <div class="metrics-grid">
              <div class="metric-card m-total"><div class="metric-val" style="color:var(--text)">{total}</div><div class="metric-lbl">Total filas</div></div>
              <div class="metric-card m-unique"><div class="metric-val" style="color:var(--green)">{uniq}</div><div class="metric-lbl">Únicas</div></div>
              <div class="metric-card m-dup"><div class="metric-val" style="color:var(--amber)">{dups}</div><div class="metric-lbl">Duplicados</div></div>
              <div class="metric-card m-time"><div class="metric-val" style="color:var(--blue)">{dur}</div><div class="metric-lbl">Tiempo</div></div>
              <div class="metric-card m-cost"><div class="metric-val" style="color:var(--accent)">{cost}</div><div class="metric-lbl">Costo est.</div></div>
            </div>""", unsafe_allow_html=True)

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
        '<div class="footer">v13.0.0 · Realizado por Johnathan Cortés ©</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
