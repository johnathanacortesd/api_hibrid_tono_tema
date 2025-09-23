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
import joblib # Importación para cargar modelos .pkl

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias con IA",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

CONCURRENT_REQUESTS = 40
SIMILARITY_THRESHOLD_TONO = 0.92
SIMILARITY_THRESHOLD_TEMAS = 0.85
SIMILARITY_THRESHOLD_TITULOS = 0.80
MIN_SIMILITUD_TEMAS_CONSOLIDACION = 0.70
MIN_CLUSTER_SIZE_TEMAS = 2
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 80
NUM_TEMAS_PRINCIPALES = 20 # Número de temas principales a generar

# ======================================
# PROMPTS PARA IA (¡Ahora explícitamente aquí!)
# ======================================
TONO_PROMPT_LLM = (
    "Analice ÚNICAMENTE el sentimiento hacia la marca específica mencionada, NO el sentimiento general del texto.\n"
    "Ignore sentimientos sobre otros temas o entidades.\n"
    "Determine polarity basado SOLO en sentimiento hacia la marca: Positivo, Negativo o Neutro.\n"
    "Considere positivo: acuerdos, alianzas, premios, reconocimientos, y la acción proactiva de un vocero respondiendo a una crisis.\n"
    "Responda en JSON con estas llaves y 'tono' final y 'justificacion' de máximo 6 palabras.\n"
    "Marca: {marca_principal}\n"
    "Aliases/voceros: {aliases_str}\n"
    "Texto: {texto_a_analizar}\n"
    '{"tono":"Positivo|Negativo|Neutro","justificacion":"..."}'
)

TEMA_PROMPT_LLM = (
    "Genere un subtema específico y preciso de 2 a 6 palabras que describa el contenido principal de las noticias.\n"
    "Asegúrese de que temas para noticias similares sean idénticos o muy cercanos, prefiriendo términos generales si aplicable para consistencia.\n"
    "INSTRUCCIONES CRITICAS:\n"
    "- Priorice temas coherentes y unificados, evitando variaciones menores.\n"
    "- NO incluya nombres de ciudades colombianas, gentilicios ni frases geograficas.\n"
    "- NO incluya la marca ni sus alias: {terminos_evitar}\n"
    "- Use sustantivos nucleares. Evite terminos genericos como 'noticias' o 'actualidad'.\n"
    "- Si los textos son similares, genere un subtema más general pero preciso para agruparlos.\n"
    "Textos a analizar:\n{lista_textos}\n"
    'Responda solo en JSON: {"subtema":"..."}'
)

TEMA_CONSOLIDACION_PROMPT_LLM = (
    "Eres un experto en categorización de noticias. Dada la siguiente lista de subtemas específicos, genera un nombre de TEMA principal, corto y conciso (2-4 palabras) que los agrupe lógicamente.\n"
    "Lista de Subtemas: {lista_subtemas}\n"
    "Responde solo con el nombre del tema principal, sin explicaciones."
)


# ======================================
# Lexicos y patrones para analisis de tono (SIN CAMBIOS)
# ======================================
CIUDADES_COLOMBIA = {
    "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta",
    "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería",
    "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo",
    "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu",
    "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico",
    "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar",
    "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila",
    "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío",
    "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca",
    "vaupés", "vaupes", "vichada"
}

GENTILICIOS_COLOMBIA = {
    "bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas",
    "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses",
    "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano",
    "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños",
    "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos",
    "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses",
    "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos",
    "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros",
    "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"
}

STOPWORDS_ES = set("""
 a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada
""".split())

POS_VARIANTS = [r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)", r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)", r"apertur(a|ar|ara|o|an)", r"estren(a|o|ara|an|ando)", r"habilit(a|o|ara|an|ando)", r"disponible", r"mejor(a|o|an|ando)", r"optimiza|amplia|expande", r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?", r"crecimi?ento|aument(a|o|an|ando)", r"gananci(a|as)|utilidad(es)?|benefici(o|os)", r"expansion|crece|crecer", r"inversion|invierte|invertir", r"innova(cion|dor|ndo)|moderniza", r"exito(so|sa)?|logr(o|os|a|an|ando)", r"reconoci(miento|do|da)|premi(o|os|ada)", r"lidera(zgo)?|lider", r"consolida|fortalece", r"oportunidad(es)?|potencial", r"solucion(es)?|resuelve", r"eficien(te|cia)", r"calidad|excelencia", r"satisfaccion|complace", r"confianza|credibilidad", r"sostenible|responsable", r"compromiso|apoya|apoyar", r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)", r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)", r"destaca(r|do|da|ndo)?", r"supera(r|ndo|cion)?", r"record|hito|milestone", r"avanza(r|do|da|ndo)?", r"benefici(a|o|ando|ar|ando)", r"importante(s)?", "prioridad", r"bienestar", r"garantizar", r"seguridad", r"atencion", r"expres(o|ó|ando)", r"señala(r|do|ando)", r"ratific(a|o|ando|ar)"]
NEG_VARIANTS = [r"demanda|denuncia|sanciona|multa|investiga|critica", r"cae|baja|pierde|crisis|quiebra|default", r"fraude|escandalo|irregularidad", r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga", r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora", r"problema(s|tica|tico)?|dificultad(es)?", r"retras(o|a|ar|ado)", r"perdida(s)?|deficit", r"conflict(o|os)?|disputa(s)?", r"rechaz(a|o|ar|ado)", r"negativ(o|a|os|as)", r"preocupa(cion|nte|do)?", r"alarma(nte)?|alerta", r"riesgo(s)?|amenaza(s)?"]
CRISIS_KEYWORDS = re.compile(r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS = re.compile(r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)
ACUERDO_PATTERNS = re.compile(r"\b(acuerdo|alianza|convenio|joint\s+venture|memorando|mou|asociaci[oó]n|colaboraci[oó]n|partnership|fusi[oó]n|integraci[oó]n)\b")
NEG_ACUERDO_PATTERNS = re.compile(r"(rompe|anula|rescinde|cancela|revoca|fracasa|frustra).{0,40}(acuerdo|alianza)|(acuerdo|alianza).{0,40}(se cae|fracasa|queda sin efecto|se rompe)", re.IGNORECASE)
EXPRESIONES_NEUTRAS = ["informa","presenta informe","segun informe","segun estudio","de acuerdo con", "participa","asiste","menciona","comenta","cita","segun medios","presenta balance", "ranking","evento","foro","conferencia","panel"]
VERBOS_DECLARATIVOS = ["dijo","afirmo","aseguro","segun","indico","apunto","declaro","explico","estimo", "segun el informe","segun la entidad","segun analistas","de acuerdo con"]
MARCADORES_CONDICIONALES = ["podria","estaria","habria","al parecer","posible","trascendio","se rumora","seria","serian"]

POS_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]

# ======================================
# Estilos CSS Mejorados
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        /* Variables CSS */
        :root {
            --primary-color: #1f77b4;
            --secondary-color: #2ca02c;
            --danger-color: #d62728;
            --warning-color: #ff7f0e;
            --dark-bg: #1e1e1e;
            --card-bg: #ffffff;
            --shadow-light: 0 2px 4px rgba(0,0,0,0.1);
            --shadow-medium: 0 4px 12px rgba(0,0,0,0.15);
            --shadow-heavy: 0 8px 24px rgba(0,0,0,0.2);
            --border-radius: 12px;
        }
        
        /* Animaciones */
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        @keyframes pulse { 0% { transform: scale(1); } 50% { transform: scale(1.02); } 100% { transform: scale(1); } }
        @keyframes slideIn { from { transform: translateX(-20px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        
        /* Header principal */
        .main-header {
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%);
            color: white; padding: 2rem; border-radius: var(--border-radius); text-align: center; font-size: 2.5rem;
            font-weight: 800; margin-bottom: 1.5rem; box-shadow: var(--shadow-medium); animation: fadeIn 0.5s ease-out;
        }
        
        .subtitle { text-align: center; color: #666; font-size: 1.1rem; margin: -1rem 0 2rem 0; animation: fadeIn 0.7s ease-out; }
        
        /* Cards mejoradas */
        .step-card {
            background: var(--card-bg); padding: 1.5rem; border-radius: var(--border-radius); box-shadow: var(--shadow-light);
            border-left: 4px solid var(--primary-color); margin: 1rem 0; transition: all 0.3s ease; animation: slideIn 0.5s ease-out;
        }
        .step-card:hover { box-shadow: var(--shadow-medium); transform: translateX(5px); }
        .success-card {
            background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%); padding: 1.5rem; border-radius: var(--border-radius);
            border: 1px solid #28a745; margin: 1rem 0; box-shadow: var(--shadow-light); animation: pulse 2s infinite;
        }
        .info-card {
            background: linear-gradient(135deg, #d1ecf1 0%, #b8daff 100%); padding: 1.5rem; border-radius: var(--border-radius);
            border: 1px solid #17a2b8; margin: 1rem 0; box-shadow: var(--shadow-light);
        }
        .warning-card {
            background: linear-gradient(135deg, #fff3cd 0%, #ffeeba 100%); padding: 1.5rem; border-radius: var(--border-radius);
            border: 1px solid var(--warning-color); margin: 1rem 0; box-shadow: var(--shadow-light);
        }
        
        /* Métricas mejoradas */
        .metric-card {
            background: var(--card-bg); padding: 1.2rem; border-radius: var(--border-radius); box-shadow: var(--shadow-light);
            text-align: center; transition: all 0.3s ease; border: 1px solid #e0e0e0; animation: fadeIn 0.8s ease-out;
        }
        .metric-card:hover { transform: translateY(-5px); box-shadow: var(--shadow-medium); border-color: var(--primary-color); }
        .metric-value { font-size: 2rem; font-weight: bold; color: var(--primary-color); margin: 0.5rem 0; }
        .metric-label { font-size: 0.9rem; color: #666; text-transform: uppercase; letter-spacing: 1px; }
        
        /* Progress bar mejorada */
        .stProgress > div > div { background: linear-gradient(90deg, var(--primary-color) 0%, var(--secondary-color) 100%); border-radius: 10px; height: 8px; }
        
        /* Botones personalizados */
        .stButton > button { border-radius: 8px; font-weight: 600; transition: all 0.3s ease; box-shadow: var(--shadow-light); }
        .stButton > button:hover { transform: translateY(-2px); box-shadow: var(--shadow-medium); }
        
        /* Badges */
        .badge { display: inline-block; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.85rem; font-weight: 600; margin: 0.2rem; animation: fadeIn 0.5s ease-out; }
        .badge-info { background: #17a2b8; color: white; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ======================================
# Autenticacion y Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    st.markdown('<div class="main-header">🔐 Portal de Acceso Seguro</div>', unsafe_allow_html=True)
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
    max_retries = 3
    delay = 1
    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay)
            delay *= 2

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 3
    delay = 1
    for attempt in range(max_retries):
        try:
            return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            await asyncio.sleep(delay)
            delay *= 2

def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def limpiar_tema(tema: str) -> str:
    if not tema: return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    if tema: tema = tema[0].upper() + tema[1:]
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
    if hasattr(cell, "hyperlink") and cell.hyperlink: return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str): return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    cleaned = tmp[0] if tmp else title
    return re.sub(r"\W+", " ", cleaned).lower().strip()

# ======================================
# Embeddings con cache
# ======================================
EMBED_CACHE: Dict[str, List[float]] = {}
@st.cache_data(ttl=3600)
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto: return None
    key = hashlib.md5(texto[:2000].encode("utf-8")).hexdigest()
    if key in EMBED_CACHE: return EMBED_CACHE[key]
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        emb = resp["data"][0]["embedding"]
        EMBED_CACHE[key] = emb
        return emb
    except Exception: return None

# ======================================
# Agrupacion de textos
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    if not textos: return {}
    embs = [get_embedding(t) for t in textos]
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
    emb_list, valid = [], []
    for i in indices:
        e = get_embedding(textos[i])
        if e is not None: emb_list.append(e); valid.append(i)
    if not emb_list: return indices[0], textos[indices[0]]
    M = np.array(emb_list)
    centro = M.mean(axis=0, keepdims=True)
    sims = cosine_similarity(M, centro).reshape(-1)
    idx = valid[int(np.argmax(sims))]
    return idx, textos[idx]

# ======================================
# Análisis de tono (Reglas, IA y PKL)
# ======================================
class ClasificadorTonoUltraV2:
    def __init__(self, marca: str, aliases: List[str]): self.marca, self.aliases = marca, aliases or []
    def _build_brand_regex(self) -> str:
        names = [self.marca] + [a for a in (self.aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        return r"\b(" + "|".join(patterns) + r")\b" if patterns else r"(a^b)"
    def _analizar_contexto_tono(self, texto: str) -> Dict[str, Any]:
        t = unidecode((texto or "").lower())
        brand_re = self._build_brand_regex()
        pos_hits = sum(1 for p in POS_PATTERNS if re.search(rf"{brand_re}.{{0,{WINDOW}}}{p.pattern}|{p.pattern}.{{0,{WINDOW}}}{brand_re}", t, re.IGNORECASE))
        neg_hits = sum(1 for p in NEG_PATTERNS if re.search(rf"{brand_re}.{{0,{WINDOW}}}{p.pattern}|{p.pattern}.{{0,{WINDOW}}}{brand_re}", t, re.IGNORECASE))
        is_crisis_response = bool(CRISIS_KEYWORDS.search(t)) and bool(re.search(rf"{brand_re}.{{0,50}}{RESPONSE_VERBS.pattern}", t, re.IGNORECASE))
        if is_crisis_response: pos_hits, neg_hits = pos_hits + 2, max(0, neg_hits - 1)
        polarity_score = pos_hits - neg_hits
        return {"polarity": "Positivo" if polarity_score > 0 else "Negativo" if polarity_score < 0 else "Neutro", "is_crisis_response": is_crisis_response}
    def _decidir_tono(self, features: Dict[str, Any]) -> Tuple[str, str]:
        if features.get("is_crisis_response"): return "Positivo", "Respuesta activa a crisis"
        if features["polarity"] == "Positivo": return "Positivo", "Acción favorable de la marca"
        if features["polarity"] == "Negativo": return "Negativo", "Hecho adverso para la marca"
        return "Neutro", "Mención informativa"
    async def _clasificar_grupo_async(self, texto_representante: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            feats = self._analizar_contexto_tono(texto_representante)
            tono, just = self._decidir_tono(feats)
            return {"tono": tono, "justificacion": just}
    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar, titulos_puros: pd.Series):
        textos, n = textos_concat.tolist(), len(textos_concat)
        progress_bar.progress(0.05, text="🔄 Agrupando noticias similares para tono...")
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i])
                return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO), agrupar_por_titulo_similar(titulos_puros.astype(str).tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        representantes = {cid: seleccionar_representante(idxs, textos)[1] for cid, idxs in comp.items()}
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [self._clasificar_grupo_async(rep_texto, semaphore) for rep_texto in representantes.values()]
        resultados_brutos = await asyncio.gather(*tasks)
        resultados_por_grupo = {list(representantes.keys())[i]: res for i, res in enumerate(resultados_brutos)}
        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            r = resultados_por_grupo.get(cid, {"tono": "Neutro", "justificacion": "Sin datos"})
            for i in idxs: resultados_finales[i] = r
        progress_bar.progress(1.0, text="✅ Análisis de tono completado")
        return resultados_finales

def analizar_tono_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[Dict[str, str]]]:
    try:
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        return [{"tono": str(p).title(), "justificacion": "Análisis con modelo PKL"} for p in predicciones]
    except Exception as e:
        st.error(f"❌ Error al procesar el `pipeline_sentimiento.pkl`: {e}")
        st.warning("El pipeline debe ser un objeto Scikit-learn que implemente `.predict()` y acepte una lista de strings.")
        return None

# ======================================
# Clasificador de Temas (IA y PKL)
# ======================================
class ClasificadorTemaDinamico:
    def __init__(self, marca: str, aliases: List[str]): self.marca, self.aliases = marca, aliases or []
    def _generar_tema_para_grupo(self, textos_muestra: List[str]) -> str:
        terminos_evitar = [self.marca.lower()] + [alias.lower() for alias in self.aliases if alias]
        lista_textos_str = "\n---\n".join([m[:500] for m in textos_muestra])
        formatted_prompt = TEMA_PROMPT_LLM.format(
            terminos_evitar=", ".join(terminos_evitar),
            lista_textos=lista_textos_str
        )
        try:
            resp = call_with_retries(openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": formatted_prompt}], max_tokens=40, temperature=0.05, response_format={"type": "json_object"})
            data = json.loads(resp.choices[0].message.content.strip())
            return limpiar_tema_geografico(limpiar_tema(data.get("subtema", "Sin tema")), self.marca, self.aliases)
        except Exception: return limpiar_tema(" ".join(string_norm_label(" ".join(textos_muestra)).split()[:4]) or "Actividad Empresarial")
    def procesar_lote(self, df_columna_resumen: pd.Series, progress_bar, titulos_puros: pd.Series) -> List[str]:
        textos, n = df_columna_resumen.tolist(), len(df_columna_resumen)
        progress_bar.progress(0.10, "🔍 Preparando agrupaciones para subtemas...")
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i])
                return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TEMAS), agrupar_por_titulo_similar(titulos_puros.astype(str).tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        mapa_idx_a_subtema, total_comp = {}, len(comp)
        for hechos, (cid, idxs) in enumerate(comp.items(), 1):
            muestra_textos = [textos[i] for i in idxs[:5]]
            subtema = self._generar_tema_para_grupo(muestra_textos)
            for i in idxs: mapa_idx_a_subtema[i] = subtema
            progress_bar.progress(0.1 + 0.5 * hechos / max(total_comp, 1), f"🏷️ Subtemas creados: {hechos}/{total_comp}")
        return [mapa_idx_a_subtema.get(i, "Sin tema") for i in range(n)]

def consolidar_subtemas_en_temas(subtemas: List[str], p_bar) -> List[str]:
    p_bar.progress(0.6, text=f"📊 Consolidando subtemas en {NUM_TEMAS_PRINCIPALES} temas...")
    mapa_subtema_a_tema, subtemas_unicos = {}, list(set(s for s in subtemas if s != "Sin tema"))
    if not subtemas_unicos or len(subtemas_unicos) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No se requiere consolidación."); return subtemas
    emb_subtemas = {st: get_embedding(st) for st in subtemas_unicos}
    subtemas_validos = [st for st, emb in emb_subtemas.items() if emb is not None]
    if len(subtemas_validos) < NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No hay suficientes subtemas para consolidar."); return subtemas
    emb_matrix = np.array([emb_subtemas[st] for st in subtemas_validos])
    clustering = AgglomerativeClustering(n_clusters=NUM_TEMAS_PRINCIPALES, metric="cosine", linkage="average").fit(emb_matrix)
    mapa_cluster_a_subtemas = defaultdict(list)
    for i, label in enumerate(clustering.labels_): mapa_cluster_a_subtemas[label].append(subtemas_validos[i])
    p_bar.progress(0.8, "🧠 Generando nombres para los temas principales...")
    for cluster_id, lista_subtemas in mapa_cluster_a_subtemas.items():
        formatted_prompt = TEMA_CONSOLIDACION_PROMPT_LLM.format(lista_subtemas=", ".join(lista_subtemas[:10]))
        try:
            resp = call_with_retries(openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": formatted_prompt}], max_tokens=20, temperature=0.1)
            tema_principal = limpiar_tema(resp.choices[0].message.content.strip().replace('"', ''))
        except Exception:
            tema_principal = max(lista_subtemas, key=len)
        for subtema in lista_subtemas: mapa_subtema_a_tema[subtema] = tema_principal
    mapa_subtema_a_tema["Sin tema"] = "Sin tema"
    p_bar.progress(1.0, "✅ Consolidación de temas completada.")
    return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

def analizar_temas_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[str]]:
    try:
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        return [str(p) for p in predicciones]
    except Exception as e:
        st.error(f"❌ Error al procesar el `pipeline_tema.pkl`: {e}")
        st.warning("Asegúrese que el pipeline es un objeto Scikit-learn que implementa `.predict()` y acepta una lista de strings.")
        return None

# ======================================
# Duplicados y Generación de Excel (SIN CAMBIOS)
# ======================================
def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({"titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "tonoai": norm_key("Tono AI"), "justificaciontono": norm_key("Justificacion Tono"), "tema": norm_key("Tema"), "subtema": norm_key("Subtema"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada")})
    rows, split_rows = [], []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)})
    for r_cells in rows:
        base = {k: extract_link(v) if k == norm_key("Link Nota") else v.value for k, v in r_cells.items()}
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in m_list or [None]:
            new = deepcopy(base)
            if m: new[key_map["menciones"]] = m
            split_rows.append(new)
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    for i in range(len(split_rows)):
        if split_rows[i]["is_duplicate"]: continue
        for j in range(i + 1, len(split_rows)):
            if split_rows[j]["is_duplicate"]: continue
            t1, t2 = normalize_title_for_comparison(str(split_rows[i].get(key_map["titulo"]))), normalize_title_for_comparison(str(split_rows[j].get(key_map["titulo"])))
            if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS and norm_key(split_rows[i].get(key_map["medio"])) == norm_key(split_rows[j].get(key_map["medio"])):
                loser_idx = i if len(t1) < len(t2) else j
                winner_idx = j if loser_idx == i else i
                split_rows[loser_idx].update({"is_duplicate": True, "idduplicada": split_rows[winner_idx].get(key_map.get("idnoticia"), "")})
    return split_rows, key_map

def generate_output_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"
    final_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Region","Titulo","Tono AI","Tema","Subtema","Resumen - Aclaracion","Link Nota","Menciones - Empresa","Justificacion Tono","ID duplicada"]
    out_sheet.append(final_order)
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    if "Hyperlink_Custom" not in out_wb.style_names: out_wb.add_named_style(link_style)
    for row_data in all_processed_rows:
        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            nk_header = norm_key(header)
            val = row_data.get(nk_header)
            
            if row_data.get("is_duplicate"):
                if nk_header in [key_map['tonoai'], key_map['tema'], key_map['subtema'], key_map['justificaciontono']]:
                    val = "Duplicada"
                elif nk_header == key_map['idduplicada']:
                    val = row_data.get(key_map['idduplicada'])

            if isinstance(val, dict) and "url" in val:
                row_to_append.append(val.get("value", "Link"))
                if val.get("url"): links_to_add[col_idx] = val["url"]
            else:
                row_to_append.append(str(val) if val is not None else None)
        out_sheet.append(row_to_append)
        for col_idx, url in links_to_add.items():
            out_sheet.cell(row=out_sheet.max_row, column=col_idx).hyperlink, out_sheet.cell(row=out_sheet.max_row, column=col_idx).style = url, "Hyperlink_Custom"
    output = io.BytesIO()
    out_wb.save(output)
    return output.getvalue()

# ======================================
# Proceso principal y UI
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases, tono_pkl_file, tema_pkl_file):
    start_time = time.time()
    try: openai.api_key, openai.aiosession.set(st.secrets["OPENAI_API_KEY"], None)
    except Exception: st.error("❌ OPENAI_API_KEY no encontrado en los Secrets de Streamlit."), st.stop()

    with st.status("📋 **Paso 1/3:** Limpieza y Mapeos", expanded=True) as s:
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        # Lógica de mapeo para region e internet se ejecuta aquí...
        df_region = pd.read_excel(region_file)
        region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
        df_internet = pd.read_excel(internet_file)
        internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
        
        for row in all_processed_rows:
            medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()
            row[key_map.get("region")] = region_map.get(medio_key, "N/A")
            if medio_key in internet_map:
                row[key_map.get("medio")] = internet_map[medio_key]
                row[key_map.get("tipodemedio")] = "Internet" # Asegurarse de que 'tipodemedio' esté en key_map si se usa
        s.update(label="✅ **Paso 1/3:** Completado", state="complete")

    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]
    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        df_temp["resumen_api"] = df_temp[key_map["titulo"]].fillna("").astype(str) + ". " + df_temp[key_map["resumen"]].fillna("").astype(str)

        with st.status("🎯 **Paso 2/3:** Análisis de Tono", expanded=True) as s:
            p_bar_tono = st.progress(0, "Iniciando análisis de tono...")
            if tono_pkl_file:
                p_bar_tono.progress(0.5, "🤖 Usando `pipeline_sentimiento.pkl`...")
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
            else:
                p_bar_tono.progress(0.1, "🤖 Usando IA para análisis de tono...")
                clasif_tono = ClasificadorTonoUltraV2(brand_name, brand_aliases)
                resultados_tono = await clasif_tono.procesar_lote_async(df_temp["resumen_api"], p_bar_tono, df_temp[key_map["titulo"]])
            df_temp[key_map["tonoai"]] = [res["tono"] for res in resultados_tono]
            df_temp[key_map["justificaciontono"]] = [res.get("justificacion", "") for res in resultados_tono]
            s.update(label="✅ **Paso 2/3:** Tono Analizado", state="complete")

        with st.status("🏷️ **Paso 3/3:** Análisis de Tema", expanded=True) as s:
            p_bar_temas = st.progress(0, "Iniciando análisis de temas...")
            if tema_pkl_file:
                p_bar_temas.progress(0.5, "🤖 Usando `pipeline_tema.pkl`...")
                temas_finales = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                if temas_finales is None: st.stop()
                df_temp[key_map["tema"]] = temas_finales
                df_temp[key_map["subtema"]] = temas_finales
            else:
                p_bar_temas.progress(0.1, "🤖 Usando IA para generar Tema y Subtema...")
                clasif_temas = ClasificadorTemaDinamico(brand_name, brand_aliases)
                subtemas = clasif_temas.procesar_lote(df_temp["resumen_api"], p_bar_temas, df_temp[key_map["titulo"]])
                df_temp[key_map["subtema"]] = subtemas
                temas_principales = consolidar_subtemas_en_temas(subtemas, p_bar_temas)
                df_temp[key_map["tema"]] = temas_principales
            s.update(label="✅ **Paso 3/3:** Temas Identificados", state="complete")
        
        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"): row.update(results_map.get(row["original_index"], {}))

    st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
    st.session_state["output_filename"] = f"Informe_IA_{brand_name.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.session_state["processing_complete"] = True
    st.session_state.update({"brand_name": brand_name, "total_rows": len(all_processed_rows), "unique_rows": len(rows_to_analyze), "duplicates": len(all_processed_rows) - len(rows_to_analyze), "process_duration": f"{time.time() - start_time:.0f}s"})

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada Obligatorios")
            col1, col2, col3 = st.columns(3)
            dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
            
            st.markdown("### 🏢 Configuración de Marca Obligatoria")
            brand_name = st.text_input("**Marca Principal**", placeholder="Ej: Bancolombia")
            brand_aliases_text = st.text_area("**Alias y voceros** (separados por ;)", placeholder="Ej: Ban;Juan Carlos Mora", height=80)

            with st.expander("⚙️ Opcional: Usar Modelos Personalizados (.pkl)"):
                st.info("Si subes un archivo aquí, se usará en lugar del análisis con IA para esa tarea.")
                tono_pkl_file = st.file_uploader("Sube `pipeline_sentimiento.pkl` para Tono", type=["pkl"])
                tema_pkl_file = st.file_uploader("Sube `pipeline_tema.pkl` para Tema", type=["pkl"])

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file, brand_name.strip()]):
                    st.error("❌ Faltan archivos obligatorios o el nombre de la marca.")
                else:
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases, tono_pkl_file, tema_pkl_file))
                    st.rerun()
    else:
        st.markdown("## 🎉 Análisis Completado Exitosamente")
        st.download_button("📥 **DESCARGAR INFORME**", data=st.session_state.output_data, file_name=st.session_state.output_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
        if st.button("🔄 **Nuevo Análisis**", use_container_width=True):
            pwd = st.session_state.get("password_correct")
            st.session_state.clear()
            st.session_state.password_correct = pwd
            st.rerun()

if __name__ == "__main__":
    main()
