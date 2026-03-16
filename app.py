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
import concurrent.futures

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

CONCURRENT_REQUESTS = 24  # Aumentado para mayor velocidad
SIMILARITY_THRESHOLD_TONO = 0.92
SIMILARITY_THRESHOLD_TEMAS = 0.88
MIN_SIMILITUD_TEMAS_CONSOLIDACION = 0.90
MIN_CLUSTER_SIZE_TEMAS = 2
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 80
SESSION_ALIASES_KEY = "brand_aliases"

# Lista de ciudades y gentilicios colombianos para filtrar
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

# ======================================
# Lexicos y patrones para analisis de tono
# ======================================
STOPWORDS_ES = set("""
 a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada
""".split())

POS_VARIANTS = [
    r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?",
    r"prepar(a|ando)",
    r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)",
    r"apertur(a|ar|ara|o|an)",
    r"estren(a|o|ara|an|ando)",
    r"habilit(a|o|ara|an|ando)",
    r"disponible",
    r"mejor(a|o|an|ando)",
    r"optimiza|amplia|expande",
    r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?",
    r"crecimi?ento|aument(a|o|an|ando)",
    r"gananci(a|as)|utilidad(es)?|benefici(o|os)",
    r"expansion|crece|crecer",
    r"inversion|invierte|invertir",
    r"innova(cion|dor|ndo)|moderniza",
    r"exito(so|sa)?|logr(o|os|a|an|ando)",
    r"reconoci(miento|do|da)|premi(o|os|ada)",
    r"lidera(zgo)?|lider",
    r"consolida|fortalece",
    r"oportunidad(es)?|potencial",
    r"solucion(es)?|resuelve",
    r"eficien(te|cia)",
    r"calidad|excelencia",
    r"satisfaccion|complace",
    r"confianza|credibilidad",
    r"sostenible|responsable",
    r"compromiso|apoya|apoyar"
]

NEG_VARIANTS = [
    r"demanda|denuncia|sanciona|multa|investiga|critica",
    r"cae|baja|pierde|crisis|quiebra|default",
    r"fraude|escandalo|irregularidad",
    r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga",
    r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora",
]

# Patrones explícitos para acuerdos
ACUERDO_PATTERNS = re.compile(
    r"\b(acuerdo|alianza|convenio|joint\s+venture|memorando|mou|asociaci[oó]n|colaboraci[oó]n|partnership|fusi[oó]n|integraci[oó]n)\b"
)
NEG_ACUERDO_PATTERNS = re.compile(
    r"(rompe|anula|rescinde|cancela|revoca|fracasa|frustra).{0,40}(acuerdo|alianza)|(acuerdo|alianza).{0,40}(se cae|fracasa|queda sin efecto|se rompe)",
    re.IGNORECASE
)

EXPRESIONES_NEUTRAS = [
    "informa","presenta informe","segun informe","segun estudio","de acuerdo con",
    "participa","asiste","menciona","comenta","cita","segun medios","presenta balance",
    "ranking","evento","foro","conferencia","panel"
]

VERBOS_DECLARATIVOS = [
    "dijo","afirmo","aseguro","segun","indico","apunto","declaro","explico","estimo",
    "segun el informe","segun la entidad","segun analistas","de acuerdo con"
]

MARCADORES_CONDICIONALES = [
    "podria","estaria","habria","al parecer","posible","trascendio","se rumora","seria","serian"
]

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
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.02); }
            100% { transform: scale(1); }
        }
        
        @keyframes slideIn {
            from { transform: translateX(-20px); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        
        /* Header principal */
        .main-header {
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%);
            color: white;
            padding: 2rem;
            border-radius: var(--border-radius);
            text-align: center;
            font-size: 2.5rem;
            font-weight: 800;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow-medium);
            animation: fadeIn 0.5s ease-out;
        }
        
        .subtitle {
            text-align: center;
            color: #666;
            font-size: 1.1rem;
            margin: -1rem 0 2rem 0;
            animation: fadeIn 0.7s ease-out;
        }
        
        /* Cards mejoradas */
        .step-card {
            background: var(--card-bg);
            padding: 1.5rem;
            border-radius: var(--border-radius);
            box-shadow: var(--shadow-light);
            border-left: 4px solid var(--primary-color);
            margin: 1rem 0;
            transition: all 0.3s ease;
            animation: slideIn 0.5s ease-out;
        }
        
        .step-card:hover {
            box-shadow: var(--shadow-medium);
            transform: translateX(5px);
        }
        
        .success-card {
            background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
            padding: 1.5rem;
            border-radius: var(--border-radius);
            border: 1px solid #28a745;
            margin: 1rem 0;
            box-shadow: var(--shadow-light);
            animation: pulse 2s infinite;
        }
        
        .info-card {
            background: linear-gradient(135deg, #d1ecf1 0%, #b8daff 100%);
            padding: 1.5rem;
            border-radius: var(--border-radius);
            border: 1px solid #17a2b8;
            margin: 1rem 0;
            box-shadow: var(--shadow-light);
        }
        
        .warning-card {
            background: linear-gradient(135deg, #fff3cd 0%, #ffeeba 100%);
            padding: 1.5rem;
            border-radius: var(--border-radius);
            border: 1px solid var(--warning-color);
            margin: 1rem 0;
            box-shadow: var(--shadow-light);
        }
        
        /* Métricas mejoradas */
        .metric-card {
            background: var(--card-bg);
            padding: 1.2rem;
            border-radius: var(--border-radius);
            box-shadow: var(--shadow-light);
            text-align: center;
            transition: all 0.3s ease;
            border: 1px solid #e0e0e0;
            animation: fadeIn 0.8s ease-out;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: var(--shadow-medium);
            border-color: var(--primary-color);
        }
        
        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            color: var(--primary-color);
            margin: 0.5rem 0;
        }
        
        .metric-label {
            font-size: 0.9rem;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        /* Progress bar mejorada */
        .stProgress > div > div {
            background: linear-gradient(90deg, var(--primary-color) 0%, var(--secondary-color) 100%);
            border-radius: 10px;
            height: 8px;
        }
        
        /* Status containers */
        .status-container {
            background: #f8f9fa;
            border-radius: var(--border-radius);
            padding: 1rem;
            margin: 1rem 0;
            border: 1px solid #dee2e6;
        }
        
        .status-active {
            border-color: var(--primary-color);
            background: linear-gradient(135deg, #f0f8ff 0%, #e6f3ff 100%);
            animation: pulse 2s infinite;
        }
        
        /* Botones personalizados */
        .stButton > button {
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: var(--shadow-light);
        }
        
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-medium);
        }
        
        /* Tablas mejoradas */
        .dataframe {
            border-radius: var(--border-radius);
            overflow: hidden;
            box-shadow: var(--shadow-light);
        }
        
        /* Inputs mejorados */
        .stTextInput > div > div > input,
        .stTextArea > div > div > textarea {
            border-radius: 8px;
            border: 2px solid #e0e0e0;
            transition: border-color 0.3s ease;
        }
        
        .stTextInput > div > div > input:focus,
        .stTextArea > div > div > textarea:focus {
            border-color: var(--primary-color);
            box-shadow: 0 0 0 2px rgba(31, 119, 180, 0.1);
        }
        
        /* Badges */
        .badge {
            display: inline-block;
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            margin: 0.2rem;
            animation: fadeIn 0.5s ease-out;
        }
        
        .badge-success {
            background: #28a745;
            color: white;
        }
        
        .badge-warning {
            background: var(--warning-color);
            color: white;
        }
        
        .badge-danger {
            background: var(--danger-color);
            color: white;
        }
        
        .badge-info {
            background: #17a2b8;
            color: white;
        }
        
        /* Loading spinner */
        .loading-spinner {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid rgba(31, 119, 180, 0.3);
            border-radius: 50%;
            border-top-color: var(--primary-color);
            animation: spin 1s ease-in-out infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        /* Responsive design */
        @media (max-width: 768px) {
            .main-header {
                font-size: 2rem;
                padding: 1.5rem;
            }
            
            .metric-card {
                padding: 1rem;
            }
            
            .metric-value {
                font-size: 1.5rem;
            }
        }
        
        /* Dark mode support */
        @media (prefers-color-scheme: dark) {
            :root {
                --card-bg: #2b2b2b;
            }
            
            .step-card, .metric-card {
                background: var(--dark-bg);
                color: #e0e0e0;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ======================================
# Cache mejorado para mayor velocidad
# ======================================
@st.cache_data(ttl=3600)
def cached_get_embedding(texto: str) -> Optional[List[float]]:
    return get_embedding(texto)

# ======================================
# Autenticacion con mejor UX
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True
    
    st.markdown('<div class="main-header">🔐 Portal de Acceso Seguro</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Sistema de Análisis de Noticias con Inteligencia Artificial</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.container():
            st.markdown('<div class="info-card">', unsafe_allow_html=True)
            st.markdown("### 🚀 Bienvenido al Sistema")
            st.markdown("""
            Esta plataforma utiliza IA avanzada para:
            - 📊 Análisis automático de tono
            - 🏷️ Clasificación inteligente de temas
            - 🔍 Detección de duplicados
            - 📈 Generación de informes detallados
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="step-card">', unsafe_allow_html=True)
            with st.form("password_form"):
                password = st.text_input(
                    "🔑 Contraseña:",
                    type="password",
                    placeholder="Ingresa tu contraseña de acceso...",
                    help="Contacta al administrador si no tienes acceso"
                )
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    submit = st.form_submit_button(
                        "🚀 Ingresar al Sistema",
                        use_container_width=True,
                        type="primary"
                    )
                with col_btn2:
                    st.form_submit_button(
                        "📧 Solicitar Acceso",
                        use_container_width=True,
                        disabled=True,
                        help="Próximamente"
                    )
                
                if submit:
                    if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                        st.session_state["password_correct"] = True
                        st.success("✅ Acceso autorizado. Redirigiendo...")
                        st.balloons()
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error("❌ Contraseña incorrecta")
                        st.warning("💡 Tip: Verifica que no tengas Caps Lock activado")
            st.markdown('</div>', unsafe_allow_html=True)
    
    return False

# ======================================
# Utilidades generales (mantiene las existentes)
# ======================================
def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3
    delay = 1
    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay *= 2

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 3
    delay = 1
    for attempt in range(max_retries):
        try:
            return await api_func(*args, **kwargs)
        except Exception:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(delay)
            delay *= 2

def norm_key(text: Any) -> str:
    if text is None:
        return ""
    s = unidecode(str(text).strip().lower())
    return re.sub(r"[^a-z0-9]+", "", s)

def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    if tema:
        tema = tema[0].upper() + tema[1:]
    invalid_words = ["en","de","del","la","el","y","o","con","sin","por","para","sobre"]
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_words:
        palabras.pop()
    tema = " ".join(palabras)
    if len(tema.split()) > 6:
        tema = " ".join(tema.split()[:6])
    return tema if tema else "Sin tema"

def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema:
        return "Sin tema"
    tema_lower = tema.lower()
    all_brand_names = [marca.lower()] + [alias.lower() for alias in aliases if alias]
    for brand_name in all_brand_names:
        tema_lower = re.sub(rf'\b{re.escape(brand_name)}\b', '', tema_lower)
        tema_lower = re.sub(rf'\b{re.escape(unidecode(brand_name))}\b', '', tema_lower)
    for ciudad in CIUDADES_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(ciudad)}\b', '', tema_lower)
    for gentilicio in GENTILICIOS_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(gentilicio)}\b', '', tema_lower)
    frases_geograficas = [
        "en colombia", "de colombia", "del pais", "en el pais", "nacional", "colombiano",
        "colombiana", "colombianos", "colombianas", "territorio nacional"
    ]
    for frase in frases_geograficas:
        tema_lower = re.sub(rf'\b{re.escape(frase)}\b', '', tema_lower)
    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
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
    toks = [t for t in s.split() if t not in STOPWORDS_ES]
    return " ".join(toks)

def jaccard(a: str, b: str) -> float:
    ta = set(string_norm_label(a).split())
    tb = set(string_norm_label(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

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
    tmp = re.split(r"\s[-|]\s", title)
    cleaned = tmp[0] if tmp else title
    cleaned = cleaned.strip()
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

# ======================================
# SimHash para near duplicates
# ======================================
def _token_hash(token: str) -> int:
    h = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
    return h & ((1 << 64) - 1)

def simhash(texto: str) -> int:
    if not texto:
        return 0
    toks = string_norm_label(texto).split()
    if not toks:
        return 0
    bits = [0] * 64
    for tok in toks:
        hv = _token_hash(tok)
        for i in range(64):
            bits[i] += 1 if (hv >> i) & 1 else -1
    v = 0
    for i in range(64):
        if bits[i] >= 0:
            v |= (1 << i)
    return v

def hamdist(a: int, b: int) -> int:
    return (a ^ b).bit_count()

# ======================================
# Embeddings con cache
# ======================================
EMBED_CACHE: Dict[str, List[float]] = {}

def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto:
        return None
    key = hashlib.md5(texto[:2000].encode("utf-8")).hexdigest()
    if key in EMBED_CACHE:
        return EMBED_CACHE[key]
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        emb = resp["data"][0]["embedding"]
        EMBED_CACHE[key] = emb
        return emb
    except Exception:
        return None

# ======================================
# Agrupacion de textos por similitud
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    buckets: Dict[int, List[int]] = defaultdict(list)
    sims: List[int] = [simhash(t or "") for t in textos]
    for i, sh in enumerate(sims):
        buckets[sh >> 8].append(i)

    grupos = {}
    gid = 0
    for _, idxs in buckets.items():
        if not idxs:
            continue
        sub = []
        if len(idxs) > 60:
            piv = sims[idxs[0]]
            cerca = [i for i in idxs if hamdist(sims[i], piv) <= 6]
            lejos = [i for i in idxs if hamdist(sims[i], piv) > 6]
            if cerca:
                sub.append(cerca)
            if lejos:
                sub.append(lejos)
        else:
            sub.append(idxs)
        for chunk in sub:
            idx_validos = []
            embs = []
            for i in chunk:
                t = textos[i]
                if not t or not isinstance(t, str):
                    continue
                e = get_embedding(t)
                if e is None:
                    continue
                idx_validos.append(i)
                embs.append(e)
            if not idx_validos:
                continue
            if len(embs) == 1:
                grupos[gid] = [idx_validos[0]]
                gid += 1
                continue
            emb_matrix = np.array(embs)
            clustering = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=1 - umbral_similitud,
                metric="cosine",
                linkage="average"
            ).fit(emb_matrix)
            tmp = defaultdict(list)
            for i, cl in enumerate(clustering.labels_):
                tmp[cl].append(idx_validos[i])
            for cl, members in tmp.items():
                grupos[gid] = members
                gid += 1
    return grupos

# Prefijos de resumen similares (para casos con títulos distintos)
def _tokens_resumen_inicial(texto: str, marca: str, aliases: List[str], max_tokens: int = 24) -> List[str]:
    if not texto:
        return []
    parte_resumen = texto.split(". ", 1)
    base = parte_resumen[1] if len(parte_resumen) > 1 else texto
    s = unidecode(base.lower())
    all_brand_names = [marca] + [a for a in aliases if a]
    for name in all_brand_names:
        if not name:
            continue
        s = re.sub(rf"\b{re.escape(unidecode(name.lower()))}\b", " ", s)
    for w in list(CIUDADES_COLOMBIA) + list(GENTILICIOS_COLOMBIA):
        s = re.sub(rf"\b{re.escape(unidecode(w.lower()))}\b", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    toks = [t for t in s.split() if t and t not in STOPWORDS_ES]
    return toks[:max_tokens]

# Agrupar por inicio de resumen igual/similar
def agrupar_por_prefijo_resumen(textos: List[str], marca: str, aliases: List[str]) -> Dict[int, List[int]]:
    gid = 0
    grupos: Dict[int, List[int]] = {}
    used = set()
    prefixes = [" ".join(_tokens_resumen_inicial(t, marca, aliases, max_tokens=24)) for t in textos]
    hashes = [simhash(p or "") for p in prefixes]
    for i in range(len(textos)):
        if i in used or not prefixes[i]:
            continue
        grupo = [i]
        used.add(i)
        for j in range(i + 1, len(textos)):
            if j in used or not prefixes[j]:
                continue
            if hamdist(hashes[i], hashes[j]) > 10:
                continue
            r = SequenceMatcher(None, prefixes[i], prefixes[j]).ratio()
            if r >= 0.88 or jaccard(prefixes[i], prefixes[j]) >= 0.70:
                grupo.append(j)
                used.add(j)
        if len(grupo) >= 2:
            grupos[gid] = grupo
            gid += 1
    return grupos

# NUEVO: agrupar por resumen puro igual o MUY similar (sin importar título)
def agrupar_por_resumen_puro(resumenes: List[str]) -> Dict[int, List[int]]:
    gid = 0
    grupos: Dict[int, List[int]] = {}
    used = set()
    norm = [string_norm_label(r or "") for r in resumenes]
    hashes = [simhash(r or "") for r in norm]
    # index por igualdad exacta normalizada
    eq_index = defaultdict(list)
    for i, r in enumerate(norm):
        if r:
            eq_index[r].append(i)
    for idxs in eq_index.values():
        if len(idxs) >= 2:
            grupos[gid] = idxs
            for k in idxs:
                used.add(k)
            gid += 1
    # cercanos por similitud
    for i in range(len(norm)):
        if i in used or not norm[i]:
            continue
        grupo = [i]
        used.add(i)
        for j in range(i + 1, len(norm)):
            if j in used or not norm[j]:
                continue
            if hamdist(hashes[i], hashes[j]) <= 8:
                if SequenceMatcher(None, norm[i], norm[j]).ratio() >= 0.92 or jaccard(norm[i], norm[j]) >= 0.75:
                    grupo.append(j)
                    used.add(j)
        if len(grupo) >= 2:
            grupos[gid] = grupo
            gid += 1
    return grupos

# Seleccionar representante
def seleccionar_representante(indices: List[int], textos: List[str]) -> Tuple[int, str]:
    emb_list = []
    valid = []
    for i in indices:
        e = get_embedding(textos[i])
        if e is not None:
            emb_list.append(e)
            valid.append(i)
    if not emb_list:
        i0 = indices[0]
        return i0, textos[i0]
    M = np.array(emb_list)
    centro = M.mean(axis=0, keepdims=True)
    sims = cosine_similarity(M, centro).reshape(-1)
    pos = int(np.argmax(sims))
    idx = valid[pos]
    return idx, textos[idx]

# ======================================
# Analisis de tono (mantiene la lógica existente)
# ======================================
def _alias_to_regex(name: str) -> str:
    s = unidecode((name or "").strip().lower())
    s = re.sub(r"\s+", " ", s)
    if not s:
        return ""
    compact = re.sub(r"[^a-z0-9]", "", s)
    if len(compact) <= 6 and " " not in s:
        parts = list(compact)
        return r"\b" + r"\s*\.?\s*".join(map(re.escape, parts)) + r"\b"
    tokens = [t for t in re.split(r"\s+", s) if t]
    return r"\b" + r"[\s\.-]+".join(map(re.escape, tokens)) + r"\b"

def _build_brand_regex(marca: str, aliases: List[str]) -> str:
    names = [marca] + [a for a in (aliases or []) if a]
    patterns = [_alias_to_regex(n) for n in names if _alias_to_regex(n)]
    for n in names:
        s = unidecode((n or "").strip().lower())
        s2 = re.sub(r"[^a-z0-9]", "", s)
        if s2:
            patterns.append(r"\b" + re.escape(s2) + r"\b")
    patterns = list(dict.fromkeys(patterns))
    return "(" + "|".join(patterns) + ")" if patterns else r"(a^b)"

POS_PATTERNS = [re.compile(rf"\b(?:{p})\b") for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b") for p in NEG_VARIANTS]

def _window_hits_patterns(t: str, brand_re: str, patterns: List[re.Pattern], window: int = WINDOW) -> int:
    hits = 0
    for pat in patterns:
        if re.search(rf"\b{brand_re}\b.{{0,{window}}}{pat.pattern}", t) or re.search(rf"{pat.pattern}.{{0,{window}}}\b{brand_re}\b", t):
            hits += 1
    return hits

def _count_list(t: str, L: List[str]) -> int:
    c = 0
    for x in L:
        c += 1 if re.search(rf"\b{re.escape(x)}\b", t) else 0
    return c

def _normalized_brand_set(marca: str, aliases: List[str]) -> set:
    vals = [marca] + [a for a in aliases if a]
    out = set()
    for v in vals:
        s = unidecode(v or "").strip().lower()
        if s:
            out.add(re.sub(r"[^a-z0-9]", "", s))
    return out

def _mention_matches_brand(mention: str, marca: str, aliases: List[str]) -> bool:
    if not mention:
        return False
    m = unidecode(str(mention)).strip().lower()
    m = re.sub(r"[^a-z0-9]", "", m)
    return m in _normalized_brand_set(marca, aliases)

def analizar_contexto_tono(texto: str, marca: str, aliases: List[str], mention_entity: Optional[str] = None) -> Dict[str, Any]:
    t = unidecode((texto or "").lower())
    brand_re = _build_brand_regex(marca, aliases)

    pos_hits = _window_hits_patterns(t, brand_re, POS_PATTERNS, WINDOW)
    neg_hits = _window_hits_patterns(t, brand_re, NEG_PATTERNS, WINDOW)

    acuerdo_near = bool(
        re.search(rf"\b{brand_re}\b.{{0,{WINDOW}}}{ACUERDO_PATTERNS.pattern}", t)
        or re.search(rf"{ACUERDO_PATTERNS.pattern}.{{0,{WINDOW}}}\b{brand_re}\b", t)
    )
    acuerdo_any = bool(ACUERDO_PATTERNS.search(t)) and not bool(NEG_ACUERDO_PATTERNS.search(t))

    brand_in_text = bool(re.search(rf"\b{brand_re}\b", t))
    mention_is_brand = _mention_matches_brand(mention_entity or "", marca, aliases)

    if acuerdo_near or (acuerdo_any and (brand_in_text or mention_is_brand)):
        pos_hits += 3

    quotes = 1 if re.search(r"\"[^\"]+\"|'[^']+'", texto or "") else 0
    declarativos = _count_list(t, VERBOS_DECLARATIVOS)
    condicionales = _count_list(t, MARCADORES_CONDICIONALES)

    agente_patterns = [
        r"anuncia", r"lanza", r"firma", r"acuerda", r"invierte", r"mejora", r"expande",
        r"inaugura", r"estrena", r"habilita", r"presenta", r"introduce", r"desarrolla",
        r"implementa", r"ofrece", r"brinda", r"proporciona", r"fortalece", r"consolida",
        r"lidera", r"innova", r"crea", r"construye", r"establece", r"promueve",
        r"impulsa", r"apoya", r"colabora", r"participa", r"contribuye", r"genera",
        r"adjudica", r"obtiene", r"gana", r"cierra", r"emite"
    ]
    agente_pattern = r"|".join(agente_patterns)
    agente = 1 if re.search(rf"\b{brand_re}\b.{{0,{WINDOW}}}\b({agente_pattern})\b", t) else 0

    objeto_patterns = [
        r"sanciona", r"multa", r"investiga", r"demanda", r"denuncia", r"boicot",
        r"critica", r"suspende", r"cierra", r"rechaza", r"cancela", r"revoca",
        r"penaliza", r"castiga", r"condena", r"acusa", r"cuestiona", r"impugna",
        r"rompe", r"anula", r"rescinde"
    ]
    objeto_pattern = r"|".join(objeto_patterns)
    objeto = 1 if re.search(rf"\b({objeto_pattern})\b.{{0,{WINDOW}}}\b{brand_re}\b", t) else 0

    referencial = 1 if _count_list(t, EXPRESIONES_NEUTRAS) > 0 and pos_hits == 0 and neg_hits == 0 else 0

    evidencia = pos_hits + neg_hits
    hedging = declarativos + condicionales + quotes

    if evidencia == 0 and referencial:
        mention_type = "Referencial"
    elif condicionales >= 1 and evidencia <= 1:
        mention_type = "Reportada"
    else:
        mention_type = "Factual"

    polarity_score = pos_hits - neg_hits

    if (acuerdo_near or (acuerdo_any and (brand_in_text or mention_is_brand))):
        polarity = "Positivo"
    else:
        if hedging >= 2 and polarity_score > 0:
            polarity_score *= 0.7
        if polarity_score > 0.4:
            polarity = "Positivo"
        elif polarity_score < -0.4:
            polarity = "Negativo"
        else:
            polarity = "Neutro"

    if agente and not objeto and polarity == "Positivo":
        actor = "MarcaAgente"
    elif objeto and not agente and polarity == "Negativo":
        actor = "MarcaObjeto"
    elif agente and objeto:
        actor = "Multiple"
    else:
        actor = "Tercero"

    if pos_hits >= 1 and neg_hits == 0 and mention_type != "Reportada":
        polarity = "Positivo"
        extended_agent_check = re.search(rf"\b{brand_re}\b.{{0,{WINDOW}}}\b({agente_pattern}|{'|'.join(POS_VARIANTS[:10])})", t)
        actor = "MarcaAgente" if (agente or extended_agent_check) else actor
        mention_type = "Factual"

    acuerdo_flag = 2 if (acuerdo_any and (brand_in_text or mention_is_brand)) else (1 if acuerdo_near else 0)

    return {
        "mention_type": mention_type,
        "actor": actor,
        "polarity": polarity,
        "pos_hits": pos_hits,
        "neg_hits": neg_hits,
        "hedging": hedging,
        "acuerdo": acuerdo_flag,
        "mention_is_brand": int(bool(mention_is_brand or brand_in_text))
    }

def decidir_tono(features: Dict[str, Any]) -> Tuple[str, str, str, str]:
    mt = features["mention_type"]
    actor = features["actor"]
    pol = features["polarity"]

    if mt == "Referencial":
        return "Neutro", mt, actor, "Mencion referencial"
    if mt == "Reportada" and pol != "Negativo":
        return "Neutro", mt, actor, "Dato reportado"

    if pol == "Positivo" and actor in ["MarcaAgente", "Multiple", "Tercero"]:
        if features.get("acuerdo", 0) >= 1:
            return "Positivo", mt, actor, "Acuerdo o alianza"
        return "Positivo", mt, actor, "Accion favorable de marca"
    if pol == "Negativo" and actor in ["MarcaObjeto", "Multiple"]:
        return "Negativo", mt, actor, "Hecho adverso"

    return "Neutro", mt, actor, "Mencion informativa"

# ======================================
# Clasificador de Tono con agrupación por RESUMEN PURO
# ======================================
class ClasificadorTonoUltraV2:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []

    async def _llm_refuerzo(self, texto: str) -> Dict[str, str]:
        prompt = (
            "Analice el texto con foco en la marca indicada.\n"
            "Determine mention_type Factual o Reportada o Referencial.\n"
            "Determine actor MarcaAgente o MarcaObjeto o Tercero o Multiple.\n"
            "Determine polarity Positivo o Negativo o Neutro.\n"
            "Considere positivo acuerdos, alianzas, convenios, partnership, joint venture, fusiones.\n"
            "Responda en JSON con estas llaves y 'tono' final y 'justificacion' de maximo 6 palabras.\n"
            f"Marca: {self.marca}\n"
            f"Texto: {texto[:MAX_TOKENS_PROMPT_TXT]}\n"
            '{"mention_type":"Factual|Reportada|Referencial","actor":"MarcaAgente|MarcaObjeto|Tercero|Multiple","polarity":"Positivo|Negativo|Neutro","tono":"Positivo|Negativo|Neutro","justificacion":"..."}'
        )
        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=90,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            tono = str(data.get("tono", "Neutro")).title()
            just = str(data.get("justificacion", "Analisis breve"))
            if len(just.split()) > 6:
                just = " ".join(just.split()[:6])
            return {
                "tono": tono if tono in ["Positivo","Negativo","Neutro"] else "Neutro",
                "mention_type": data.get("mention_type", "Factual"),
                "actor": data.get("actor", "Tercero"),
                "polarity": data.get("polarity", "Neutro"),
                "justificacion": just
            }
        except Exception:
            return {
                "tono": "Neutro",
                "mention_type": "Reportada",
                "actor": "Tercero",
                "polarity": "Neutro",
                "justificacion": "Fallo de refuerzo"
            }

    async def _clasificar_grupo_async(self, cluster_id: Any, texto_representante: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            feats = analizar_contexto_tono(texto_representante, self.marca, self.aliases)
            tono_h, mt_h, actor_h, just_h = decidir_tono(feats)

            if (feats["pos_hits"] + feats["neg_hits"]) == 0 or (tono_h == "Neutro" and feats["hedging"] >= 1):
                ref = await self._llm_refuerzo(texto_representante)
                return cluster_id, {"tono": ref["tono"], "justificacion": ref["justificacion"], "detalle": ref}

            return cluster_id, {"tono": tono_h, "justificacion": just_h, "detalle": feats}

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar, resumen_puro: Optional[pd.Series] = None):
        textos = textos_concat.tolist()
        n = len(textos)
        progress_bar.progress(0.05, text="🔄 Agrupando noticias similares...")

        # DSU para unir: 1) similitud semántica 2) prefijo de resumen 3) resumen puro igual/similar
        class DSU:
            def __init__(self, n):
                self.p = list(range(n))
                self.r = [0]*n
            def find(self, x):
                if self.p[x] != x:
                    self.p[x] = self.find(self.p[x])
                return self.p[x]
            def union(self, a, b):
                ra, rb = self.find(a), self.find(b)
                if ra == rb:
                    return
                if self.r[ra] < self.r[rb]:
                    ra, rb = rb, ra
                self.p[rb] = ra
                if self.r[ra] == self.r[rb]:
                    self.r[ra] += 1

        dsu = DSU(n)
        # 1) grupos semánticos sobre texto concatenado título+resumen
        g_sem = agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO)
        for _, idxs in g_sem.items():
            base = idxs[0]
            for j in idxs[1:]:
                dsu.union(base, j)
        # 2) prefijos de resumen similares
        g_pref = agrupar_por_prefijo_resumen(textos, self.marca, self.aliases)
        for _, idxs in g_pref.items():
            base = idxs[0]
            for j in idxs[1:]:
                dsu.union(base, j)
        # 3) resumen puro igual/similar
        if resumen_puro is not None:
            g_res = agrupar_por_resumen_puro(resumen_puro.astype(str).tolist())
            for _, idxs in g_res.items():
                base = idxs[0]
                for j in idxs[1:]:
                    dsu.union(base, j)

        # componentes finales
        comp: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            comp[dsu.find(i)].append(i)

        representantes = {}
        for cid, idxs in comp.items():
            _, rep_txt = seleccionar_representante(idxs, textos)
            representantes[cid] = rep_txt

        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [self._clasificar_grupo_async(cid, rep_texto, semaphore) for cid, rep_texto in representantes.items()]

        total = len(tasks) if tasks else 1
        resultados_brutos = []
        done = 0
        for f in asyncio.as_completed(tasks):
            res = await f
            done += 1
            progress_bar.progress(0.05 + 0.80 * done / total, text=f"🎯 Analizando tono: {done}/{total}")
            resultados_brutos.append(res)

        resultados_por_grupo = dict(resultados_brutos)
        progress_bar.progress(0.90, text="🔄 Propagando resultados...")

        resultados_finales = [None] * len(textos)
        for cid, idxs in comp.items():
            r = resultados_por_grupo.get(cid, {"tono": "Neutro", "justificacion": "Sin datos"})
            for i in idxs:
                resultados_finales[i] = r

        for i in range(len(textos)):
            if resultados_finales[i] is None:
                _, r = await self._clasificar_grupo_async(f"fb_{i}", textos[i], semaphore)
                resultados_finales[i] = r

        progress_bar.progress(1.0, text="✅ Análisis de tono completado")
        return resultados_finales

# ======================================
# Temas dinamicos con consolidacion y refuerzo por prefijo y RESUMEN PURO
# ======================================
class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))
        self.r = [0]*n
    def find(self, x: int) -> int:
        if self.p[x] != x:
            self.p[x] = self.find(self.p[x])
        return self.p[x]
    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.r[ra] < self.r[rb]:
            ra, rb = rb, ra
        self.p[rb] = ra
        if self.r[ra] == self.r[rb]:
            self.r[ra] += 1

class ClasificadorTemaDinamico:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []

    def _prompt_tema(self, muestras: List[str]) -> str:
        terminos_evitar = [self.marca.lower()] + [alias.lower() for alias in self.aliases if alias]
        terminos_evitar_str = ", ".join(terminos_evitar)
        lista = "\n---\n".join([m[:500] for m in muestras])
        return (
            "Genere un tema específico y preciso de 2 a 6 palabras que describa el contenido principal.\n"
            "INSTRUCCIONES CRITICAS:\n"
            "- NO incluya nombres de ciudades colombianas\n"
            "- NO incluya gentilicios\n"
            "- NO incluya frases geograficas\n"
            f"- NO incluya la marca ni sus alias: {terminos_evitar_str}\n"
            "- Use sustantivos nucleares con modificadores claros\n"
            "- Evite terminos genericos como 'Actualidad' o 'General'\n"
            "- Enfoque en sector, actividad o hecho concreto\n"
            "Ejemplos: 'Servicios Financieros', 'Tecnologia Movil', 'Retail Alimentario'\n"
            f"Textos a analizar:\n{lista}\n"
            'Responda solo en JSON: {"tema":"..."}'
        )

    def _generar_tema_para_grupo(self, textos_muestra: List[str]) -> str:
        prompt = self._prompt_tema(textos_muestra)
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            contenido = resp.choices[0].message.content.strip()
            data = json.loads(contenido)
            tema_crudo = data.get("tema", "Sin tema")
            tema = limpiar_tema_geografico(limpiar_tema(tema_crudo), self.marca, self.aliases)
            if tema.lower() in ["actualidad", "noticias", "general", "varios", "informacion", "información"]:
                palabras = string_norm_label(" ".join(textos_muestra)).split()
                tema = limpiar_tema(" ".join([w for w in palabras[:4]]) or "Sector Empresarial")
            return tema
        except Exception:
            try:
                palabras_contenido = string_norm_label(" ".join(textos_muestra)).split()
                return limpiar_tema(" ".join(palabras_contenido[:4]) or "Actividad Empresarial")
            except Exception:
                return "Sin tema"

    def _construir_componentes(self, textos_concat: List[str], resumen_puro_list: Optional[List[str]], pbar):
        n = len(textos_concat)
        pbar.progress(0.12, "🔍 Analizando similitud semántica...")
        grupos_sem = agrupar_textos_similares(textos_concat, SIMILARITY_THRESHOLD_TEMAS)
        pbar.progress(0.24, "📝 Comparando inicios de resumen...")
        grupos_pref = agrupar_por_prefijo_resumen(textos_concat, self.marca, self.aliases)
        pbar.progress(0.30, "🔗 Uniendo agrupaciones...")
        dsu = DSU(n)
        for g in [grupos_sem, grupos_pref]:
            for _, idxs in g.items():
                if len(idxs) >= 2:
                    base = idxs[0]
                    for j in idxs[1:]:
                        dsu.union(base, j)
        if resumen_puro_list is not None:
            grupos_res = agrupar_por_resumen_puro(resumen_puro_list)
            for _, idxs in grupos_res.items():
                base = idxs[0]
                for j in idxs[1:]:
                    dsu.union(base, j)
        comp: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            comp[dsu.find(i)].append(i)
        return list(comp.values())

    def procesar_lote(self, df_columna_resumen: pd.Series, progress_bar, nombre_cliente: str, resumen_puro: Optional[pd.Series] = None) -> List[str]:
        textos = df_columna_resumen.tolist()
        n = len(textos)
        if n == 0:
            return []

        progress_bar.progress(0.10, "🔍 Preparando agrupaciones temáticas...")
        componentes = self._construir_componentes(textos, resumen_puro.astype(str).tolist() if resumen_puro is not None else None, progress_bar)

        progress_bar.progress(0.42, "🎯 Generando temas inteligentes...")
        mapa_idx_a_tema_inicial: Dict[int, str] = {}
        temas_iniciales_a_indices: Dict[str, List[int]] = defaultdict(list)

        total_comp = len(componentes)
        hechos = 0
        for comp in componentes:
            if len(comp) >= MIN_CLUSTER_SIZE_TEMAS:
                centro_id, _ = seleccionar_representante(comp, textos)
                centro_emb = get_embedding(textos[centro_id])
                emb_pairs = []
                for i in comp:
                    e = get_embedding(textos[i])
                    if e is not None and centro_emb is not None:
                        sim = cosine_similarity([e], [centro_emb]).ravel()[0]
                        emb_pairs.append((sim, i))
                emb_pairs.sort(reverse=True)
                muestra_idx = [j for _, j in emb_pairs[:5]] or comp[:3]
            else:
                muestra_idx = comp[:1]

            tema = self._generar_tema_para_grupo([textos[j] for j in muestra_idx])
            for i in comp:
                mapa_idx_a_tema_inicial[i] = tema
                temas_iniciales_a_indices[tema].append(i)

            hechos += 1
            progress_bar.progress(0.42 + 0.26 * hechos / max(total_comp, 1), f"🏷️ Temas creados: {hechos}/{total_comp}")

        progress_bar.progress(0.70, "🔄 Consolidando etiquetas similares...")
        lista_temas = list(temas_iniciales_a_indices.keys())
        if len(lista_temas) > 1:
            emb_temas = []
            temas_validos = []
            for t in lista_temas:
                e = get_embedding(t)
                if e is not None:
                    emb_temas.append(e)
                    temas_validos.append(t)
            mapa_tema_a_consolidado = {t: t for t in lista_temas}
            if len(temas_validos) >= 2:
                E = np.array(emb_temas)
                S = cosine_similarity(E)
                usados = set()
                for i, ti in enumerate(temas_validos):
                    if ti in usados:
                        continue
                    grupo = [ti]
                    for j, tj in enumerate(temas_validos):
                        if j <= i or tj in usados:
                            continue
                        sim_e = S[i, j]
                        sim_j = jaccard(ti, tj)
                        sim_mix = 0.7 * sim_e + 0.3 * sim_j
                        if sim_mix >= MIN_SIMILITUD_TEMAS_CONSOLIDACION:
                            grupo.append(tj)
                            usados.add(tj)
                    candidato = max(grupo, key=lambda x: (len(string_norm_label(x).split()), len(x)))
                    candidato = limpiar_tema_geografico(candidato, self.marca, self.aliases)
                    for t in grupo:
                        mapa_tema_a_consolidado[t] = candidato
            temas_finales = [mapa_tema_a_consolidado.get(mapa_idx_a_tema_inicial.get(i, ""), "Sin tema") for i in range(n)]
        else:
            temas_finales = [mapa_idx_a_tema_inicial.get(i, "Sin tema") for i in range(n)]

        progress_bar.progress(0.92, "✨ Aplicando limpieza final...")
        temas_finales = [limpiar_tema_geografico(t, self.marca, self.aliases) for t in temas_finales]
        progress_bar.progress(1.0, "✅ Temas identificados")
        return temas_finales

# ======================================
# Duplicados y transformacion de filas
# ======================================
def are_duplicates(r1: Dict[str, Any], r2: Dict[str, Any], k: Dict[str, str], thresh: float = 0.9, days: int = 1) -> bool:
    if norm_key(r1.get(k["menciones"])) != norm_key(r2.get(k["menciones"])):
        return False
    if norm_key(r1.get(k["medio"])) != norm_key(r2.get(k["medio"])):
        return False
    t1 = normalize_title_for_comparison(r1.get(k["titulo"]))
    t2 = normalize_title_for_comparison(r2.get(k["titulo"]))
    try:
        f1 = pd.to_datetime(r1.get(k["fecha"]))
        f2 = pd.to_datetime(r2.get(k["fecha"]))
        if pd.notna(f1) and pd.notna(f2):
            if abs((f1.date() - f2.date()).days) > days:
                return False
    except Exception:
        pass
    if not t1 or not t2:
        return False
    sim_titles = SequenceMatcher(None, t1, t2).ratio()
    if sim_titles >= thresh:
        return True
    rsum1 = str(r1.get(k.get("resumen", "resumen"), "")).lower()
    rsum2 = str(r2.get(k.get("resumen", "resumen"), "")).lower()
    if rsum1 and rsum2 and SequenceMatcher(None, rsum1, rsum2).ratio() >= 0.92:
        return True
    return False

# ======================================
# Lectura de Excel y mapeos
# ======================================
def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str):
        return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {
        "fm": "Radio",
        "am": "Radio",
        "radio": "Radio",
        "aire": "Televisión",
        "cable": "Televisión",
        "tv": "Televisión",
        "television": "Televisión",
        "televisión": "Televisión",
        "senal abierta": "Televisión",
        "señal abierta": "Televisión",
        "diario": "Prensa",
        "prensa": "Prensa",
        "revista": "Prensa",
        "online": "Internet",
        "internet": "Internet",
        "digital": "Internet",
        "web": "Internet"
    }
    return mapping.get(t, tipo_raw)

def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({
        "titulo": norm_key("Titulo"),
        "resumen": norm_key("Resumen - Aclaracion"),
        "menciones": norm_key("Menciones - Empresa"),
        "medio": norm_key("Medio"),
        "fecha": norm_key("Fecha"),
        "link_nota": norm_key("Link Nota"),
        "link_streaming": norm_key("Link (Streaming - Imagen)"),
        "tono": norm_key("Tono"),
        "tema": norm_key("Tema"),
        "tonoai": norm_key("Tono AI"),
        "justificaciontono": norm_key("Justificacion Tono"),
        "tipodemedio": norm_key("Tipo de Medio"),
        "region": norm_key("Region")
    })

    rows = []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row):
            continue
        rows.append({norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)})

    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        if not m_list:
            split_rows.append(base)
        else:
            for m in m_list:
                new = deepcopy(base)
                new[key_map["menciones"]] = m
                split_rows.append(new)

    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})

    for i in range(len(split_rows)):
        if split_rows[i]["is_duplicate"]:
            continue
        for j in range(i + 1, len(split_rows)):
            if not split_rows[j]["is_duplicate"] and are_duplicates(split_rows[i], split_rows[j], key_map):
                split_rows[j]["is_duplicate"] = True

    for row in split_rows:
        if row["is_duplicate"]:
            row.update({
                key_map["tono"]: "Duplicada",
                key_map["tema"]: "-",
                key_map["tonoai"]: "-",
                key_map["justificaciontono"]: "Noticia duplicada."
            })

    return split_rows, key_map

# ======================================
# Reglas de enlaces por tipo de medio
# ======================================
def _empty_link():
    return {"value": "", "url": None}

def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
    tkey = key_map.get("tipodemedio")
    ln_key = key_map.get("link_nota")
    ls_key = key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key):
        return

    tipo = normalizar_tipo_medio(str(row.get(tkey, "")))
    ln = row.get(ln_key) or _empty_link()
    ls = row.get(ls_key) or _empty_link()

    def has_url(x):
        return isinstance(x, dict) and bool(x.get("url"))

    if tipo in ["Radio", "Televisión"]:
        row[ls_key] = _empty_link()
    elif tipo == "Internet":
        row[ln_key], row[ls_key] = ls, ln
    elif tipo == "Prensa":
        if not has_url(ln) and has_url(ls):
            row[ln_key] = ls
        row[ls_key] = _empty_link()

# ======================================
# Salida a Excel
# ======================================
def generate_output_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"

    final_order = [
        "ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region",
        "Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres",
        "CPE","Tier","Audiencia","Tono","Tono AI","Tema","Resumen - Aclaracion",
        "Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","Justificacion Tono"
    ]
    out_sheet.append(final_order)

    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    try:
        out_wb.add_named_style(link_style)
    except Exception:
        pass

    for row_data in all_processed_rows:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(row_data.get(key_map.get("resumen")))
        row_to_append = []
        links_to_add = {}

        for col_idx, header in enumerate(final_order, start=1):
            val = row_data.get(norm_key(header))
            cell_value = ""
            if isinstance(val, dict) and "url" in val:
                cell_value = val.get("value", "Link")
                if val.get("url"):
                    links_to_add[col_idx] = val["url"]
            elif val is not None:
                cell_value = str(val)
            row_to_append.append(cell_value)

        out_sheet.append(row_to_append)
        if links_to_add:
            for col_idx, url in links_to_add.items():
                cell = out_sheet.cell(row=out_sheet.max_row, column=col_idx)
                cell.hyperlink = url
                cell.style = "Hyperlink_Custom"

    output = io.BytesIO()
    out_wb.save(output)
    output.seek(0)
    return output.getvalue()

# ======================================
# Proceso principal asincrono con interfaz mejorada
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases):
    # Guardar tiempo de inicio
    start_time = time.time()
    
    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
        openai.aiosession.set(None)
    except Exception:
        st.error("❌ Error: OPENAI_API_KEY no encontrado en los Secrets.")
        st.stop()

    # Status containers mejorados
    with st.status("📋 **Paso 1/5:** Limpieza y detección de duplicados", expanded=True) as s:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("🔍 Cargando y normalizando datos...")
        with col2:
            st.markdown('<div class="loading-spinner"></div>', unsafe_allow_html=True)
        
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        duplicados_encontrados = sum(1 for row in all_processed_rows if row.get("is_duplicate"))
        
        st.success(f"✅ **{len(all_processed_rows) - duplicados_encontrados}** noticias únicas | **{duplicados_encontrados}** duplicados detectados")
        s.update(label="✅ **Paso 1/5:** Limpieza completada", state="complete")

    with st.status("🗺️ **Paso 2/5:** Aplicando mapeos y normalización", expanded=True) as s:
        st.write("📍 Procesando regiones y medios digitales...")
        
        df_region = pd.read_excel(region_file)
        region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}

        df_internet = pd.read_excel(internet_file)
        internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}

        regiones_aplicadas = 0
        internet_aplicados = 0
        tipo_actualizado = 0
        tipo_normalizado = 0

        for row in all_processed_rows:
            medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()

            if medio_key in region_map:
                row[key_map.get("region", "region")] = region_map[medio_key]
                regiones_aplicadas += 1
            else:
                row[key_map.get("region", "region")] = "N/A"

            if key_map.get("tipodemedio") in row:
                tipo_val_raw = row.get(key_map.get("tipodemedio"))
                tipo_norm = normalizar_tipo_medio(tipo_val_raw)
                if tipo_norm != tipo_val_raw:
                    row[key_map.get("tipodemedio")] = tipo_norm
                    tipo_normalizado += 1

            if medio_key in internet_map:
                row[key_map.get("medio")] = internet_map[medio_key]
                if key_map.get("tipodemedio"):
                    if row.get(key_map.get("tipodemedio")) != "Internet":
                        row[key_map.get("tipodemedio")] = "Internet"
                        tipo_actualizado += 1
                internet_aplicados += 1

            fix_links_by_media_type(row, key_map)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📍 Regiones", regiones_aplicadas)
        with col2:
            st.metric("🌐 Internet", internet_aplicados)
        with col3:
            st.metric("📺 Tipos normalizados", tipo_normalizado)
        with col4:
            st.metric("🔄 Tipos actualizados", tipo_actualizado)
        
        s.update(label="✅ **Paso 2/5:** Mapeos aplicados", state="complete")

    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]
    
    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        titulo_col = key_map["titulo"]
        resumen_col = key_map.get("resumen", "resumen")
        df_temp["resumen_api"] = df_temp[titulo_col].fillna("").astype(str) + ". " + df_temp[resumen_col].fillna("").astype(str)

        with st.status("🎯 **Paso 3/5:** Análisis inteligente de tono", expanded=True) as s:
            col1, col2 = st.columns([2, 1])
            with col1:
                st.write(f"📊 Analizando **{len(rows_to_analyze)}** noticias únicas")
                st.write(f"🏢 **Marca:** {brand_name}")
                if brand_aliases:
                    st.write(f"🔤 **Alias:** {', '.join(brand_aliases)}")
            with col2:
                st.info(f"⚡ Modelo: {OPENAI_MODEL_CLASIFICACION}")
            
            p_bar = st.progress(0)
            clasif = ClasificadorTonoUltraV2(brand_name, brand_aliases)
            resultados_tono = await clasif.procesar_lote_async(
                df_temp["resumen_api"], p_bar, resumen_puro=df_temp[resumen_col]
            )

            menc_col = key_map["menciones"] if key_map.get("menciones") in df_temp.columns else None
            if menc_col:
                for i, txt in enumerate(df_temp["resumen_api"].tolist()):
                    tnorm = unidecode((txt or "").lower())
                    if ACUERDO_PATTERNS.search(tnorm) and not NEG_ACUERDO_PATTERNS.search(tnorm):
                        mention_val = str(df_temp.iloc[i][menc_col]) if menc_col else ""
                        if _mention_matches_brand(mention_val, brand_name, brand_aliases):
                            resultados_tono[i] = {"tono": "Positivo", "justificacion": "Acuerdo o alianza", "detalle": {"rule": "postajuste_acuerdo"}}

            df_temp[key_map["tonoai"]] = [res["tono"] for res in resultados_tono]
            df_temp[key_map["justificaciontono"]] = [res.get("justificacion", "") for res in resultados_tono]

            tonos = [res["tono"] for res in resultados_tono]
            positivos = tonos.count("Positivo")
            negativos = tonos.count("Negativo")
            neutros = tonos.count("Neutro")
            
            st.markdown("### 📈 Resultados del análisis de tono")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value" style="color: #28a745;">🟢 {positivos}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-label">Positivos ({positivos/len(tonos)*100:.1f}%)</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            with col2:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value" style="color: #d62728;">🔴 {negativos}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-label">Negativos ({negativos/len(tonos)*100:.1f}%)</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            with col3:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value" style="color: #666;">⚪ {neutros}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-label">Neutros ({neutros/len(tonos)*100:.1f}%)</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            s.update(label="✅ **Paso 3/5:** Análisis de tono completado", state="complete")

        with st.status("🏷️ **Paso 4/5:** Identificación inteligente de temas", expanded=True) as s:
            st.write("🤖 Aplicando clustering semántico y análisis temático...")
            p_bar = st.progress(0)
            clasificador_temas = ClasificadorTemaDinamico(brand_name, brand_aliases)
            temas = clasificador_temas.procesar_lote(
                df_temp["resumen_api"], p_bar, brand_name, resumen_puro=df_temp[resumen_col]
            )
            df_temp[key_map["tema"]] = temas
            
            temas_unicos = len(set(temas))
            st.success(f"✅ **{temas_unicos}** temas únicos identificados")
            
            temas_frecuentes = Counter(temas).most_common(5)
            st.markdown("### 🏆 Top 5 temas principales")
            for idx, (tema, count) in enumerate(temas_frecuentes, 1):
                badge_class = "badge-success" if idx == 1 else ("badge-info" if idx <= 3 else "badge-warning")
                st.markdown(
                    f'<span class="badge {badge_class}">#{idx}</span> **{tema}**: {count} noticias',
                    unsafe_allow_html=True
                )
            
            s.update(label="✅ **Paso 4/5:** Temas identificados", state="complete")

        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"):
                result = results_map.get(row["original_index"])
                if result:
                    row.update(result)

    with st.status("📊 **Paso 5/5:** Generando informe final", expanded=True) as s:
        st.write("📝 Compilando resultados y generando Excel...")
        st.write("🔗 Preservando hipervínculos originales...")
        
        # Calcular duración del proceso
        end_time = time.time()
        duration = end_time - start_time
        minutes = int(duration // 60)
        seconds = int(duration % 60)
        duration_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        
        st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_IA_{brand_name.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        st.session_state["brand_name"] = brand_name
        st.session_state["total_rows"] = len(all_processed_rows)
        st.session_state["unique_rows"] = len(rows_to_analyze) if rows_to_analyze else 0
        st.session_state["duplicates"] = len(all_processed_rows) - (len(rows_to_analyze) if rows_to_analyze else 0)
        st.session_state["process_duration"] = duration_str
        
        st.success("✅ **Informe generado exitosamente**")
        s.update(label="✅ **Paso 5/5:** Proceso completado", state="complete")

# ======================================
# Interfaz principal mejorada
# ======================================
def main():
    load_custom_css()

    if check_password():
        st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtitle">Análisis avanzado de tono y temas con inteligencia artificial contextual</div>', unsafe_allow_html=True)

        if not st.session_state.get("processing_complete", False):
            # Tabs para mejor organización
            tab1, tab2 = st.tabs(["🚀 Configuración", "📖 Instrucciones"])
            
            with tab1:
                st.markdown("### 📁 Carga de archivos")
                with st.form("input_form"):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        dossier_file = st.file_uploader(
                            "**1. Dossier Principal** (.xlsx)",
                            type=["xlsx"],
                            help="Archivo Excel con las noticias a analizar"
                        )
                        
                        col_files1, col_files2 = st.columns(2)
                        with col_files1:
                            region_file = st.file_uploader(
                                "**2. Mapeo de Región** (.xlsx)",
                                type=["xlsx"],
                                help="Mapeo de medios a regiones geográficas"
                            )
                        with col_files2:
                            internet_file = st.file_uploader(
                                "**3. Mapeo Internet** (.xlsx)",
                                type=["xlsx"],
                                help="Normalización de medios digitales"
                            )
                    
                    with col2:
                        st.markdown('<div class="info-card">', unsafe_allow_html=True)
                        st.markdown("#### 📋 Checklist")
                        st.markdown(
                            f"""
                            {'✅' if dossier_file else '⭕'} Dossier cargado  
                            {'✅' if region_file else '⭕'} Regiones cargado  
                            {'✅' if internet_file else '⭕'} Internet cargado
                            """
                        )
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                    st.markdown("---")
                    st.markdown("### 🏢 Configuración de marca")
                    
                    col_brand1, col_brand2 = st.columns([1, 2])
                    with col_brand1:
                        brand_name = st.text_input(
                            "**Marca Principal**",
                            placeholder="Ej: Banco Popular",
                            help="Nombre principal de la marca a analizar"
                        )
                    with col_brand2:
                        brand_aliases_text = st.text_area(
                            "**Alias, variaciones y voceros** (separados por ;)",
                            placeholder="Ej: Popular;BanPop;CEO Juan Pérez;Presidente Ejecutivo",
                            height=80,
                            help="Incluye variaciones del nombre, siglas y principales voceros"
                        )
                    
                    st.markdown("---")
                    col_submit1, col_submit2, col_submit3 = st.columns([1, 2, 1])
                    with col_submit2:
                        submitted = st.form_submit_button(
                            "🚀 **INICIAR ANÁLISIS COMPLETO**",
                            use_container_width=True,
                            type="primary"
                        )

                    if submitted:
                        missing_items = []
                        if not dossier_file:
                            missing_items.append("Dossier Principal")
                        if not region_file:
                            missing_items.append("Mapeo de Región")
                        if not internet_file:
                            missing_items.append("Mapeo Internet")
                        if not brand_name.strip():
                            missing_items.append("Marca Principal")

                        if missing_items:
                            st.error(f"❌ **Faltan elementos requeridos:** {', '.join(missing_items)}")
                        else:
                            aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()] if brand_aliases_text else []
                            
                            with st.container():
                                st.markdown('<div class="success-card">', unsafe_allow_html=True)
                                st.success("✅ **Configuración validada correctamente**")
                                st.info(f"📊 **Marca:** {brand_name} | **Alias:** {len(aliases)} | **Archivos:** 3 cargados")
                                st.markdown('</div>', unsafe_allow_html=True)
                            
                            asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases))
                            st.rerun()
            
            with tab2:
                st.markdown("""
                ### 📚 Guía de uso
                
                #### 1️⃣ **Archivos requeridos**
                - **Dossier Principal:** Excel con las noticias (columnas: Título, Resumen, Medio, etc.)
                - **Mapeo de Región:** Excel con 2 columnas (Medio → Región)
                - **Mapeo Internet:** Excel con 2 columnas (Medio Digital → Nombre normalizado)
                
                #### 2️⃣ **Configuración de marca**
                - **Marca Principal:** Nombre oficial de la empresa/organización
                - **Alias:** Incluye variaciones, siglas, nombres de voceros principales
                
                #### 3️⃣ **Proceso de análisis**
                El sistema realizará automáticamente:
                - 🔍 Detección y eliminación de duplicados
                - 📍 Mapeo geográfico y normalización de medios
                - 🎯 Análisis de tono con IA (Positivo/Negativo/Neutro)
                - 🏷️ Clasificación temática inteligente
                - 📊 Generación de informe Excel con hipervínculos
                
                #### ⚡ **Tips para mejores resultados**
                - Asegúrate de que los archivos tengan el formato correcto
                - Incluye todos los alias relevantes de la marca
                - El proceso toma aproximadamente 1-3 minutos para 500 noticias
                
                #### 🔒 **Seguridad**
                - Los datos no se almacenan permanentemente
                - Procesamiento seguro con OpenAI API
                - Sesión encriptada y temporal
                """)
                
        else:
            # Pantalla de resultados completados
            st.markdown("## 🎉 Análisis Completado Exitosamente")
            
            # Métricas principales con diseño mejorado
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value">{st.session_state.get("total_rows", 0)}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">📰 Total Noticias</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value" style="color: #28a745;">{st.session_state.get("unique_rows", 0)}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">✅ Únicas Procesadas</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value" style="color: #ff7f0e;">{st.session_state.get("duplicates", 0)}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">🔄 Duplicados</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col4:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value" style="color: #1f77b4;">100%</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">✨ Completado</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("---")
            
            # Sección de descarga mejorada
            st.markdown('<div class="success-card">', unsafe_allow_html=True)
            col_info, col_actions = st.columns([3, 2])
            
            with col_info:
                st.markdown("### 📊 Informe generado")
                st.markdown(f"""
                **Marca analizada:** {st.session_state.get('brand_name', 'N/A')}  
                **Archivo:** {st.session_state.get('output_filename', 'informe.xlsx')}  
                **Generado:** {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}  
                **Duración del proceso:** ⏱️ {st.session_state.get('process_duration', 'N/A')}
                """)
            
            with col_actions:
                st.markdown("### 🎯 Acciones disponibles")
                st.download_button(
                    "📥 **DESCARGAR INFORME**",
                    data=st.session_state["output_data"],
                    file_name=st.session_state["output_filename"],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
                
                if st.button("🔄 **Nuevo Análisis**", use_container_width=True):
                    password_correct = st.session_state.get("password_correct")
                    st.session_state.clear()
                    st.session_state.password_correct = password_correct
                    st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)

            # Detalles técnicos expandibles
            with st.expander("🔧 **Detalles Técnicos del Proceso**"):
                col_tech1, col_tech2 = st.columns(2)
                
                with col_tech1:
                    st.markdown("""
                    #### ⚙️ Configuración utilizada
                    - **Modelo de Clasificación:** `{}`
                    - **Modelo de Embeddings:** `{}`
                    - **Umbral Similitud Tono:** `{}`
                    - **Umbral Similitud Temas:** `{}`
                    """.format(
                        OPENAI_MODEL_CLASIFICACION,
                        OPENAI_MODEL_EMBEDDING,
                        SIMILARITY_THRESHOLD_TONO,
                        SIMILARITY_THRESHOLD_TEMAS
                    ))
                
                with col_tech2:
                    st.markdown("""
                    #### 📊 Proceso ejecutado
                    1. ✅ Normalización y deduplicación
                    2. ✅ Mapeos geográficos y de medios
                    3. ✅ Análisis de tono con IA
                    4. ✅ Clasificación temática
                    5. ✅ Generación de informe Excel
                    """)
                
                st.info(f"""
                **Solicitudes concurrentes:** {CONCURRENT_REQUESTS}  
                **Ventana contextual:** {WINDOW} caracteres  
                **Timestamp:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                """)
            
            # Sección de ayuda
            with st.expander("❓ **Preguntas Frecuentes**"):
                st.markdown("""
                **¿Cómo interpreto los resultados de tono?**
                - 🟢 **Positivo:** Noticias favorables, acuerdos, logros, expansiones
                - 🔴 **Negativo:** Problemas, crisis, demandas, críticas
                - ⚪ **Neutro:** Menciones informativas sin valoración
                
                **¿Qué significa la justificación del tono?**
                Es una explicación breve generada por IA sobre por qué se asignó ese tono específico.
                
                **¿Puedo reprocesar el mismo archivo?**
                Sí, simplemente haz clic en "Nuevo Análisis" y vuelve a cargar los archivos.
                
                **¿Los hipervínculos se mantienen?**
                Sí, todos los enlaces del archivo original se preservan en el informe final.
                """)

        # Footer mejorado
        st.markdown("---")
        col_footer1, col_footer2, col_footer3 = st.columns([1, 2, 1])
        with col_footer2:
            st.markdown(
                """
                <div style="text-align: center; color: #666; font-size: 0.9rem;">
                    <p>Sistema de Análisis de Noticias v2.0 | Realizado por Johnathan Cortés</p>
                    <p>© 2025 - Todos los derechos reservados</p>
                </div>
                """,
                unsafe_allow_html=True
            )

if __name__ == "__main__":
    main()
