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
    r"compromiso|apoya|apoyar",
    r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)",
    r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)",
    r"destaca(r|do|da|ndo)?",
    r"supera(r|ndo|cion)?",
    r"record|hito|milestone",
    r"avanza(r|do|da|ndo)?",
    r"benefici(a|o|ando|ar|ando)",
    r"importante(s)?",
    r"prioridad",
    r"bienestar",
    r"garantizar",
    r"seguridad",
    r"atencion",
    r"expres(o|ó|ando)",
    r"señala(r|do|ando)",
    r"ratific(a|o|ando|ar)"
]

NEG_VARIANTS = [
    r"demanda|denuncia|sanciona|multa|investiga|critica",
    r"cae|baja|pierde|crisis|quiebra|default",
    r"fraude|escandalo|irregularidad",
    r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga",
    r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora",
    r"problema(s|tica|tico)?|dificultad(es)?",
    r"retras(o|a|ar|ado)",
    r"perdida(s)?|deficit",
    r"conflict(o|os)?|disputa(s)?",
    r"rechaz(a|o|ar|ado)",
    r"negativ(o|a|os|as)",
    r"preocupa(cion|nte|do)?",
    r"alarma(nte)?|alerta",
    r"riesgo(s)?|amenaza(s)?"
]

# LÉXICOS PARA CONTEXTO DE CRISIS
CRISIS_KEYWORDS = re.compile(r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS = re.compile(r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)

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
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)
            delay *= 2

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 3
    delay = 1
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
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
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
# Agrupacion de textos por similitud (mejorada para precisión)
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

def agrupar_por_titulo_similar(titulos: List[str]) -> Dict[int, List[int]]:
    gid = 0
    grupos: Dict[int, List[int]] = {}
    used = set()
    norm_titles = [normalize_title_for_comparison(t) for t in titulos]

    exact_map = defaultdict(list)
    for i, t in enumerate(norm_titles):
        if t:
            exact_map[t].append(i)

    for _, idxs in exact_map.items():
        if len(idxs) >= 2:
            grupos[gid] = idxs
            for i in idxs:
                used.add(i)
            gid += 1

    for i in range(len(norm_titles)):
        if i in used or not norm_titles[i]:
            continue
        grupo_actual = [i]
        used.add(i)
        norm_i = norm_titles[i]
        
        for j in range(i + 1, len(norm_titles)):
            if j in used or not norm_titles[j]:
                continue
            norm_j = norm_titles[j]
            
            similitud = SequenceMatcher(None, norm_i, norm_j).ratio()
            
            es_prefijo = False
            shorter, longer = (norm_i, norm_j) if len(norm_i) < len(norm_j) else (norm_j, norm_i)
            if longer.startswith(shorter) and len(shorter.split()) >= 5:
                es_prefijo = True

            if similitud >= SIMILARITY_THRESHOLD_TITULOS or es_prefijo:
                grupo_actual.append(j)
                used.add(j)
        
        if len(grupo_actual) >= 2:
            grupos[gid] = grupo_actual
            gid += 1
            
    return grupos

def agrupar_por_resumen_puro(resumenes: List[str]) -> Dict[int, List[int]]:
    gid = 0
    grupos: Dict[int, List[int]] = {}
    used = set()
    norm = [string_norm_label(r or "") for r in resumenes]
    hashes = [simhash(r or "") for r in norm]

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
# Análisis de tono centrado en la marca
# ======================================
def _extract_brand_sentences(texto: str, brand_re: str, max_sentences: int = 3) -> List[str]:
    """Extrae las oraciones que contienen la marca"""
    sentences = re.split(r'[.!?]+', texto)
    brand_sentences = []
    for sent in sentences:
        if re.search(rf"\b{brand_re}\b", sent, re.IGNORECASE):
            brand_sentences.append(sent.strip())
            if len(brand_sentences) >= max_sentences:
                break
    return brand_sentences

def _analyze_sentiment_direction(sentence: str, brand_re: str, pos_patterns: List[re.Pattern], neg_patterns: List[re.Pattern]) -> Dict[str, int]:
    """Analiza si el sentimiento está dirigido a la marca o es sobre otro tema"""
    result = {"directed_pos": 0, "directed_neg": 0, "general_pos": 0, "general_neg": 0}
    
    pos_found = sum(1 for p in pos_patterns if p.search(sentence))
    neg_found = sum(1 for p in neg_patterns if p.search(sentence))
    
    brand_match = re.search(rf"\b{brand_re}\b", sentence, re.IGNORECASE)
    if brand_match:
        brand_pos = brand_match.start()
        
        for pat in pos_patterns:
            for match in pat.finditer(sentence):
                if abs(match.start() - brand_pos) < 50:
                    result["directed_pos"] += 1
                else:
                    result["general_pos"] += 1
                    
        for pat in neg_patterns:
            for match in pat.finditer(sentence):
                if abs(match.start() - brand_pos) < 50:
                    result["directed_neg"] += 1
                else:
                    result["general_neg"] += 1
    else:
        result["general_pos"] = pos_found
        result["general_neg"] = neg_found
    
    return result

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

def _window_hits_patterns(t: str, brand_re: str, patterns: List[re.Pattern], window: int = WINDOW) -> int:
    hits = 0
    for pat in patterns:
        if re.search(rf"\b{brand_re}\b.{{0,{window}}}{pat.pattern}", t, re.IGNORECASE) or re.search(rf"{pat.pattern}.{{0,{window}}}\b{brand_re}\b", t, re.IGNORECASE):
            hits += 1
    return hits

def _count_list(t: str, L: List[str]) -> int:
    c = 0
    for x in L:
        c += 1 if re.search(rf"\b{re.escape(x)}\b", t, re.IGNORECASE) else 0
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

    brand_sentences = _extract_brand_sentences(t, brand_re)
    
    directed_pos, directed_neg, general_pos, general_neg = 0, 0, 0, 0
    for sent in brand_sentences[:3]:
        analysis = _analyze_sentiment_direction(sent, brand_re, POS_PATTERNS, NEG_PATTERNS)
        directed_pos += analysis["directed_pos"]
        directed_neg += analysis["directed_neg"]
        general_pos += analysis["general_pos"]
        general_neg += analysis["general_neg"]
    
    window_pos_hits = _window_hits_patterns(t, brand_re, POS_PATTERNS, WINDOW)
    window_neg_hits = _window_hits_patterns(t, brand_re, NEG_PATTERNS, WINDOW)
    
    pos_hits = directed_pos * 2 + window_pos_hits
    neg_hits = directed_neg * 2 + window_neg_hits

    if re.search(r"\b(segun|de acuerdo con|basado en)\s+(un\s+)?(estudio|informe|analisis|investigacion|ranking)\b", t, re.IGNORECASE):
        pos_hits += 2
    if re.search(rf"\b{brand_re}\b.{{0,30}}\b(gan[oaó]|destac[aoó]|primer|lider|mejor|top|record|hito)", t, re.IGNORECASE):
        pos_hits += 3

    brand_in_text = bool(re.search(rf"\b{brand_re}\b", t, re.IGNORECASE))
    mention_is_brand = _mention_matches_brand(mention_entity or "", marca, aliases)
    
    acuerdo_any = bool(ACUERDO_PATTERNS.search(t)) and not bool(NEG_ACUERDO_PATTERNS.search(t))
    acuerdo_near = bool(re.search(rf"\b{brand_re}\b.{{0,{WINDOW}}}{ACUERDO_PATTERNS.pattern}", t, re.IGNORECASE) or re.search(rf"{ACUERDO_PATTERNS.pattern}.{{0,{WINDOW}}}\b{brand_re}\b", t, re.IGNORECASE))
    if acuerdo_near or (acuerdo_any and (brand_in_text or mention_is_brand)):
        pos_hits += 3

    is_crisis_context = bool(CRISIS_KEYWORDS.search(t))
    is_brand_responding = bool(re.search(rf"\b{brand_re}\b.{{0,50}}{RESPONSE_VERBS.pattern}", t, re.IGNORECASE))
    is_crisis_response = is_crisis_context and is_brand_responding
    
    if is_crisis_response:
        pos_hits += 2 
        neg_hits = max(0, neg_hits - 1)

    quotes = 1 if re.search(r"\"[^\"]+\"|'[^']+'", texto or "") else 0
    declarativos = _count_list(t, VERBOS_DECLARATIVOS)
    condicionales = _count_list(t, MARCADORES_CONDICIONALES)

    agente_patterns = [r"anuncia", r"lanza", r"firma", r"acuerda", r"invierte", r"mejora", r"expande", r"inaugura", r"estrena", r"habilita", r"presenta", r"introduce", r"desarrolla", r"implementa", r"ofrece", r"brinda", r"proporciona", r"fortalece", r"consolida", r"lidera", r"innova", r"crea", r"construye", r"establece", r"promueve", r"impulsa", r"apoya", r"colabora", r"participa", r"contribuye", r"genera", r"adjudica", r"obtiene", r"gana", r"cierra", r"emite", r"recibe", r"celebra"]
    agente_pattern = r"|".join(agente_patterns)
    agente = 1 if re.search(rf"\b{brand_re}\b.{{0,{WINDOW}}}\b({agente_pattern})\b", t, re.IGNORECASE) else 0

    objeto_patterns = [r"sanciona", r"multa", r"investiga", r"demanda", r"denuncia", r"boicot", r"critica", r"suspende", r"cierra", r"rechaza", r"cancela", r"revoca", r"penaliza", r"castiga", r"condena", r"acusa", r"cuestiona", r"impugna", r"rompe", r"anula", r"rescinde"]
    objeto_pattern = r"|".join(objeto_patterns)
    objeto = 1 if re.search(rf"\b({objeto_pattern})\b.{{0,{WINDOW}}}\b{brand_re}\b", t, re.IGNORECASE) else 0

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
    if general_pos + general_neg > directed_pos + directed_neg * 2:
        polarity_score *= 0.5

    if acuerdo_near or (acuerdo_any and (brand_in_text or mention_is_brand)):
        polarity = "Positivo"
    else:
        if hedging >= 2 and polarity_score > 0:
            polarity_score *= 0.7
        if polarity_score > 0.3:
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
        extended_agent_check = re.search(rf"\b{brand_re}\b.{{0,{WINDOW}}}\b({agente_pattern}|{'|'.join(POS_VARIANTS[:10])})", t, re.IGNORECASE)
        actor = "MarcaAgente" if (agente or extended_agent_check) else actor
        mention_type = "Factual"

    return {
        "mention_type": mention_type,
        "actor": actor,
        "polarity": polarity,
        "pos_hits": pos_hits,
        "neg_hits": neg_hits,
        "directed_pos": directed_pos,
        "directed_neg": directed_neg,
        "hedging": hedging,
        "acuerdo": 2 if (acuerdo_any and (brand_in_text or mention_is_brand)) else (1 if acuerdo_near else 0),
        "mention_is_brand": int(bool(mention_is_brand or brand_in_text)),
        "is_crisis_response": is_crisis_response
    }

def decidir_tono(features: Dict[str, Any]) -> Tuple[str, str, str, str]:
    mt = features["mention_type"]
    actor = features.get("actor", "Tercero")
    pol = features["polarity"]
    
    if features.get("is_crisis_response"):
        return "Positivo", "Factual", "MarcaAgente", "Respuesta activa a crisis"

    directed_score = features.get("directed_pos", 0) - features.get("directed_neg", 0)
    
    if mt == "Referencial":
        return "Neutro", mt, actor, "Mencion referencial"
    if mt == "Reportada" and pol != "Negativo":
        return "Neutro", mt, actor, "Dato reportado"

    if directed_score > 0:
        return "Positivo", mt, actor, "Sentimiento positivo hacia marca"
    elif directed_score < -1:
        return "Negativo", mt, actor, "Sentimiento negativo hacia marca"

    if pol == "Positivo" and actor in ["MarcaAgente", "Multiple", "Tercero"]:
        if features.get("acuerdo", 0) >= 1:
            return "Positivo", mt, actor, "Acuerdo o alianza"
        if features.get("pos_hits", 0) > features.get("neg_hits", 0):
            return "Positivo", mt, actor, "Accion favorable"
        return "Positivo", mt, actor, "Mencion positiva"
    if pol == "Negativo" and actor in ["MarcaObjeto", "Multiple"]:
        return "Negativo", mt, actor, "Hecho adverso"

    return "Neutro", mt, actor, "Mencion informativa"


# ======================================
# Clasificador de Tono con IA y PKL
# ======================================
class ClasificadorTonoUltraV2:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []

    async def _llm_refuerzo(self, texto: str) -> Dict[str, str]:
        aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
        prompt = (
            "Analice ÚNICAMENTE el sentimiento hacia la marca específica mencionada, NO el sentimiento general del texto.\n"
            "Ignore sentimientos sobre otros temas o entidades.\n"
            "Determine mention_type: Factual, Reportada o Referencial.\n"
            "Determine actor: MarcaAgente, MarcaObjeto, Tercero o Multiple.\n"
            "Determine polarity basado SOLO en sentimiento hacia la marca: Positivo, Negativo o Neutro.\n"
            "Considere positivo: acuerdos, alianzas, premios, reconocimientos, y la acción proactiva de un vocero respondiendo a una crisis.\n"
            f"Marca: {self.marca}\n"
            f"Aliases/voceros: {aliases_str}\n"
            "Responda en JSON con estas llaves y 'tono' final y 'justificacion' de máximo 6 palabras.\n"
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

            if feats.get("is_crisis_response"):
                return cluster_id, {"tono": "Positivo", "justificacion": "Respuesta activa a crisis", "detalle": feats}

            if feats.get("directed_pos", 0) == 0 and feats.get("directed_neg", 0) == 0:
                if (feats["pos_hits"] + feats["neg_hits"]) <= 1 or (tono_h == "Neutro" and feats["hedging"] >= 1):
                    ref = await self._llm_refuerzo(texto_representante)
                    return cluster_id, {"tono": ref["tono"], "justificacion": ref["justificacion"], "detalle": ref}

            return cluster_id, {"tono": tono_h, "justificacion": just_h, "detalle": feats}

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar, resumen_puro: Optional[pd.Series] = None, titulos_puros: Optional[pd.Series] = None):
        textos = textos_concat.tolist()
        n = len(textos)
        progress_bar.progress(0.05, text="🔄 Agrupando noticias similares para tono...")

        class DSU_local:
            def __init__(self, n):
                self.p = list(range(n))
                self.r = [0]*n
            def find(self, x):
                if self.p[x] != x:
                    self.p[x] = self.find(self.p[x])
                return self.p[x]
            def union(self, a, b):
                ra, rb = self.find(a), self.find(b)
                if ra == rb: return
                if self.r[ra] < self.r[rb]: ra, rb = rb, ra
                self.p[rb] = ra
                if self.r[ra] == self.r[rb]: self.r[ra] += 1

        dsu = DSU_local(n)
        
        for g in [
            agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO),
            agrupar_por_prefijo_resumen(textos, self.marca, self.aliases),
            agrupar_por_resumen_puro(resumen_puro.astype(str).tolist()) if resumen_puro is not None else {},
            agrupar_por_titulo_similar(titulos_puros.astype(str).tolist()) if titulos_puros is not None else {}
        ]:
            for _, idxs in g.items():
                base = idxs[0]
                for j in idxs[1:]:
                    dsu.union(base, j)

        comp: Dict[int, List[int]] = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)

        representantes = {cid: seleccionar_representante(idxs, textos)[1] for cid, idxs in comp.items()}
        
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [self._clasificar_grupo_async(cid, rep_texto, semaphore) for cid, rep_texto in representantes.items()]

        resultados_brutos = []
        done = 0
        total = len(tasks) or 1
        for f in asyncio.as_completed(tasks):
            res = await f
            done += 1
            progress_bar.progress(0.05 + 0.80 * done / total, text=f"🎯 Analizando tono: {done}/{total}")
            resultados_brutos.append(res)

        resultados_por_grupo = dict(resultados_brutos)
        progress_bar.progress(0.90, text="🔄 Propagando resultados...")

        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            r = resultados_por_grupo.get(cid, {"tono": "Neutro", "justificacion": "Sin datos"})
            for i in idxs:
                resultados_finales[i] = r

        for i in range(n):
            if resultados_finales[i] is None:
                _, r = await self._clasificar_grupo_async(f"fb_{i}", textos[i], semaphore)
                resultados_finales[i] = r

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
        if ra == rb: return
        if self.r[ra] < self.r[rb]: ra, rb = rb, ra
        self.p[rb] = ra
        if self.r[ra] == self.r[rb]: self.r[ra] += 1

class ClasificadorTemaDinamico:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []

    def _prompt_subtema(self, muestras: List[str]) -> str:
        terminos_evitar = [self.marca.lower()] + [alias.lower() for alias in self.aliases if alias]
        lista = "\n---\n".join([m[:500] for m in muestras])
        return (
            "Genere un subtema específico y preciso de 2 a 6 palabras que describa el contenido principal COMÚN entre los textos.\n"
            "Asegúrese de que subtemas para noticias similares sean idénticos o muy cercanos, prefiriendo términos generales si aplicable para consistencia.\n"
            "INSTRUCCIONES CRITICAS:\n"
            "- Priorice subtemas coherentes y unificados, evitando variaciones menores.\n"
            "- NO incluya nombres de ciudades colombianas, gentilicios ni frases geograficas.\n"
            f"- NO incluya la marca ni sus alias: {', '.join(terminos_evitar)}\n"
            "- Use sustantivos nucleares. Evite terminos genericos como 'noticias' o 'actualidad'.\n"
            "- Si los textos son similares, genere un subtema más general pero preciso para agruparlos.\n"
            f"Textos a analizar:\n{lista}\n"
            'Responda solo en JSON: {"subtema":"..."}'
        )

    def _generar_subtema_para_grupo(self, textos_muestra: List[str]) -> str:
        prompt = self._prompt_subtema(textos_muestra)
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}], max_tokens=40,
                temperature=0.05,  # Reducida para más consistencia
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            subtema = limpiar_tema_geografico(limpiar_tema(data.get("subtema", "Sin tema")), self.marca, self.aliases)
            if subtema.lower() in ["actualidad", "noticias", "general", "varios", "informacion"]:
                return limpiar_tema(" ".join(string_norm_label(" ".join(textos_muestra)).split()[:4]) or "Sector Empresarial")
            return subtema
        except Exception:
            return limpiar_tema(" ".join(string_norm_label(" ".join(textos_muestra)).split()[:4]) or "Actividad Empresarial")

    def _construir_componentes(self, textos_concat: List[str], resumen_puro_list: Optional[List[str]], titulos_puros_list: Optional[List[str]], pbar):
        n = len(textos_concat)
        pbar.progress(0.12, "🔍 Analizando similitud semántica...")
        dsu = DSU(n)
        grupos_list = [
            agrupar_textos_similares(textos_concat, SIMILARITY_THRESHOLD_TEMAS),
            agrupar_por_prefijo_resumen(textos_concat, self.marca, self.aliases),
            agrupar_por_resumen_puro(resumen_puro_list) if resumen_puro_list else {},
            agrupar_por_titulo_similar(titulos_puros_list) if titulos_puros_list else {}
        ]
        pbar.progress(0.30, "🔗 Uniendo agrupaciones...")
        for g in grupos_list:
            for _, idxs in g.items():
                if len(idxs) >= 2:
                    base = idxs[0]
                    for j in idxs[1:]:
                        dsu.union(base, j)
        comp: Dict[int, List[int]] = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        return list(comp.values())

    def procesar_lote(self, df_columna_resumen: pd.Series, progress_bar, nombre_cliente: str, resumen_puro: Optional[pd.Series] = None, titulos_puros: Optional[pd.Series] = None) -> List[str]:
        textos = df_columna_resumen.tolist()
        n = len(textos)
        if n == 0: return []

        progress_bar.progress(0.10, "🔍 Preparando agrupaciones temáticas para subtemas...")
        componentes = self._construir_componentes(textos, resumen_puro.astype(str).tolist() if resumen_puro is not None else None, titulos_puros.astype(str).tolist() if titulos_puros is not None else None, progress_bar)

        progress_bar.progress(0.42, "🎯 Generando subtemas inteligentes...")
        mapa_idx_a_subtema_inicial: Dict[int, str] = {}
        total_comp = len(componentes)
        for hechos, comp in enumerate(componentes, 1):
            muestra_idx = comp[:5]
            if len(comp) >= MIN_CLUSTER_SIZE_TEMAS:
                centro_id, _ = seleccionar_representante(comp, textos)
                centro_emb = get_embedding(textos[centro_id])
                if centro_emb:
                    emb_pairs = [(cosine_similarity([get_embedding(textos[i])], [centro_emb]).ravel()[0], i) for i in comp if get_embedding(textos[i])]
                    emb_pairs.sort(reverse=True)
                    muestra_idx = [j for _, j in emb_pairs[:5]] or comp[:5]
            
            subtema = self._generar_subtema_para_grupo([textos[j] for j in muestra_idx])
            for i in comp: mapa_idx_a_subtema_inicial[i] = subtema
            progress_bar.progress(0.42 + 0.26 * hechos / max(total_comp, 1), f"🏷️ Subtemas creados: {hechos}/{total_comp}")

        # No se realiza consolidación inicial aquí, ya que se hará en `consolidar_subtemas_en_temas`
        subtemas_finales = [mapa_idx_a_subtema_inicial.get(i, "Sin tema") for i in range(n)]
        subtemas_finales = [limpiar_tema_geografico(t, self.marca, self.aliases) for t in subtemas_finales]
        progress_bar.progress(1.0, "✅ Subtemas identificados")
        return subtemas_finales

def consolidar_subtemas_en_temas(subtemas: List[str], p_bar) -> List[str]:
    """Agrupa subtemas en un número fijo de temas principales usando IA."""
    p_bar.progress(0.1, text=f"📊 Consolidando {len(set(subtemas))} subtemas en {NUM_TEMAS_PRINCIPALES} temas principales...")
    
    mapa_subtema_a_tema = {}
    subtemas_unicos = list(set(s for s in subtemas if s != "Sin tema"))

    if not subtemas_unicos or len(subtemas_unicos) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, text="ℹ️ No se requiere consolidación de temas principales.")
        return subtemas # Devuelve los subtemas como temas si no hay suficientes para agrupar

    emb_subtemas = {st: get_embedding(st) for st in subtemas_unicos}
    subtemas_validos = [st for st, emb in emb_subtemas.items() if emb is not None]
    
    if len(subtemas_validos) < NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, text="ℹ️ No hay suficientes subtemas válidos para consolidar.")
        return subtemas

    emb_matrix = np.array([emb_subtemas[st] for st in subtemas_validos])
    
    p_bar.progress(0.4, text="🔄 Realizando clustering de subtemas para consolidar...")
    clustering = AgglomerativeClustering(n_clusters=NUM_TEMAS_PRINCIPALES, metric="cosine", linkage="average").fit(emb_matrix)
    
    mapa_cluster_a_subtemas = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        mapa_cluster_a_subtemas[label].append(subtemas_validos[i])

    p_bar.progress(0.6, text="🧠 Generando nombres para los temas principales (GPT-4.1-nano)...")
    for cluster_id, lista_subtemas in mapa_cluster_a_subtemas.items():
        muestra = lista_subtemas[:10] # Tomar hasta 10 subtemas como muestra para el prompt
        prompt = (
            "Eres un experto en categorización de noticias. Dada la siguiente lista de subtemas específicos, genera un nombre de TEMA principal, corto y conciso (2-4 palabras) que los agrupe lógicamente.\n"
            f"Lista de Subtemas: {', '.join(muestra)}\n"
            "Responde solo con el nombre del tema principal, sin explicaciones."
        )
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=20,
                temperature=0.1
            )
            tema_principal = resp.choices[0].message.content.strip().replace('"', '')
            tema_principal = limpiar_tema(tema_principal)
            for subtema in lista_subtemas:
                mapa_subtema_a_tema[subtema] = tema_principal
        except Exception:
            # Si falla la API, usar el subtema más largo como nombre del tema
            tema_principal = max(lista_subtemas, key=len)
            for subtema in lista_subtemas:
                mapa_subtema_a_tema[subtema] = tema_principal

    mapa_subtema_a_tema["Sin tema"] = "Sin tema"
    p_bar.progress(0.9, text="✅ Nombres de temas principales generados.")
    
    temas_finales = [mapa_subtema_a_tema.get(st, st) for st in subtemas]
    p_bar.progress(1.0, text="✅ Consolidación de temas completada.")
    return temas_finales

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
# Duplicados y Generación de Excel
# ======================================
def are_duplicates(r1: Dict[str, Any], r2: Dict[str, Any], k: Dict[str, str], thresh: float = 0.9, days: int = 1) -> Tuple[bool, str]:
    if norm_key(r1.get(k["menciones"])) != norm_key(r2.get(k["menciones"])) or norm_key(r1.get(k["medio"])) != norm_key(r2.get(k["medio"])):
        return False, ""

    tipo_medio = normalizar_tipo_medio(str(r1.get(k.get("tipodemedio"), "")))
    if tipo_medio.lower() in ["prensa", "diario", "revista", "revistas"]:
        return False, ""
    
    t1_orig, t2_orig = str(r1.get(k["titulo"])), str(r2.get(k["titulo"]))
    t1, t2 = normalize_title_for_comparison(t1_orig), normalize_title_for_comparison(t2_orig)
    
    try:
        f1, f2 = pd.to_datetime(r1.get(k["fecha"])), pd.to_datetime(r2.get(k["fecha"]))
        if pd.notna(f1) and pd.notna(f2) and abs((f1.date() - f2.date()).days) > days:
            return False, ""
    except Exception: pass
    
    if not t1 or not t2: return False, ""
    
    def get_winner(len1, len2): return "first" if len1 > len2 else "second"

    if tipo_medio.lower() in ["internet", "online"]:
        url1 = (r1.get(k.get("link_nota")) or {}).get("url")
        url2 = (r2.get(k.get("link_nota")) or {}).get("url")
        if url1 and url1 == url2:
            return True, get_winner(len(t1_orig), len(t2_orig))
    
    if tipo_medio.lower() in ["radio", "televisión", "television", "aire", "cable", "am", "fm"]:
        if str(r1.get(k.get("hora"))) != str(r2.get(k.get("hora"))): return False, ""
        if SequenceMatcher(None, t1, t2).ratio() >= 1.0:
            return True, get_winner(len(t1_orig), len(t2_orig))
        return False, ""
    
    if SequenceMatcher(None, t1, t2).ratio() >= thresh:
        return True, get_winner(len(t1_orig), len(t2_orig))
    
    rsum1, rsum2 = str(r1.get(k.get("resumen"), "")).lower(), str(r2.get(k.get("resumen"), "")).lower()
    if rsum1 and rsum2 and SequenceMatcher(None, rsum1, rsum2).ratio() >= 0.92:
        return True, get_winner(len(t1_orig), len(t2_orig))
    
    return False, ""

def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = { "fm": "Radio", "am": "Radio", "radio": "Radio", "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión", "diario": "Prensa", "prensa": "Prensa", "revista": "Prensa", "revistas": "Prensa", "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"}
    return mapping.get(t, tipo_raw)

def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({ "titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "fecha": norm_key("Fecha"), "link_nota": norm_key("Link Nota"), "link_streaming": norm_key("Link (Streaming - Imagen)"), "tono": norm_key("Tono"), "tema": norm_key("Tema"), "tonoai": norm_key("Tono AI"), "justificaciontono": norm_key("Justificacion Tono"), "tipodemedio": norm_key("Tipo de Medio"), "region": norm_key("Region"), "hora": norm_key("Hora"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada"), "subtema": norm_key("Subtema") }) # Added subtema

    rows = []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)})

    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in m_list or [None]:
            new = deepcopy(base)
            if m: new[key_map["menciones"]] = m
            split_rows.append(new)

    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})

    for i in range(len(split_rows)):
        if split_rows[i]["is_duplicate"]: continue
        for j in range(i + 1, len(split_rows)):
            if split_rows[j]["is_duplicate"]: continue
            es_dup, cual = are_duplicates(split_rows[i], split_rows[j], key_map)
            if es_dup:
                if cual == "first":
                    split_rows[i].update({"is_duplicate": True, "idduplicada": split_rows[j].get(key_map.get("idnoticia"), "")})
                    break
                else:
                    split_rows[j].update({"is_duplicate": True, "idduplicada": split_rows[i].get(key_map.get("idnoticia"), "")})

    for row in split_rows:
        if row["is_duplicate"]:
            row.update({ key_map["tono"]: "Duplicada", key_map["tema"]: "-", key_map["subtema"]: "-", key_map["tonoai"]: "-", key_map["justificaciontono"]: "Noticia duplicada." }) # Added subtema to duplicated
    return split_rows, key_map

def _empty_link(): return {"value": "", "url": None}

def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
    tkey, ln_key, ls_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key): return
    tipo = normalizar_tipo_medio(str(row.get(tkey, "")))
    ln, ls = row.get(ln_key) or _empty_link(), row.get(ls_key) or _empty_link()
    has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
    if tipo in ["Radio", "Televisión"]: row[ls_key] = _empty_link()
    elif tipo == "Internet": row[ln_key], row[ls_key] = ls, ln
    elif tipo == "Prensa":
        if not has_url(ln) and has_url(ls): row[ln_key] = ls
        row[ls_key] = _empty_link()

def generate_output_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"
    final_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Tono AI","Tema","Subtema","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","Justificacion Tono","ID duplicada"]
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    out_sheet.append(final_order)
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    try: out_wb.add_named_style(link_style)
    except: pass

    for row_data in all_processed_rows:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(row_data.get(key_map.get("resumen")))
        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            nk_header = norm_key(header)
            val = row_data.get(nk_header)

            if row_data.get("is_duplicate"):
                if nk_header in [key_map['tonoai'], key_map['tema'], key_map['subtema'], key_map['justificaciontono']]:
                    val = "Duplicada"
                elif nk_header == key_map['idduplicada']:
                    val = row_data.get(key_map['idduplicada'])
            
            cell_value = None
            if header in numeric_columns:
                try: cell_value = float(val) if val is not None and str(val).strip() != "" else None
                except (ValueError, TypeError): cell_value = str(val)
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
# Proceso principal y UI
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases, tono_pkl_file, tema_pkl_file):
    start_time = time.time()
    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
        openai.aiosession.set(None)
    except Exception:
        st.error("❌ Error: OPENAI_API_KEY no encontrado en los Secrets de Streamlit.")
        st.stop()

    with st.status("📋 **Paso 1/5:** Limpieza y detección de duplicados", expanded=True) as s:
        st.write("🔍 Cargando y normalizando datos...")
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        duplicados = sum(1 for row in all_processed_rows if row.get("is_duplicate"))
        st.success(f"✅ **{len(all_processed_rows) - duplicados}** noticias únicas | **{duplicados}** duplicados detectados")
        s.update(label="✅ **Paso 1/5:** Limpieza completada", state="complete")

    with st.status("🗺️ **Paso 2/5:** Aplicando mapeos y normalización", expanded=True) as s:
        st.write("📍 Procesando regiones y medios digitales...")
        df_region = pd.read_excel(region_file)
        region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
        df_internet = pd.read_excel(internet_file)
        internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
        
        for row in all_processed_rows:
            medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()
            row[key_map.get("region", "region")] = region_map.get(medio_key, "N/A")
            if key_map.get("tipodemedio") in row:
                row[key_map.get("tipodemedio")] = normalizar_tipo_medio(row.get(key_map.get("tipodemedio")))
            if medio_key in internet_map:
                row[key_map.get("medio")] = internet_map[medio_key]
                if key_map.get("tipodemedio"): row[key_map.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, key_map)
        s.update(label="✅ **Paso 2/5:** Mapeos aplicados", state="complete")

    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]
    
    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        titulo_col = key_map["titulo"]
        resumen_col = key_map.get("resumen", "resumen")
        df_temp["resumen_api"] = df_temp[titulo_col].fillna("").astype(str) + ". " + df_temp[resumen_col].fillna("").astype(str)

        # --- ANÁLISIS DE TONO ---
        with st.status("🎯 **Paso 3/5:** Análisis inteligente de tono", expanded=True) as s:
            p_bar = st.progress(0, "Iniciando análisis de tono...")
            if tono_pkl_file:
                st.write(f"🤖 Usando modelo de tono personalizado (`pipeline_sentimiento.pkl`) para **{len(rows_to_analyze)}** noticias...")
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
            else:
                st.write(f"📊 Analizando tono con IA (reglas + LLM para refuerzo) para **{len(rows_to_analyze)}** noticias únicas de **{brand_name}**...")
                clasif = ClasificadorTonoUltraV2(brand_name, brand_aliases)
                resultados_tono = await clasif.procesar_lote_async(
                    df_temp["resumen_api"], p_bar, resumen_puro=df_temp[resumen_col], titulos_puros=df_temp[titulo_col]
                )

            df_temp[key_map["tonoai"]] = [res["tono"] for res in resultados_tono]
            df_temp[key_map["justificaciontono"]] = [res.get("justificacion", "") for res in resultados_tono]
            
            tonos = df_temp[key_map["tonoai"]].value_counts()
            positivos, negativos, neutros = tonos.get("Positivo", 0), tonos.get("Negativo", 0), tonos.get("Neutro", 0)
            total_tonos = max(1, len(resultados_tono))
            
            st.markdown("### 📈 Resultados del análisis de tono")
            col1, col2, col3 = st.columns(3)
            with col1: st.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #28a745;">🟢 {positivos}</div><div class="metric-label">Positivos ({positivos/total_tonos*100:.1f}%)</div></div>', unsafe_allow_html=True)
            with col2: st.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #d62728;">🔴 {negativos}</div><div class="metric-label">Negativos ({negativos/total_tonos*100:.1f}%)</div></div>', unsafe_allow_html=True)
            with col3: st.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #666;">⚪ {neutros}</div><div class="metric-label">Neutros ({neutros/total_tonos*100:.1f}%)</div></div>', unsafe_allow_html=True)
            
            s.update(label="✅ **Paso 3/5:** Análisis de tono completado", state="complete")

        # --- ANÁLISIS DE TEMAS ---
        with st.status("🏷️ **Paso 4/5:** Identificación inteligente de temas", expanded=True) as s:
            p_bar = st.progress(0, "Iniciando análisis de temas...")
            if tema_pkl_file:
                st.write(f"🤖 Clasificando temas con modelo personalizado (`pipeline_tema.pkl`) para **{len(rows_to_analyze)}** noticias...")
                temas_asignados = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                if temas_asignados is None: st.stop()
                df_temp[key_map["tema"]] = temas_asignados
                df_temp[key_map["subtema"]] = temas_asignados # Cuando se usa PKL de tema, tema y subtema son lo mismo
                
                st.success(f"✅ **{len(set(temas_asignados))}** temas identificados desde PKL")
                st.markdown("### 🏆 Top 5 temas principales (desde PKL)")
                for idx, (tema, count) in enumerate(Counter(temas_asignados).most_common(5), 1):
                    st.markdown(f'<span class="badge badge-info">#{idx}</span> **{tema}**: {count} noticias', unsafe_allow_html=True)

            else:
                st.write("🤖 Generando Subtemas detallados con IA...")
                clasificador_temas = ClasificadorTemaDinamico(brand_name, brand_aliases)
                subtemas = clasificador_temas.procesar_lote(
                    df_temp["resumen_api"], p_bar, brand_name, resumen_puro=df_temp[resumen_col], titulos_puros=df_temp[titulo_col]
                )
                df_temp[key_map["subtema"]] = subtemas
                
                st.write(f"🔄 Consolidando {len(set(subtemas))} Subtemas en {NUM_TEMAS_PRINCIPALES} Temas principales con IA...")
                p_bar_consolidacion = st.progress(0)
                temas_principales = consolidar_subtemas_en_temas(subtemas, p_bar_consolidacion)
                df_temp[key_map["tema"]] = temas_principales
            
                st.success(f"✅ **{len(set(temas_principales))}** temas principales y **{len(set(subtemas))}** subtemas únicos identificados")
                st.markdown("### 🏆 Top 5 temas principales")
                for idx, (tema, count) in enumerate(Counter(temas_principales).most_common(5), 1):
                    st.markdown(f'<span class="badge badge-info">#{idx}</span> **{tema}**: {count} noticias', unsafe_allow_html=True)
            
            s.update(label="✅ **Paso 4/5:** Temas identificados", state="complete")

        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"):
                result = results_map.get(row["original_index"])
                if result: row.update(result)

    with st.status("📊 **Paso 5/5:** Generando informe final", expanded=True) as s:
        st.write("📝 Compilando resultados y generando Excel...")
        duration = time.time() - start_time
        duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"
        
        st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_IA_{brand_name.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        st.session_state.update({ "brand_name": brand_name, "total_rows": len(all_processed_rows), "unique_rows": len(rows_to_analyze), "duplicates": len(all_processed_rows) - len(rows_to_analyze), "process_duration": duration_str })
        s.update(label="✅ **Paso 5/5:** Proceso completado", state="complete")

def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Análisis personalizable de Tono y Tema/Subtema</div>', unsafe_allow_html=True)

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
                st.info("Sube archivos aquí para anular el análisis por defecto y usar tus propios modelos o listas.")
                tono_pkl_file = st.file_uploader("Sube `pipeline_sentimiento.pkl` para Tono", type=["pkl"], help="El archivo debe ser un pipeline de Scikit-learn guardado con joblib que implemente .predict() y tome texto como entrada.")
                tema_pkl_file = st.file_uploader("Sube `pipeline_tema.pkl` para Tema", type=["pkl"], help="El archivo debe ser un pipeline de Scikit-learn guardado con joblib que implemente .predict() y tome texto como entrada.")

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file, brand_name.strip()]):
                    st.error("❌ Faltan archivos obligatorios o el nombre de la marca.")
                else:
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases, tono_pkl_file, tema_pkl_file))
                    st.rerun()
    else:
        st.markdown("## 🎉 Análisis Completado Exitosamente")
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f'<div class="metric-card"><div class="metric-value">{st.session_state.total_rows}</div><div class="metric-label">📰 Total Noticias</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #28a745;">{st.session_state.unique_rows}</div><div class="metric-label">✅ Únicas</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #ff7f0e;">{st.session_state.duplicates}</div><div class="metric-label">🔄 Duplicados</div></div>', unsafe_allow_html=True)
        c4.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #1f77b4;">{st.session_state.process_duration}</div><div class="metric-label">⏱️ Duración</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="success-card">', unsafe_allow_html=True)
        c1, c2 = st.columns([3, 2])
        with c1:
            st.markdown(f"### 📊 Informe para: {st.session_state.brand_name}")
            st.markdown(f"**Archivo:** `{st.session_state.output_filename}`")
        with c2:
            st.download_button("📥 **DESCARGAR INFORME**", data=st.session_state.output_data, file_name=st.session_state.output_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
            if st.button("🔄 **Nuevo Análisis**", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v3.2 | Realizado por Johnathan Cortés</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
