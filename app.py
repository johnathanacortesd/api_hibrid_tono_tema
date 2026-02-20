# ======================================
# app.py - Sistema de Análisis de Noticias con IA v8.0
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
from openai import AsyncOpenAI, OpenAI

# ======================================
# Configuración general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias con IA",
    page_icon="📰",
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
SIMILARITY_THRESHOLD_TEMAS = 0.82
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
for key in ['tokens_input', 'tokens_output', 'tokens_embedding']:
    if key not in st.session_state:
        st.session_state[key] = 0

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
    "capitalinos", "antioqueño", "antioqueños", "paisa", "paisas",
    "medellense", "caleño", "caleños", "valluno", "vallecaucano",
    "barranquillero", "cartagenero", "costeño", "costeños",
    "cucuteño", "bumangués", "santandereano", "boyacense",
    "tolimense", "huilense", "nariñense", "pastuso",
    "cordobés", "caucano", "chocoano", "casanareño",
    "caqueteño", "guajiro", "llanero", "amazonense",
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

# Patrones positivos contextuales (deben estar CERCA de la marca)
POS_BRAND_PATTERNS = [
    # Lanzamientos y novedades
    r"lanz[aó]|lanzamiento|lanzar[aá]|estrena|habilita|inaugur[aó]",
    r"nuev[oa]\s+\w+",
    r"apertur[aó]|abri[óo]\s+(su|una|nuevo)",
    # Alianzas y acuerdos
    r"alianza|acuerdo|convenio|colaboraci[oó]n|asociaci[oó]n|fusi[oó]n",
    # Crecimiento y resultados positivos
    r"crecimiento|crecieron|aument[oó]|ganancia|utilidad|beneficio",
    r"récord|record|hito|supera(r|ndo|ción)",
    # Reconocimiento
    r"reconocimiento|premio|premia|galardon|destac[aó]|lidera",
    # Innovación
    r"innova(ción|dor|ndo)|moderniza|transforma(ción)?|digitaliza",
    # Éxito
    r"éxito|exitoso|logr[oó]|consolida|fortalece",
    # Responsabilidad y compromiso
    r"sostenible|responsab(le|ilidad)|compromiso|bienestar",
    # Patrocinio y apoyo
    r"patrocin(io|a|ador)|auspicia|apoya|respalda",
    # Respuesta a crisis (positivo para la marca)
    r"atiende|activa\s+plan|gestiona|responde\s+ante|lidera\s+respuesta",
]

# Patrones negativos contextuales
NEG_BRAND_PATTERNS = [
    # Acciones legales y regulatorias
    r"demanda(do|da|n)?|denuncia(do|da)?|sancion(ado|ada|es)?|multa(do|da)?",
    r"investiga(do|da|ción)|irregularidad|fraud[e]|escándalo",
    # Pérdidas y crisis
    r"crisis|quiebra|default|pérdi(da|das)|déficit",
    r"ca[eí]da|baja(ron)?|desplom[eó]|retroceso",
    # Fallos operativos
    r"fall[aó]|interrupci[oó]n|suspende|cierra|cancel[aó]",
    r"filtraci[oó]n|hackeo|ataque\s+cibern|phishing",
    # Quejas y problemas
    r"queja|reclamo|reclama(ción|ciones)|incumpl(e|imiento)",
    r"problema(s|tica)?|dificultad|deterioro",
    # Conflictos
    r"conflicto|disputa|huelga|boicot|protest[aó]",
    r"rechaz[aó]|neg[oó]|desmiente",
    # Riesgo
    r"riesgo|amenaza|alerta|alarma|preocupa(ción|nte)",
]

CRISIS_KEYWORDS = re.compile(
    r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|"
    r"afectaciones|damnificados|tragedia|alerta\s+roja)\b",
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
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown("""
    <style>
    :root {
        --primary-color: #1f77b4;
        --secondary-color: #2ca02c;
        --card-bg: #ffffff;
        --shadow-light: 0 2px 4px rgba(0,0,0,0.1);
        --border-radius: 12px;
    }
    .main-header {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%);
        color: white; padding: 2rem; border-radius: var(--border-radius);
        text-align: center; font-size: 2.5rem; font-weight: 800;
        margin-bottom: 1.5rem; box-shadow: var(--shadow-light);
    }
    .subtitle {
        text-align: center; color: #666; font-size: 1.1rem;
        margin: -1rem 0 2rem 0;
    }
    .metric-card {
        background: var(--card-bg); padding: 1.2rem;
        border-radius: var(--border-radius); box-shadow: var(--shadow-light);
        text-align: center; border: 1px solid #e0e0e0;
    }
    .metric-value {
        font-size: 2rem; font-weight: bold; color: var(--primary-color);
    }
    .metric-label {
        font-size: 0.9rem; color: #666; text-transform: uppercase;
    }
    .success-card {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        padding: 1.5rem; border-radius: var(--border-radius);
        border: 1px solid #28a745; margin: 1rem 0;
        box-shadow: var(--shadow-light);
    }
    .stButton > button { border-radius: 8px; font-weight: 600; }
    </style>
    """, unsafe_allow_html=True)


# ======================================
# Clientes OpenAI (API v1+)
# ======================================
def get_sync_client() -> OpenAI:
    return OpenAI(api_key=st.secrets["OPENAI_API_KEY"])


def get_async_client() -> AsyncOpenAI:
    return AsyncOpenAI(api_key=st.secrets["OPENAI_API_KEY"])


# ======================================
# Utilidades de autenticación y helpers
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True
    st.markdown(
        '<div class="main-header">🔐 Portal de Acceso Seguro</div>',
        unsafe_allow_html=True
    )
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("🔑 Contraseña:", type="password")
            if st.form_submit_button(
                "🚀 Ingresar", use_container_width=True, type="primary"
            ):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True
                    st.success("✅ Acceso autorizado.")
                    st.balloons()
                    time.sleep(1.5)
                    st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
    return False


def _count_usage(resp):
    """Extrae y acumula usage de una respuesta de OpenAI API v1+."""
    usage = getattr(resp, 'usage', None)
    if usage is None:
        return
    st.session_state['tokens_input'] += getattr(usage, 'prompt_tokens', 0)
    st.session_state['tokens_output'] += getattr(usage, 'completion_tokens', 0)
    st.session_state['tokens_embedding'] += getattr(usage, 'total_tokens', 0) if hasattr(usage, 'total_tokens') and not hasattr(usage, 'prompt_tokens') else 0


def _count_embedding_usage(resp):
    """Cuenta tokens de embedding específicamente."""
    usage = getattr(resp, 'usage', None)
    if usage:
        st.session_state['tokens_embedding'] += getattr(usage, 'total_tokens', 0)


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
    """Limpia y normaliza un tema a 2-5 palabras significativas."""
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    # Remover puntuación final
    tema = re.sub(r'[.,:;!?]+$', '', tema)
    if not tema:
        return "Sin tema"

    # Capitalizar primera letra
    tema = tema[0].upper() + tema[1:]

    # Remover palabras vacías al final
    invalid_trailing = {
        "en", "de", "del", "la", "el", "y", "o", "con",
        "sin", "por", "para", "sobre", "al", "los", "las",
        "un", "una", "su", "sus"
    }
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_trailing:
        palabras.pop()

    # Limitar a 5 palabras
    if len(palabras) > 5:
        palabras = palabras[:5]

    tema = " ".join(palabras)
    return tema if tema else "Sin tema"


def limpiar_tema_geografico(
    tema: str, marca: str, aliases: List[str]
) -> str:
    """Remueve referencias a marca, ciudades y gentilicios del tema."""
    if not tema:
        return "Sin tema"

    tema_lower = tema.lower()

    # Remover marca y aliases
    all_brand_names = [marca.lower()] + [a.lower() for a in aliases if a]
    for brand_name in all_brand_names:
        tema_lower = re.sub(
            rf'\b{re.escape(brand_name)}\b', '', tema_lower, flags=re.IGNORECASE
        )
        tema_lower = re.sub(
            rf'\b{re.escape(unidecode(brand_name))}\b', '', tema_lower,
            flags=re.IGNORECASE
        )

    # Remover ciudades y gentilicios
    for ciudad in CIUDADES_COLOMBIA:
        tema_lower = re.sub(
            rf'\b{re.escape(ciudad)}\b', '', tema_lower, flags=re.IGNORECASE
        )
    for gentilicio in GENTILICIOS_COLOMBIA:
        tema_lower = re.sub(
            rf'\b{re.escape(gentilicio)}\b', '', tema_lower, flags=re.IGNORECASE
        )

    # Frases geográficas
    frases_geo = [
        "en colombia", "de colombia", "del pais", "en el pais",
        "nacional", "territorio nacional"
    ]
    for frase in frases_geo:
        tema_lower = re.sub(
            rf'\b{re.escape(frase)}\b', '', tema_lower, flags=re.IGNORECASE
        )

    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
    if not palabras:
        return "Sin tema"

    tema_limpio = " ".join(palabras)
    tema_limpio = tema_limpio[0].upper() + tema_limpio[1:]
    return limpiar_tema(tema_limpio)


# ======================================
# Embeddings con deduplicación interna
# ======================================
def get_embeddings_batch(
    textos: List[str], batch_size: int = 100
) -> List[Optional[List[float]]]:
    """
    Genera embeddings con deduplicación: textos idénticos se embeddean una sola vez.
    """
    if not textos:
        return []

    client = get_sync_client()

    # Deduplicar textos
    texto_to_idx: Dict[str, List[int]] = defaultdict(list)
    for i, t in enumerate(textos):
        key = (t or "")[:2000]
        texto_to_idx[key].append(i)

    unique_texts = list(texto_to_idx.keys())
    unique_embeddings: Dict[str, List[float]] = {}

    for i in range(0, len(unique_texts), batch_size):
        batch = unique_texts[i:i + batch_size]
        batch_clean = [t if t else " " for t in batch]
        try:
            resp = call_with_retries(
                client.embeddings.create,
                input=batch_clean,
                model=OPENAI_MODEL_EMBEDDING
            )
            _count_embedding_usage(resp)
            for j, emb_data in enumerate(resp.data):
                unique_embeddings[batch[j]] = emb_data.embedding
        except Exception:
            for j, texto in enumerate(batch):
                try:
                    resp = client.embeddings.create(
                        input=[texto if texto else " "],
                        model=OPENAI_MODEL_EMBEDDING
                    )
                    _count_embedding_usage(resp)
                    unique_embeddings[texto] = resp.data[0].embedding
                except Exception:
                    unique_embeddings[texto] = None

    # Reconstruir lista completa
    resultados = [None] * len(textos)
    for text_key, indices in texto_to_idx.items():
        emb = unique_embeddings.get(text_key)
        for idx in indices:
            resultados[idx] = emb

    return resultados


# ======================================
# DSU (Disjoint Set Union) robusto
# ======================================
class DSU:
    """Union-Find con path compression y union by rank."""

    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, i: int) -> int:
        while self.parent[i] != i:
            self.parent[i] = self.parent[self.parent[i]]  # path halving
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
def agrupar_textos_similares(
    textos: List[str], umbral_similitud: float
) -> Dict[int, List[int]]:
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
        n_clusters=None,
        distance_threshold=1 - umbral_similitud,
        metric="precomputed",
        linkage="average"
    ).fit(dist_matrix)

    grupos = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        grupos[label].append(valid_indices[i])

    return {gid: g for gid, g in enumerate(grupos.values()) if len(g) >= 2}


def agrupar_por_titulo_similar(titulos: List[str]) -> Dict[int, List[int]]:
    gid = 0
    grupos = {}
    used = set()
    norm_titles = [normalize_title_for_comparison(t) for t in titulos]

    for i in range(len(norm_titles)):
        if i in used or not norm_titles[i]:
            continue
        grupo_actual = [i]
        used.add(i)
        for j in range(i + 1, len(norm_titles)):
            if j in used or not norm_titles[j]:
                continue
            ratio = SequenceMatcher(
                None, norm_titles[i], norm_titles[j]
            ).ratio()
            if ratio >= SIMILARITY_THRESHOLD_TITULOS:
                grupo_actual.append(j)
                used.add(j)
        if len(grupo_actual) >= 2:
            grupos[gid] = grupo_actual
            gid += 1

    return grupos


def seleccionar_representante(
    indices: List[int], textos: List[str]
) -> Tuple[int, str]:
    """Selecciona el texto más cercano al centroide del grupo."""
    if len(indices) == 1:
        return indices[0], textos[indices[0]]

    subset_textos = [textos[i] for i in indices]
    embs = get_embeddings_batch(subset_textos)
    valid_embs = []
    valid_indices = []

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
    """
    Intenta extraer el nombre del cliente/marca desde un pipeline PKL.
    Busca en atributos comunes del modelo o vectorizador.
    """
    try:
        pkl_file.seek(0)
        pipeline = joblib.load(pkl_file)
        pkl_file.seek(0)

        # Buscar en atributos del pipeline
        for attr_name in ['marca', 'brand', 'client', 'cliente', 'target_name',
                          'brand_name', 'client_name']:
            if hasattr(pipeline, attr_name):
                val = getattr(pipeline, attr_name)
                if isinstance(val, str) and val.strip():
                    return val.strip()

        # Buscar en steps del pipeline si es sklearn Pipeline
        if hasattr(pipeline, 'steps'):
            for step_name, step_obj in pipeline.steps:
                for attr_name in ['marca', 'brand', 'client', 'cliente']:
                    if hasattr(step_obj, attr_name):
                        val = getattr(step_obj, attr_name)
                        if isinstance(val, str) and val.strip():
                            return val.strip()

        # Buscar en metadata
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
# CLASIFICADOR DE TONO v8.0
# ======================================
class ClasificadorTonoV8:
    """
    Clasificador de tono centrado en la alusión al cliente.

    Mejoras sobre v3:
    - Extracción de contexto centrada en la marca con ventana adaptativa
    - Análisis de relación sujeto-verbo entre marca y acción
    - Prompt de LLM con ejemplos y razonamiento estructurado
    - Propagación garantizada dentro de grupos de noticias similares
    """

    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex(marca, aliases)
        self.async_client = get_async_client()

    def _build_brand_regex(self, marca: str, aliases: List[str]) -> re.Pattern:
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = []
        for n in names:
            if n.strip():
                escaped = re.escape(unidecode(n.strip().lower()))
                patterns.append(escaped)
        if not patterns:
            return re.compile(r"(a^b)")  # Never matches
        return re.compile(
            r"\b(" + "|".join(patterns) + r")\b",
            re.IGNORECASE
        )

    def _extract_brand_contexts(self, texto: str) -> List[str]:
        """
        Extrae fragmentos centrados en la marca con ventana adaptativa.
        Busca límites de oración para contexto más coherente.
        """
        texto_lower = unidecode(texto.lower())
        matches = list(self.brand_pattern.finditer(texto_lower))

        if not matches:
            # Sin mención directa: usar inicio del texto como contexto limitado
            return [texto[:400]]

        contextos = []
        oraciones = re.split(r'(?<=[.!?])\s+', texto)

        for match in matches[:5]:  # Máximo 5 menciones
            pos = match.start()

            # Determinar ventana adaptativa
            snippet = texto_lower[max(0, pos - 30):min(len(texto_lower), pos + 80)]
            has_action_word = bool(re.search(
                r'(lanz|denunci|sancion|innov|crisis|acuerd|alianz|premi|'
                r'multa|demand|creci|pérdi|gananci)',
                snippet
            ))
            window = MAX_CONTEXT_WINDOW if has_action_word else MIN_CONTEXT_WINDOW

            # Encontrar las oraciones que contienen la mención
            char_count = 0
            relevant_sentences = []
            for sent in oraciones:
                sent_start = char_count
                sent_end = char_count + len(sent)
                # ¿Esta oración está en la ventana?
                if (sent_start <= pos + window) and (sent_end >= max(0, pos - window)):
                    relevant_sentences.append(sent.strip())
                char_count = sent_end + 1  # +1 por el espacio

            if relevant_sentences:
                contexto = " ".join(relevant_sentences)
            else:
                start = max(0, pos - window)
                end = min(len(texto), pos + window)
                # Extender hasta fin de oración
                while end < len(texto) and texto[end] not in '.!?\n':
                    end += 1
                contexto = texto[start:end + 1].strip()

            if contexto:
                contextos.append(contexto)

        # Deduplicar manteniendo orden
        seen = set()
        unique = []
        for c in contextos:
            key = c[:100]
            if key not in seen:
                seen.add(key)
                unique.append(c)

        return unique[:4]

    def _analyze_brand_sentiment_rules(
        self, contextos: List[str]
    ) -> Optional[str]:
        """
        Análisis basado en reglas con detección de relación marca-acción.
        Solo asigna tono si hay alta confianza.
        """
        pos_score = 0
        neg_score = 0

        for contexto in contextos:
            t = unidecode(contexto.lower())

            # Detectar si la marca es SUJETO de la acción
            brand_match = self.brand_pattern.search(t)
            if not brand_match:
                continue

            brand_pos = brand_match.start()

            # Verificar negación cerca de la marca
            pre_brand = t[max(0, brand_pos - 40):brand_pos]
            has_negation = bool(re.search(
                r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente)\b',
                pre_brand
            ))

            # Contexto post-marca (donde están las acciones)
            post_brand = t[brand_pos:min(len(t), brand_pos + 150)]

            # Detección de respuesta a crisis (marca que gestiona = positivo)
            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(post_brand):
                pos_score += 4
                continue

            # Contar patrones positivos CERCA de la marca
            for p in POS_COMPILED:
                if p.search(post_brand):
                    if has_negation:
                        neg_score += 1
                    else:
                        pos_score += 1.5

            # Contar patrones negativos CERCA de la marca
            for p in NEG_COMPILED:
                if p.search(post_brand):
                    if has_negation:
                        pos_score += 1  # Negación de negativo = positivo
                    else:
                        neg_score += 1.5

            # También revisar pre-marca (la marca como objeto)
            pre_context = t[max(0, brand_pos - 100):brand_pos]
            for p in NEG_COMPILED:
                if p.search(pre_context):
                    neg_score += 1
            for p in POS_COMPILED:
                if p.search(pre_context):
                    pos_score += 1

        # Solo decidir si hay señal clara
        if pos_score >= 3 and pos_score > neg_score * 2:
            return "Positivo"
        elif neg_score >= 3 and neg_score > pos_score * 2:
            return "Negativo"

        return None  # Dejar al LLM

    async def _llm_classify_tone(
        self, contextos: List[str], semaphore: asyncio.Semaphore
    ) -> str:
        """
        Clasificación de tono por LLM con prompt mejorado y ejemplos.
        """
        async with semaphore:
            aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
            contextos_texto = "\n---\n".join(c[:500] for c in contextos[:3])

            prompt = f"""Eres un analista de reputación corporativa. Analiza el SENTIMIENTO específicamente hacia la marca '{self.marca}' (alias: {aliases_str}) en los siguientes fragmentos de noticias.

IMPORTANTE: Clasifica el tono SOLO según cómo la noticia afecta la imagen/reputación de '{self.marca}', NO el tono general de la noticia.

CRITERIOS:
- **Positivo**: La marca logra algo, lanza producto, recibe reconocimiento, forma alianza, crece, responde bien ante crisis, innova, apoya causas
- **Negativo**: La marca es criticada, sancionada, multada, demandada, sufre pérdidas, tiene fallos, recibe quejas, está en escándalo
- **Neutro**: La marca es mencionada informativamente sin juicio claro, o el evento no impacta directamente su reputación

EJEMPLOS:
- "X lanzó su nueva plataforma digital" → Positivo (logro de la marca)
- "X fue multada por la SIC" → Negativo (sanción a la marca)
- "En la reunión participó X junto a otros actores" → Neutro (mención sin impacto)
- "Tras la emergencia, X activó su plan de contingencia" → Positivo (respuesta positiva)
- "Usuarios reportan fallas en la app de X" → Negativo (fallo del servicio)

FRAGMENTOS:
---
{contextos_texto}
---

Responde ÚNICAMENTE en JSON: {{"tono": "Positivo" | "Negativo" | "Neutro"}}"""

            try:
                resp = await acall_with_retries(
                    self.async_client.chat.completions.create,
                    model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=30,
                    temperature=0.0,
                    response_format={"type": "json_object"}
                )
                _count_usage(resp)

                data = json.loads(resp.choices[0].message.content.strip())
                tono = str(data.get("tono", "Neutro")).strip().title()
                if tono in ["Positivo", "Negativo", "Neutro"]:
                    return tono
                return "Neutro"
            except Exception:
                return "Neutro"

    async def _classify_group(
        self, texto_representante: str, semaphore: asyncio.Semaphore
    ) -> str:
        """Clasifica un grupo: reglas primero, LLM como fallback."""
        contextos = self._extract_brand_contexts(texto_representante)
        tono_reglas = self._analyze_brand_sentiment_rules(contextos)
        if tono_reglas:
            return tono_reglas
        return await self._llm_classify_tone(contextos, semaphore)

    async def procesar_lote_async(
        self,
        textos_concat: pd.Series,
        progress_bar,
        resumen_puro: pd.Series,
        titulos_puros: pd.Series
    ) -> List[Dict[str, str]]:
        """
        Procesa lote completo con agrupación robusta y propagación de tono.
        """
        textos = textos_concat.tolist()
        n = len(textos)

        progress_bar.progress(0.05, text="🔄 Agrupando noticias similares...")

        # Unificar grupos por múltiples criterios
        dsu = DSU(n)

        # 1. Agrupación por embedding de contenido
        grupos_emb = agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO)
        for _, idxs in grupos_emb.items():
            for j in idxs[1:]:
                dsu.union(idxs[0], j)

        # 2. Agrupación por título similar
        grupos_titulo = agrupar_por_titulo_similar(titulos_puros.tolist())
        for _, idxs in grupos_titulo.items():
            for j in idxs[1:]:
                dsu.union(idxs[0], j)

        comp = dsu.components()

        # Seleccionar representante de cada grupo
        representantes = {}
        for cid, idxs in comp.items():
            _, rep_text = seleccionar_representante(idxs, textos)
            representantes[cid] = rep_text

        progress_bar.progress(
            0.15,
            text=f"📊 {len(comp)} grupos identificados. Analizando tono..."
        )

        # Clasificar cada grupo
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        cids = list(representantes.keys())
        tasks = [
            self._classify_group(representantes[cid], semaphore)
            for cid in cids
        ]

        resultados_por_grupo = {}
        completed = 0
        for cid, coro in zip(cids, asyncio.as_completed(tasks)):
            tono = await coro
            resultados_por_grupo[cid] = tono
            completed += 1
            if completed % 10 == 0 or completed == len(tasks):
                progress_bar.progress(
                    0.15 + 0.80 * completed / len(tasks),
                    text=f"🎯 Tono: {completed}/{len(tasks)} grupos"
                )

        # Propagar tono a todos los miembros del grupo
        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            tono = resultados_por_grupo.get(cid, "Neutro")
            for i in idxs:
                resultados_finales[i] = {"tono": tono}

        progress_bar.progress(1.0, text="✅ Análisis de tono completado")
        return resultados_finales


def analizar_tono_con_pkl(
    textos: List[str], pkl_file: io.BytesIO
) -> Optional[List[Dict[str, str]]]:
    try:
        pkl_file.seek(0)
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        TONO_MAP = {
            1: "Positivo", "1": "Positivo",
            0: "Neutro", "0": "Neutro",
            -1: "Negativo", "-1": "Negativo",
            "Positivo": "Positivo", "Negativo": "Negativo", "Neutro": "Neutro",
            "positivo": "Positivo", "negativo": "Negativo", "neutro": "Neutro",
        }
        return [
            {"tono": TONO_MAP.get(p, str(p).title())}
            for p in predicciones
        ]
    except Exception as e:
        st.error(f"❌ Error con pipeline_sentimiento.pkl: {e}")
        return None


# ======================================
# CLASIFICADOR DE SUBTEMAS v8.0
# ======================================
class ClasificadorSubtemaV8:
    """
    Clasificador de subtemas con agrupación inteligente y etiquetas precisas.

    Mejoras:
    - Pre-agrupación por hashes de contenido
    - Clustering por lotes con umbral optimizado
    - Generación de subtemas con prompt más restrictivo
    - Fusión semántica bidimensional (etiqueta + contenido)
    - Cache robusto para evitar duplicidad de etiquetas
    """

    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.cache_subtemas: Dict[str, str] = {}
        self.sync_client = get_sync_client()

    def _preagrupar_identicos(
        self, textos: List[str], titulos: List[str], resumenes: List[str]
    ) -> Dict[int, List[int]]:
        """Pre-agrupa textos con título o resumen casi idéntico."""

        def normalizar_rapido(t):
            if not t:
                return ""
            t = unidecode(str(t).lower())
            t = re.sub(r'[^a-z0-9\s]', '', t)
            return ' '.join(t.split()[:40])

        titulo_hash_index: Dict[str, List[int]] = defaultdict(list)
        resumen_hash_index: Dict[str, List[int]] = defaultdict(list)

        for i, titulo in enumerate(titulos):
            norm = normalizar_rapido(titulo)
            if norm:
                h = hashlib.md5(norm.encode()).hexdigest()
                titulo_hash_index[h].append(i)

        for i, resumen in enumerate(resumenes):
            norm = normalizar_rapido(resumen)
            if norm:
                h = hashlib.md5(norm[:150].encode()).hexdigest()
                resumen_hash_index[h].append(i)

        grupos = {}
        usado = set()
        gid = 0

        for indices in titulo_hash_index.values():
            if len(indices) >= 2:
                nuevos = [i for i in indices if i not in usado]
                if len(nuevos) >= 2:
                    grupos[gid] = nuevos
                    usado.update(nuevos)
                    gid += 1

        for indices in resumen_hash_index.values():
            nuevos = [i for i in indices if i not in usado]
            if len(nuevos) >= 2:
                grupos[gid] = nuevos
                usado.update(nuevos)
                gid += 1

        return grupos

    def _clustering_por_lotes(
        self, textos: List[str], titulos: List[str], indices: List[int]
    ) -> Dict[int, List[int]]:
        """Clustering semántico por lotes para textos no pre-agrupados."""
        if len(indices) < 2:
            return {}

        BATCH_SIZE = 500
        grupos_finales = {}
        grupo_offset = 0

        for batch_start in range(0, len(indices), BATCH_SIZE):
            batch_idxs = indices[batch_start:batch_start + BATCH_SIZE]
            batch_texts = [
                f"{titulos[i][:150]} {textos[i][:1200]}"
                for i in batch_idxs
            ]

            embs = get_embeddings_batch(batch_texts)
            valid_embs = []
            valid_idxs = []
            for k, e in enumerate(embs):
                if e is not None:
                    valid_embs.append(e)
                    valid_idxs.append(batch_idxs[k])

            if len(valid_embs) < 2:
                continue

            sim_matrix = cosine_similarity(np.array(valid_embs))
            dist_matrix = 1 - sim_matrix
            np.fill_diagonal(dist_matrix, 0)
            dist_matrix = np.clip(dist_matrix, 0, 2)

            clustering = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=0.20,  # ~0.80 similitud
                metric='precomputed',
                linkage='average'
            ).fit(dist_matrix)

            grupos = defaultdict(list)
            for i, lbl in enumerate(clustering.labels_):
                grupos[lbl].append(valid_idxs[i])

            for lbl, idxs in grupos.items():
                if len(idxs) >= 2:
                    grupos_finales[grupo_offset + lbl] = idxs
            grupo_offset += max(clustering.labels_) + 1 if len(clustering.labels_) > 0 else 0

        return grupos_finales

    def _generar_subtema(
        self, textos_muestra: List[str], titulos_muestra: List[str]
    ) -> str:
        """Genera subtema preciso de 2-5 palabras para un grupo de noticias."""

        # Cache key basado en títulos normalizados
        titles_normalized = sorted(
            normalize_title_for_comparison(t)
            for t in titulos_muestra[:5]
        )
        cache_key = hashlib.md5(
            "|".join(titles_normalized).encode()
        ).hexdigest()

        if cache_key in self.cache_subtemas:
            return self.cache_subtemas[cache_key]

        # Extraer keywords de títulos
        all_words = []
        for t in titulos_muestra[:5]:
            words = [
                w for w in string_norm_label(t).split()
                if w not in STOPWORDS_ES and len(w) > 3
            ]
            all_words.extend(words)
        keywords = [w for w, _ in Counter(all_words).most_common(8)]

        # Título más representativo
        titulos_display = "\n".join(
            f"- {t[:120]}" for t in titulos_muestra[:5]
        )

        prompt = f"""Genera una ETIQUETA TEMÁTICA de 2 a 5 palabras para agrupar estas noticias.

TÍTULOS:
{titulos_display}

KEYWORDS: {', '.join(keywords)}

REGLAS ESTRICTAS:
1. NO incluir nombre de empresa/marca (como '{self.marca}')
2. NO incluir ciudades ni países
3. NO usar verbos conjugados
4. Ser ESPECÍFICO al evento/tema (NO "Noticias Varias", "Actividad", "Gestión")
5. Usar sustantivos y adjetivos descriptivos
6. MÁXIMO 5 palabras

BUENOS EJEMPLOS: "Expansión Red Sucursales", "Resultados Financieros Trimestrales", "Alianza Sector Salud", "Transformación Digital"
MALOS EJEMPLOS: "Noticias", "Actividades Varias", "Colombia", "Gestión Empresa"

Responde SOLO en JSON: {{"subtema": "..."}}"""

        try:
            resp = call_with_retries(
                self.sync_client.chat.completions.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=30,
                temperature=0.05,
                response_format={"type": "json_object"}
            )
            _count_usage(resp)

            data = json.loads(resp.choices[0].message.content.strip())
            subtema = data.get("subtema", "Varios")
            subtema = limpiar_tema_geografico(
                limpiar_tema(subtema), self.marca, self.aliases
            )

            # Validar que no sea genérico
            genericos = {
                "sin tema", "varios", "noticias", "actividad",
                "gestión", "información", "nota", "noticia"
            }
            if subtema.lower().strip() in genericos:
                # Usar keywords como fallback
                if keywords:
                    subtema = " ".join(keywords[:3]).title()
                else:
                    subtema = "Actividad Corporativa"

            self.cache_subtemas[cache_key] = subtema
            return subtema
        except Exception:
            if keywords:
                subtema = " ".join(keywords[:3]).title()
                self.cache_subtemas[cache_key] = subtema
                return subtema
            return "Actividad Corporativa"

    def _fusionar_subtemas_similares(
        self, subtemas: List[str], textos: List[str]
    ) -> List[str]:
        """
        Fusiona subtemas con semántica similar usando TANTO etiquetas COMO contenido.
        """
        df_temp = pd.DataFrame({'label': subtemas, 'text': textos})
        unique_labels = list(df_temp['label'].unique())

        if len(unique_labels) < 2:
            return subtemas

        # Embeddings de etiquetas
        embs_labels = get_embeddings_batch(unique_labels)

        # Centroides de contenido por etiqueta
        todos_embs = get_embeddings_batch(textos)
        centroides_contenido = []

        valid_labels = []
        valid_label_embs = []

        for i, label in enumerate(unique_labels):
            if embs_labels[i] is None:
                continue

            idxs = df_temp.index[df_temp['label'] == label].tolist()[:40]
            vecs = [todos_embs[j] for j in idxs if todos_embs[j] is not None]

            if vecs:
                centroides_contenido.append(np.mean(vecs, axis=0))
                valid_labels.append(label)
                valid_label_embs.append(embs_labels[i])
            else:
                centroides_contenido.append(embs_labels[i])
                valid_labels.append(label)
                valid_label_embs.append(embs_labels[i])

        if len(valid_labels) < 2:
            return subtemas

        # Similitud combinada: 40% etiqueta + 60% contenido
        matrix_labels = np.array(valid_label_embs)
        matrix_content = np.array(centroides_contenido)

        sim_labels = cosine_similarity(matrix_labels)
        sim_content = cosine_similarity(matrix_content)
        sim_combined = 0.4 * sim_labels + 0.6 * sim_content

        dist_matrix = 1 - sim_combined
        np.fill_diagonal(dist_matrix, 0)
        dist_matrix = np.clip(dist_matrix, 0, 2)

        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=1 - UMBRAL_FUSION_SUBTEMAS,
            metric='precomputed',
            linkage='average'
        ).fit(dist_matrix)

        # El representante de cada cluster es el subtema más frecuente
        mapa_fusion = {}
        for cluster_id in set(clustering.labels_):
            cluster_indices = [
                i for i, x in enumerate(clustering.labels_)
                if x == cluster_id
            ]
            labels_in_cluster = [valid_labels[i] for i in cluster_indices]

            # Contar frecuencias
            counts = {
                lbl: sum(1 for s in subtemas if s == lbl)
                for lbl in labels_in_cluster
            }
            representante = max(
                labels_in_cluster,
                key=lambda x: (counts.get(x, 0), -len(x))
            )
            for lbl in labels_in_cluster:
                mapa_fusion[lbl] = representante

        return [mapa_fusion.get(lbl, lbl) for lbl in subtemas]

    def procesar_lote(
        self,
        textos_concat: pd.Series,
        progress_bar,
        resumen_puro: pd.Series,
        titulos_puros: pd.Series
    ) -> List[str]:
        """Proceso completo de generación de subtemas."""
        textos = textos_concat.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumen_puro.tolist()
        n = len(textos)

        progress_bar.progress(0.05, "⚡ Pre-agrupando noticias idénticas...")

        # Fase 1: Pre-agrupación rápida
        grupos_rapidos = self._preagrupar_identicos(textos, titulos, resumenes)

        dsu = DSU(n)
        for idxs in grupos_rapidos.values():
            for j in idxs[1:]:
                dsu.union(idxs[0], j)

        comp = dsu.components()
        indices_sueltos = [
            idxs[0] for idxs in comp.values() if len(idxs) == 1
        ]

        progress_bar.progress(0.15, f"📊 {len(grupos_rapidos)} grupos rápidos. Refinando...")

        # Fase 2: Clustering semántico para sueltos
        if len(indices_sueltos) > 1:
            grupos_cluster = self._clustering_por_lotes(
                textos, titulos, indices_sueltos
            )
            for idxs in grupos_cluster.values():
                for j in idxs[1:]:
                    dsu.union(idxs[0], j)

        comp = dsu.components()
        total_grupos = len(comp)
        progress_bar.progress(0.30, f"🏷️ Etiquetando {total_grupos} grupos...")

        # Fase 3: Generar subtema para cada grupo
        mapa_subtemas = {}
        for k, (lid, idxs) in enumerate(comp.items()):
            if k % 15 == 0:
                progress_bar.progress(
                    0.30 + 0.40 * k / max(total_grupos, 1),
                    f"🏷️ Etiquetando: {k}/{total_grupos}"
                )
            subtema = self._generar_subtema(
                [textos[i] for i in idxs],
                [titulos[i] for i in idxs]
            )
            for i in idxs:
                mapa_subtemas[i] = subtema

        subtemas_brutos = [mapa_subtemas.get(i, "Varios") for i in range(n)]
        n_brutos = len(set(subtemas_brutos))

        progress_bar.progress(0.75, "🔗 Fusionando subtemas similares...")

        # Fase 4: Fusión por similitud semántica
        subtemas_fusionados = self._fusionar_subtemas_similares(
            subtemas_brutos, textos
        )
        n_fusionados = len(set(subtemas_fusionados))

        st.info(f"📉 Subtemas: {n_brutos} → {n_fusionados}")
        progress_bar.progress(1.0, "✅ Subtemas listos")

        return subtemas_fusionados


# ======================================
# Consolidación de Subtemas en Temas Principales
# ======================================
def consolidar_subtemas_en_temas(
    subtemas: List[str],
    textos: List[str],
    p_bar,
    marca: str = "",
    aliases: List[str] = None
) -> List[str]:
    """
    Consolida subtemas en categorías temáticas principales (máx NUM_TEMAS_PRINCIPALES).
    Usa similitud combinada de etiquetas + contenido.
    """
    aliases = aliases or []
    client = get_sync_client()

    p_bar.progress(0.1, text="📊 Analizando estructura de temas...")

    df_temas = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    unique_subtemas = list(df_temas['subtema'].unique())

    if len(unique_subtemas) <= NUM_TEMAS_PRINCIPALES:
        # Ya hay pocos subtemas, usarlos directamente como temas
        p_bar.progress(1.0, "✅ Temas finalizados")
        return subtemas

    # Embeddings de etiquetas
    embs_labels = get_embeddings_batch(unique_subtemas)

    # Centroides de contenido
    todos_embs = get_embeddings_batch(textos)
    centroides = []
    valid_subtemas = []
    valid_label_embs = []

    for i, subt in enumerate(unique_subtemas):
        if embs_labels[i] is None:
            continue

        idxs = df_temas.index[df_temas['subtema'] == subt].tolist()[:30]
        vecs = [todos_embs[j] for j in idxs if todos_embs[j] is not None]

        if vecs:
            centroides.append(np.mean(vecs, axis=0))
        else:
            centroides.append(embs_labels[i])

        valid_subtemas.append(subt)
        valid_label_embs.append(embs_labels[i])

    if len(valid_subtemas) < 2:
        p_bar.progress(1.0, "✅ Temas finalizados")
        return subtemas

    # Similitud combinada
    matrix_labels = np.array(valid_label_embs)
    matrix_content = np.array(centroides)

    sim_labels = cosine_similarity(matrix_labels)
    sim_content = cosine_similarity(matrix_content)
    sim_final = 0.35 * sim_labels + 0.65 * sim_content

    dist_final = 1 - sim_final
    np.fill_diagonal(dist_final, 0)
    dist_final = np.clip(dist_final, 0, 2)

    n_clusters_target = min(NUM_TEMAS_PRINCIPALES, len(valid_subtemas))

    clustering = AgglomerativeClustering(
        n_clusters=n_clusters_target,
        metric='precomputed',
        linkage='average'
    ).fit(dist_final)

    # Generar nombre para cada cluster
    clusters_contenidos = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        clusters_contenidos[label].append(valid_subtemas[i])

    p_bar.progress(0.5, text="🏷️ Nombrando categorías temáticas...")

    mapa_tema_final = {}
    for cid, lista_subt in clusters_contenidos.items():
        subtemas_str = ", ".join(lista_subt[:8])

        # Obtener algunos títulos representativos del cluster
        titulos_sample = []
        for subt in lista_subt[:3]:
            rows = df_temas[df_temas['subtema'] == subt].head(2)
            for _, row in rows.iterrows():
                # Extraer título del texto (primera oración)
                primera_oracion = row['texto'].split('.')[0][:100]
                titulos_sample.append(primera_oracion)

        context = "\n".join(f"- {t}" for t in titulos_sample[:5])

        prompt = f"""Genera un NOMBRE DE CATEGORÍA TEMÁTICA (2-3 palabras) que agrupe estos subtemas:

Subtemas: {subtemas_str}

Noticias ejemplo:
{context}

REGLAS:
1. Máximo 3 palabras
2. NO verbos conjugados
3. NO nombre de empresas
4. Ser descriptivo pero conciso
5. Usar sustantivos

BUENOS: "Resultados Financieros", "Expansión Comercial", "Innovación Digital", "Responsabilidad Social"
MALOS: "Varios", "Noticias", "Actividades", "Gestión General"

Responde SOLO el nombre, sin JSON ni comillas."""

        try:
            resp = call_with_retries(
                client.chat.completions.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=15,
                temperature=0.1
            )
            _count_usage(resp)

            nombre_tema = limpiar_tema(
                resp.choices[0].message.content.strip()
                .replace('"', '').replace('.', '')
            )

            # Validar
            if not nombre_tema or nombre_tema.lower() in {
                "sin tema", "varios", "noticias"
            }:
                nombre_tema = lista_subt[0]

        except Exception:
            nombre_tema = lista_subt[0]

        # Remover marca si se coló
        if marca:
            nombre_tema = limpiar_tema_geografico(
                nombre_tema, marca, aliases
            )

        for subt in lista_subt:
            mapa_tema_final[subt] = nombre_tema

    temas_finales = [mapa_tema_final.get(subt, subt) for subt in subtemas]
    n_temas = len(set(temas_finales))

    st.info(f"📉 Temas consolidados en {n_temas} categorías")
    p_bar.progress(1.0, "✅ Temas finalizados")

    return temas_finales


def analizar_temas_con_pkl(
    textos: List[str], pkl_file: io.BytesIO
) -> Optional[List[str]]:
    try:
        pkl_file.seek(0)
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"❌ Error con pipeline_tema.pkl: {e}")
        return None


# ======================================
# Detección de Duplicados
# ======================================
def detectar_duplicados_avanzado(
    rows: List[Dict], key_map: Dict[str, str]
) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url = {}
    seen_broadcast = {}
    online_title_buckets: Dict[Tuple, List[int]] = defaultdict(list)

    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"):
            continue

        tipo_medio = normalizar_tipo_medio(
            str(row.get(key_map.get("tipodemedio")))
        )
        mencion_norm = norm_key(row.get(key_map.get("menciones")))
        medio_norm = norm_key(row.get(key_map.get("medio")))

        if tipo_medio == "Internet":
            link_info = row.get(key_map.get("link_nota"), {})
            url = link_info.get("url") if isinstance(link_info, dict) else None
            if url and mencion_norm:
                key = (url, mencion_norm)
                if key in seen_online_url:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_online_url[key]].get(
                        key_map.get("idnoticia"), ""
                    )
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
                    row["idduplicada"] = processed_rows[seen_broadcast[key]].get(
                        key_map.get("idnoticia"), ""
                    )
                else:
                    seen_broadcast[key] = i

    # Duplicados por título similar en mismo medio
    for indices in online_title_buckets.values():
        if len(indices) < 2:
            continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx1, idx2 = indices[i], indices[j]
                if (processed_rows[idx1].get("is_duplicate") or
                        processed_rows[idx2].get("is_duplicate")):
                    continue
                t1 = normalize_title_for_comparison(
                    processed_rows[idx1].get(key_map.get("titulo"))
                )
                t2 = normalize_title_for_comparison(
                    processed_rows[idx2].get(key_map.get("titulo"))
                )
                if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    if len(t1) < len(t2):
                        processed_rows[idx1]["is_duplicate"] = True
                        processed_rows[idx1]["idduplicada"] = processed_rows[idx2].get(
                            key_map.get("idnoticia"), ""
                        )
                    else:
                        processed_rows[idx2]["is_duplicate"] = True
                        processed_rows[idx2]["idduplicada"] = processed_rows[idx1].get(
                            key_map.get("idnoticia"), ""
                        )

    return processed_rows


# ======================================
# Procesamiento del Dossier
# ======================================
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
        "region": norm_key("Region")
    })

    rows = []
    split_rows = []

    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row):
            continue
        rows.append({
            norm_keys[i]: c
            for i, c in enumerate(row)
            if i < len(norm_keys)
        })

    for r_cells in rows:
        base = {}
        for k, v in r_cells.items():
            if k in [key_map["link_nota"], key_map["link_streaming"]]:
                base[k] = extract_link(v)
            else:
                base[k] = v.value

        if key_map.get("tipodemedio") in base:
            base[key_map["tipodemedio"]] = normalizar_tipo_medio(
                base.get(key_map["tipodemedio"])
            )

        m_list = [
            m.strip()
            for m in str(base.get(key_map["menciones"], "")).split(";")
            if m.strip()
        ]

        for m in m_list or [None]:
            new = deepcopy(base)
            if m:
                new[key_map["menciones"]] = m
            split_rows.append(new)

    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})

    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)

    for row in processed_rows:
        if row["is_duplicate"]:
            row.update({
                key_map["tonoiai"]: "Duplicada",
                key_map["tema"]: "Duplicada",
                key_map["subtema"]: "Duplicada"
            })

    return processed_rows, key_map


def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
    tkey = key_map.get("tipodemedio")
    ln_key = key_map.get("link_nota")
    ls_key = key_map.get("link_streaming")

    if not (tkey and ln_key and ls_key):
        return

    tipo = row.get(tkey, "")
    ln = row.get(ln_key) or {"value": "", "url": None}
    ls = row.get(ls_key) or {"value": "", "url": None}

    has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))

    if tipo in ["Radio", "Televisión"]:
        row[ls_key] = {"value": "", "url": None}
    elif tipo == "Internet":
        row[ln_key], row[ls_key] = ls, ln
    elif tipo in ["Prensa", "Revista"]:
        if not has_url(ln) and has_url(ls):
            row[ln_key] = ls
        row[ls_key] = {"value": "", "url": None}


# ======================================
# Generación de Excel de salida
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
    numeric_columns = {
        "ID Noticia", "Nro. Pagina", "Dimension",
        "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"
    }

    out_sheet.append(final_order)

    link_style = NamedStyle(
        name="Hyperlink_Custom",
        font=Font(color="0000FF", underline="single")
    )
    if "Hyperlink_Custom" not in out_wb.style_names:
        out_wb.add_named_style(link_style)

    for row_data in all_processed_rows:
        titulo_key = key_map.get("titulo")
        if titulo_key and titulo_key in row_data:
            row_data[titulo_key] = clean_title_for_output(
                row_data.get(titulo_key)
            )

        resumen_key = key_map.get("resumen")
        if resumen_key and resumen_key in row_data:
            row_data[resumen_key] = corregir_texto(row_data.get(resumen_key))

        row_to_append = []
        links_to_add = {}

        for col_idx, header in enumerate(final_order, 1):
            nk_header = norm_key(header)
            data_key = key_map.get(nk_header, nk_header)
            val = row_data.get(data_key)
            cell_value = None

            if header in numeric_columns:
                try:
                    cell_value = float(val) if val is not None and str(val).strip() != "" else None
                except (ValueError, TypeError):
                    cell_value = str(val) if val is not None else None
            elif isinstance(val, dict) and "url" in val:
                cell_value = val.get("value", "Link")
                if val.get("url"):
                    links_to_add[col_idx] = val["url"]
            elif val is not None:
                cell_value = str(val)

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
# Proceso principal completo
# ======================================
async def run_full_process_async(
    dossier_file, region_file, internet_file,
    brand_name, brand_aliases,
    tono_pkl_file, tema_pkl_file, analysis_mode
):
    # Reset contadores
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0

    start_time = time.time()

    if "API" in analysis_mode:
        try:
            _ = st.secrets["OPENAI_API_KEY"]
        except Exception:
            st.error("❌ Error: OPENAI_API_KEY no encontrado.")
            st.stop()

    # Paso 1: Limpieza y duplicados
    with st.status("📋 **Paso 1/5:** Limpieza y duplicados", expanded=True) as s:
        all_processed_rows, key_map = run_dossier_logic(
            load_workbook(dossier_file, data_only=True).active
        )
        s.update(label="✅ **Paso 1/5:** Limpieza completada", state="complete")

    # Paso 2: Mapeos
    with st.status("🗺️ **Paso 2/5:** Mapeos y Normalización", expanded=True) as s:
        df_region = pd.read_excel(region_file)
        region_map = {
            str(k).lower().strip(): v
            for k, v in pd.Series(
                df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]
            ).to_dict().items()
        }

        df_internet = pd.read_excel(internet_file)
        internet_map = {
            str(k).lower().strip(): v
            for k, v in pd.Series(
                df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]
            ).to_dict().items()
        }

        for row in all_processed_rows:
            original_medio_key = str(
                row.get(key_map.get("medio"), "")
            ).lower().strip()
            row[key_map.get("region")] = region_map.get(
                original_medio_key, "N/A"
            )
            if original_medio_key in internet_map:
                row[key_map.get("medio")] = internet_map[original_medio_key]
                row[key_map.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, key_map)

        s.update(label="✅ **Paso 2/5:** Mapeos aplicados", state="complete")

    rows_to_analyze = [
        row for row in all_processed_rows if not row.get("is_duplicate")
    ]

    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        df_temp["resumen_api"] = (
            df_temp[key_map["titulo"]].fillna("").astype(str) + ". " +
            df_temp[key_map["resumen"]].fillna("").astype(str)
        )

        # Paso 3: Tono
        with st.status("🎯 **Paso 3/5:** Análisis de Tono", expanded=True) as s:
            p_bar = st.progress(0)

            if ("PKL" in analysis_mode) and tono_pkl_file:
                resultados_tono = analizar_tono_con_pkl(
                    df_temp["resumen_api"].tolist(), tono_pkl_file
                )
                if resultados_tono is None:
                    st.stop()
                p_bar.progress(1.0, "✅ Tono (PKL)")
            elif "API" in analysis_mode:
                clasif_tono = ClasificadorTonoV8(brand_name, brand_aliases)
                resultados_tono = await clasif_tono.procesar_lote_async(
                    df_temp["resumen_api"], p_bar,
                    df_temp[key_map["resumen"]],
                    df_temp[key_map["titulo"]]
                )
            else:
                resultados_tono = [{"tono": "N/A"}] * len(rows_to_analyze)
                p_bar.progress(1.0, "⏭️ Sin análisis de tono")

            df_temp[key_map["tonoiai"]] = [
                res["tono"] for res in resultados_tono
            ]
            s.update(
                label="✅ **Paso 3/5:** Tono Analizado",
                state="complete"
            )

        # Paso 4: Temas
        with st.status("🏷️ **Paso 4/5:** Análisis de Tema y Subtema", expanded=True) as s:
            p_bar = st.progress(0)

            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A"] * len(rows_to_analyze)
                temas_principales = ["N/A"] * len(rows_to_analyze)
            else:
                clasif_subtemas = ClasificadorSubtemaV8(
                    brand_name, brand_aliases
                )
                subtemas = clasif_subtemas.procesar_lote(
                    df_temp["resumen_api"], p_bar,
                    df_temp[key_map["resumen"]],
                    df_temp[key_map["titulo"]]
                )

                temas_principales = consolidar_subtemas_en_temas(
                    subtemas,
                    df_temp["resumen_api"].tolist(),
                    p_bar,
                    marca=brand_name,
                    aliases=brand_aliases
                )

            df_temp[key_map["subtema"]] = subtemas

            if ("PKL" in analysis_mode) and tema_pkl_file:
                temas_pkl = analizar_temas_con_pkl(
                    df_temp["resumen_api"].tolist(), tema_pkl_file
                )
                if temas_pkl:
                    df_temp[key_map["tema"]] = temas_pkl
            else:
                df_temp[key_map["tema"]] = temas_principales

            s.update(
                label="✅ **Paso 4/5:** Clasificación Completada",
                state="complete"
            )

        # Aplicar resultados
        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"):
                row.update(results_map.get(row["original_index"], {}))

    # Calcular costos
    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    total_cost = cost_input + cost_output + cost_embedding
    cost_str = f"${total_cost:.4f} USD"

    # Paso 5: Generar Excel
    with st.status("📊 **Paso 5/5:** Generando informe final", expanded=True) as s:
        duration_str = f"{time.time() - start_time:.0f}s"
        st.session_state["output_data"] = generate_output_excel(
            all_processed_rows, key_map
        )
        st.session_state["output_filename"] = (
            f"Informe_IA_{brand_name.replace(' ', '_')}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name": brand_name,
            "brand_aliases": brand_aliases,
            "total_rows": len(all_processed_rows),
            "unique_rows": len(rows_to_analyze),
            "duplicates": len(all_processed_rows) - len(rows_to_analyze),
            "process_duration": duration_str,
            "process_cost": cost_str
        })
        s.update(
            label="✅ **Paso 5/5:** Proceso completado",
            state="complete"
        )


# ======================================
# Análisis Rápido
# ======================================
async def run_quick_analysis_async(
    df: pd.DataFrame, title_col: str, summary_col: str,
    brand_name: str, aliases: List[str]
):
    # Reset contadores
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0

    df['texto_analisis'] = (
        df[title_col].fillna('').astype(str) + ". " +
        df[summary_col].fillna('').astype(str)
    )

    with st.status("🎯 **Paso 1/2:** Analizando Tono...", expanded=True) as s:
        p_bar = st.progress(0, "Iniciando análisis de tono...")
        clasif_tono = ClasificadorTonoV8(brand_name, aliases)
        resultados_tono = await clasif_tono.procesar_lote_async(
            df["texto_analisis"], p_bar,
            df[summary_col].fillna(''),
            df[title_col].fillna('')
        )
        df['Tono IA'] = [res["tono"] for res in resultados_tono]
        s.update(label="✅ **Paso 1/2:** Tono Analizado", state="complete")

    with st.status("🏷️ **Paso 2/2:** Analizando Tema y Subtema...", expanded=True) as s:
        p_bar = st.progress(0, "Generando subtemas...")
        clasif_subtemas = ClasificadorSubtemaV8(brand_name, aliases)
        subtemas = clasif_subtemas.procesar_lote(
            df["texto_analisis"], p_bar,
            df[summary_col].fillna(''),
            df[title_col].fillna('')
        )
        df['Subtema'] = subtemas

        p_bar_temas = st.progress(0, "Consolidando temas...")
        temas_principales = consolidar_subtemas_en_temas(
            subtemas, df["texto_analisis"].tolist(), p_bar_temas,
            marca=brand_name, aliases=aliases
        )
        df['Tema'] = temas_principales
        s.update(
            label="✅ **Paso 2/2:** Clasificación Finalizada",
            state="complete"
        )

    df.drop(columns=['texto_analisis'], inplace=True)

    # Costos
    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    total_cost = cost_input + cost_output + cost_embedding
    st.session_state['quick_cost'] = f"${total_cost:.4f} USD"

    return df


def generate_quick_analysis_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Analisis')
    return output.getvalue()


def render_quick_analysis_tab():
    st.header("Análisis Rápido con IA")
    st.info(
        "Análisis avanzado de Tono, Tema y Subtema centrado en la marca."
    )

    if 'quick_analysis_result' in st.session_state:
        st.success("🎉 Análisis Rápido Completado")
        cost = st.session_state.get('quick_cost', "$0.00")
        st.metric(label="Costo Estimado", value=cost)

        st.dataframe(st.session_state.quick_analysis_result.head(10))
        excel_data = generate_quick_analysis_excel(
            st.session_state.quick_analysis_result
        )
        st.download_button(
            label="📥 **Descargar Resultados**",
            data=excel_data,
            file_name="Analisis_Rapido_IA.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )
        if st.button("🔄 Nuevo Análisis"):
            for key in [
                'quick_analysis_result', 'quick_analysis_df',
                'quick_file_name', 'quick_cost'
            ]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        return

    if 'quick_analysis_df' not in st.session_state:
        quick_file = st.file_uploader(
            "📂 **Sube tu archivo Excel**", type=["xlsx"],
            label_visibility="collapsed", key="quick_uploader"
        )
        if quick_file:
            with st.spinner("Leyendo archivo..."):
                try:
                    st.session_state.quick_analysis_df = pd.read_excel(quick_file)
                    st.session_state.quick_file_name = quick_file.name
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.stop()
    else:
        st.success(
            f"✅ Archivo **'{st.session_state.quick_file_name}'** cargado."
        )
        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df
            columns = df.columns.tolist()

            col1, col2 = st.columns(2)
            title_col = col1.selectbox(
                "Columna **Título**", options=columns, index=0
            )
            summary_index = 1 if len(columns) > 1 else 0
            summary_col = col2.selectbox(
                "Columna **Resumen**", options=columns, index=summary_index
            )

            st.write("---")

            # Intentar extraer marca del PKL si se proporciona
            st.markdown("#### 🏢 Configuración de Marca")
            st.caption(
                "Si subes un PKL que contenga la marca, "
                "el campo se autocompletará."
            )

            pkl_for_brand = st.file_uploader(
                "PKL para detectar marca (opcional)",
                type=["pkl"], key="quick_pkl_brand"
            )

            default_brand = ""
            if pkl_for_brand:
                detected = extraer_marca_de_pkl(pkl_for_brand)
                if detected:
                    default_brand = detected
                    st.success(f"🎯 Marca detectada del PKL: **{detected}**")

            brand_name = st.text_input(
                "**Marca Principal**",
                value=default_brand,
                placeholder="Ej: Siemens"
            )
            brand_aliases_text = st.text_area(
                "**Alias** (separados por ;)",
                placeholder="Ej: Siemens Healthineers; Siemens Energy",
                height=80
            )

            if st.form_submit_button(
                "🚀 **Analizar**", use_container_width=True, type="primary"
            ):
                if not brand_name:
                    st.error("❌ Falta nombre de marca.")
                else:
                    try:
                        _ = st.secrets["OPENAI_API_KEY"]
                    except Exception:
                        st.error("❌ OPENAI_API_KEY no encontrada.")
                        st.stop()

                    aliases = [
                        a.strip() for a in brand_aliases_text.split(";")
                        if a.strip()
                    ]
                    with st.spinner("🧠 Analizando..."):
                        st.session_state.quick_analysis_result = asyncio.run(
                            run_quick_analysis_async(
                                df.copy(), title_col, summary_col,
                                brand_name, aliases
                            )
                        )
                    st.rerun()

        if st.button("⬅️ Cargar otro"):
            for key in [
                'quick_analysis_df', 'quick_file_name',
                'quick_analysis_result', 'quick_cost'
            ]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()


# ======================================
# Main UI
# ======================================
def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown(
        '<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<div class="subtitle">Análisis preciso de Tono y Tema centrado en la marca</div>',
        unsafe_allow_html=True
    )

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("input_form"):
                st.markdown("### 📂 Archivos de Entrada")
                col1, col2, col3 = st.columns(3)
                dossier_file = col1.file_uploader(
                    "**1. Dossier** (.xlsx)", type=["xlsx"]
                )
                region_file = col2.file_uploader(
                    "**2. Región** (.xlsx)", type=["xlsx"]
                )
                internet_file = col3.file_uploader(
                    "**3. Internet** (.xlsx)", type=["xlsx"]
                )

                st.markdown("### 🏢 Configuración de Marca")

                # Detectar marca desde PKL
                st.caption(
                    "Si subes un PKL con la marca embebida, "
                    "el nombre se autocompletará."
                )

                brand_name = st.text_input(
                    "**Marca Principal**",
                    placeholder="Ej: Bancolombia",
                    key="main_brand_name"
                )
                brand_aliases_text = st.text_area(
                    "**Alias** (sep. por ;)",
                    placeholder="Ej: Ban;Juan Carlos Mora",
                    height=80,
                    key="main_brand_aliases"
                )

                st.markdown("### ⚙️ Modo de Análisis")
                analysis_mode = st.radio(
                    "Selecciona modo:",
                    options=[
                        "Híbrido (PKL + API)",
                        "Solo Modelos PKL",
                        "API de OpenAI"
                    ],
                    index=0,
                    key="analysis_mode_radio"
                )

                tono_pkl_file = None
                tema_pkl_file = None

                if "PKL" in analysis_mode:
                    c1, c2 = st.columns(2)
                    tono_pkl_file = c1.file_uploader(
                        "`sentimiento.pkl`", type=["pkl"]
                    )
                    tema_pkl_file = c2.file_uploader(
                        "`tema.pkl`", type=["pkl"]
                    )

                    # Autodetectar marca desde PKL de tono
                    if tono_pkl_file and not brand_name:
                        detected = extraer_marca_de_pkl(tono_pkl_file)
                        if detected:
                            st.info(
                                f"🎯 Marca detectada del PKL: **{detected}**"
                            )
                            brand_name = detected

                if st.form_submit_button(
                    "🚀 **INICIAR**", use_container_width=True, type="primary"
                ):
                    if not all([
                        dossier_file, region_file, internet_file,
                        brand_name.strip()
                    ]):
                        st.error("❌ Faltan datos obligatorios.")
                    else:
                        aliases = [
                            a.strip()
                            for a in brand_aliases_text.split(";")
                            if a.strip()
                        ]
                        asyncio.run(run_full_process_async(
                            dossier_file, region_file, internet_file,
                            brand_name, aliases,
                            tono_pkl_file, tema_pkl_file, analysis_mode
                        ))
                        st.rerun()
        else:
            st.markdown("## 🎉 Análisis Completado")
            c1, c2, c3, c4, c5 = st.columns(5)

            c1.markdown(
                f'<div class="metric-card"><div class="metric-value">'
                f'{st.session_state.total_rows}</div>'
                f'<div class="metric-label">Total</div></div>',
                unsafe_allow_html=True
            )
            c2.markdown(
                f'<div class="metric-card"><div class="metric-value" '
                f'style="color:green;">{st.session_state.unique_rows}</div>'
                f'<div class="metric-label">Únicas</div></div>',
                unsafe_allow_html=True
            )
            c3.markdown(
                f'<div class="metric-card"><div class="metric-value" '
                f'style="color:orange;">{st.session_state.duplicates}</div>'
                f'<div class="metric-label">Duplicados</div></div>',
                unsafe_allow_html=True
            )
            c4.markdown(
                f'<div class="metric-card"><div class="metric-value" '
                f'style="color:blue;">{st.session_state.process_duration}'
                f'</div><div class="metric-label">Tiempo</div></div>',
                unsafe_allow_html=True
            )
            c5.markdown(
                f'<div class="metric-card"><div class="metric-value" '
                f'style="color:red;">'
                f'{st.session_state.get("process_cost", "$0.00")}</div>'
                f'<div class="metric-label">Costo Est.</div></div>',
                unsafe_allow_html=True
            )

            st.markdown('<div class="success-card">', unsafe_allow_html=True)
            st.download_button(
                "📥 **DESCARGAR INFORME**",
                data=st.session_state.output_data,
                file_name=st.session_state.output_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )

            if st.button("🔄 **Nuevo Análisis**", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        render_quick_analysis_tab()

    st.markdown(
        "<hr><div style='text-align:center;color:#666;font-size:0.8rem;'>"
        "<p>v8.0.0 | 🤖 Realizado por Johnathan Cortés ©️</p></div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
