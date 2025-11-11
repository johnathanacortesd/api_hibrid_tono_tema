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
import gc     # Importación para el recolector de basura

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias con IA",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# MEJORA 1: Actualización del modelo y parámetros
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14" # Modelo actualizado

CONCURRENT_REQUESTS = 40
SIMILARITY_THRESHOLD_TONO = 0.92
SIMILARITY_THRESHOLD_TEMAS = 0.85 # Mantenido para consolidación de temas
SIMILARITY_THRESHOLD_SUBTEMAS = 0.88 # Nuevo umbral para agrupación inicial de subtemas
SIMILARITY_THRESHOLD_TITULOS = 0.95 
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 150 # Ventana de contexto ampliada
NUM_TEMAS_PRINCIPALES = 25 
NUM_SUBTEMAS_MAXIMOS = 25      # Para <= 500 noticias
NUM_SUBTEMAS_MAXIMOS_LARGE = 35 # Para > 500 noticias

# Lista de ciudades y gentilicios colombianos para filtrar
CIUDADES_COLOMBIA = { "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo", "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu", "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico", "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila", "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío", "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca", "vaupés", "vaupes", "vichada"}
GENTILICIOS_COLOMBIA = {"bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos", "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos", "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros", "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"}

# ======================================
# Lexicos y patrones para analisis de tono
# ======================================
STOPWORDS_ES = set(""" a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada """.split())
POS_VARIANTS = [ r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)", r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)", r"apertur(a|ar|ara|o|an)", r"estren(a|o|ara|an|ando)", r"habilit(a|o|ara|an|ando)", r"disponible", r"mejor(a|o|an|ando)", r"optimiza|amplia|expande", r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?", r"crecimi?ento|aument(a|o|an|ando)", r"gananci(a|as)|utilidad(es)?|benefici(o|os)", r"expansion|crece|crecer", r"inversion|invierte|invertir", r"innova(cion|dor|ndo)|moderniza", r"exito(so|sa)?|logr(o|os|a|an|ando)", r"reconoci(miento|do|da)|premi(o|os|ada)", r"lidera(zgo)?|lider", r"consolida|fortalece", r"oportunidad(es)?|potencial", r"solucion(es)?|resuelve", r"eficien(te|cia)", r"calidad|excelencia", r"satisfaccion|complace", r"confianza|credibilidad", r"sostenible|responsable", r"compromiso|apoya|apoyar", r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)", r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)", r"destaca(r|do|da|ndo)?", r"supera(r|ndo|cion)?", r"record|hito|milestone", r"avanza(r|do|da|ndo)?", r"benefici(a|o|ando|ar|ando)", r"importante(s)?", r"prioridad", r"bienestar", r"garantizar", r"seguridad", r"atencion", r"expres(o|ó|ando)", r"señala(r|do|ando)", r"ratific(a|o|ando|ar)"]
NEG_VARIANTS = [r"demanda|denuncia|sanciona|multa|investiga|critica", r"cae|baja|pierde|crisis|quiebra|default", r"fraude|escandalo|irregularidad", r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga", r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora", r"problema(s|tica|ico)?|dificultad(es)?", r"retras(o|a|ar|ado)", r"perdida(s)?|deficit", r"conflict(o|os)?|disputa(s)?", r"rechaz(a|o|ar|ado)", r"negativ(o|a|os|as)", r"preocupa(cion|nte|do)?", r"alarma(nte)?|alerta", r"riesgo(s)?|amenaza(s)?"]
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
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #1f77b4; --secondary-color: #2ca02c; --card-bg: #ffffff; --shadow-light: 0 2px 4px rgba(0,0,0,0.1); --border-radius: 12px; }
        .main-header { background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%); color: white; padding: 2rem; border-radius: var(--border-radius); text-align: center; font-size: 2.5rem; font-weight: 800; margin-bottom: 1.5rem; box-shadow: var(--shadow-light); }
        .subtitle { text-align: center; color: #666; font-size: 1.1rem; margin: -1rem 0 2rem 0; }
        .metric-card { background: var(--card-bg); padding: 1.2rem; border-radius: var(--border-radius); box-shadow: var(--shadow-light); text-align: center; border: 1px solid #e0e0e0; }
        .metric-value { font-size: 2rem; font-weight: bold; color: var(--primary-color); }
        .metric-label { font-size: 0.9rem; color: #666; text-transform: uppercase; }
        .success-card { background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%); padding: 1.5rem; border-radius: var(--border-radius); border: 1px solid #28a745; margin: 1rem 0; box-shadow: var(--shadow-light); }
        .stButton > button { border-radius: 8px; font-weight: 600; }
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
                    st.success("✅ Acceso autorizado."); st.balloons(); time.sleep(1.5); st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
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

def simhash(texto: str) -> int:
    if not texto: return 0
    toks = string_norm_label(texto).split()
    if not toks: return 0
    bits = [0] * 64
    for tok in toks:
        hv = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16) & ((1 << 64) - 1)
        for i in range(64): bits[i] += 1 if (hv >> i) & 1 else -1
    v = 0
    for i in range(64):
        if bits[i] >= 0: v |= (1 << i)
    return v

def hamdist(a: int, b: int) -> int:
    return bin(a ^ b).count('1')

@st.cache_data(ttl=3600)
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto: return None
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        return resp["data"][0]["embedding"]
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

def agrupar_por_resumen_puro(resumenes: List[str]) -> Dict[int, List[int]]:
    gid, grupos, used = 0, {}, set()
    norm = [string_norm_label(r or "") for r in resumenes]
    hashes = [simhash(r or "") for r in norm]
    for i in range(len(norm)):
        if i in used or not norm[i]: continue
        grupo = [i]
        used.add(i)
        for j in range(i + 1, len(norm)):
            if j in used or not norm[j]: continue
            if hamdist(hashes[i], hashes[j]) <= 8 and SequenceMatcher(None, norm[i], norm[j]).ratio() >= 0.92:
                grupo.append(j)
                used.add(j)
        if len(grupo) >= 2:
            grupos[gid] = grupo
            gid += 1
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
# MEJORA 3: NUEVO CLASIFICADOR DE TONO CONTEXTUAL
# ======================================
class ClasificadorTonoUltraV3:
    """Versión mejorada con análisis contextual enfocado en la marca"""
    
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex(marca, aliases)
    
    def _build_brand_regex(self, marca: str, aliases: List[str]) -> str:
        """Construye regex optimizado para detectar marca y alias"""
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        return r"\b(" + "|".join(patterns) + r")\b" if patterns else r"(a^b)"
    
    def _extract_brand_context(self, texto: str, window: int = 150) -> List[str]:
        """Extrae fragmentos del texto donde aparece la marca con contexto ampliado"""
        texto_lower = unidecode(texto.lower())
        contextos = []
        
        for match in re.finditer(self.brand_pattern, texto_lower, re.IGNORECASE):
            start = max(0, match.start() - window)
            end = min(len(texto_lower), match.end() + window)
            contextos.append(texto[start:end])
        
        return contextos if contextos else [texto[:500]]  # Fallback al inicio si no hay menciones
    
    def _analizar_contexto_reglas(self, contextos: List[str]) -> Optional[str]:
        """Análisis basado en reglas con contexto mejorado"""
        pos_score = 0
        neg_score = 0
        
        for contexto in contextos:
            t = unidecode(contexto.lower())
            
            # Detectar negaciones cerca de la marca
            tiene_negacion = bool(re.search(r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente)\b.{0,30}' + self.brand_pattern, t, re.IGNORECASE))
            
            # Contar patrones positivos/negativos
            pos_hits = sum(1 for p in POS_PATTERNS if p.search(t))
            neg_hits = sum(1 for p in NEG_PATTERNS if p.search(t))
            
            # Crisis con respuesta = Positivo
            if CRISIS_KEYWORDS.search(t) and RESPONSE_VERBS.search(t):
                pos_score += 3
                continue
            
            # Ajustar scores por negación
            if tiene_negacion:
                pos_score -= pos_hits
                neg_score += pos_hits
            else:
                pos_score += pos_hits
                neg_score += neg_hits
        
        # Decisión con umbral más alto
        if pos_score >= 3 and pos_score > neg_score * 1.5:
            return "Positivo"
        elif neg_score >= 3 and neg_score > pos_score * 1.5:
            return "Negativo"
        
        return None  # Requiere análisis de LLM
    
    async def _llm_refuerzo_mejorado(self, contextos: List[str]) -> Dict[str, str]:
        """LLM con prompt mejorado enfocado en la marca"""
        aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
        contextos_texto = "\n---\n".join(contextos[:3])  # Max 3 contextos
        
        prompt = f"""Eres un analista de medios experto. Analiza ÚNICAMENTE el sentimiento hacia la marca '{self.marca}' (alias: {aliases_str}) en estos fragmentos de texto.

IMPORTANTE:
- Ignora el sentimiento general del artículo
- Enfócate SOLO en cómo se presenta a la marca
- Positivo: logros, lanzamientos, reconocimientos, respuestas proactivas a crisis, declaraciones constructivas de voceros
- Negativo: críticas directas, sanciones, pérdidas, escándalos que involucren a la marca
- Neutro: menciones informativas, datos sin valoración, o si la marca no es el foco

Fragmentos de contexto:
---
{contextos_texto}
---

Responde SOLO en JSON: {{"tono":"Positivo|Negativo|Neutro", "razon":"breve explicación"}}"""

        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            tono = str(data.get("tono", "Neutro")).title()
            return {"tono": tono if tono in ["Positivo","Negativo","Neutro"] else "Neutro"}
        except Exception as e:
            return {"tono": "Neutro"}
    
    async def _clasificar_grupo_async(self, texto_representante: str, semaphore: asyncio.Semaphore):
        """Clasificación mejorada con análisis contextual"""
        async with semaphore:
            # Extraer contextos donde aparece la marca
            contextos = self._extract_brand_context(texto_representante, window=WINDOW)
            
            # Intentar clasificar con reglas primero
            tono_reglas = self._analizar_contexto_reglas(contextos)
            if tono_reglas:
                return {"tono": tono_reglas}
            
            # Si las reglas no son concluyentes, usar LLM
            return await self._llm_refuerzo_mejorado(contextos)

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar, resumen_puro: pd.Series, titulos_puros: pd.Series):
        textos, n = textos_concat.tolist(), len(textos_concat)
        progress_bar.progress(0.05, text="🔄 Agrupando noticias para análisis de tono...")
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i]); return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO), agrupar_por_titulo_similar(titulos_puros.tolist()), agrupar_por_resumen_puro(resumen_puro.tolist())]:
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
            progress_bar.progress(0.1 + 0.85 * (i + 1) / len(tasks), text=f"🎯 Analizando tono: {i+1}/{len(tasks)}")
        
        resultados_por_grupo = {list(representantes.keys())[i]: res for i, res in enumerate(resultados_brutos)}
        
        resultados_finales = [None] * n
        for cid, idxs in comp.items():
            r = resultados_por_grupo.get(cid, {"tono": "Neutro"})
            for i in idxs: resultados_finales[i] = r
        
        progress_bar.progress(1.0, text="✅ Análisis de tono completado")
        return resultados_finales

def analizar_tono_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[Dict[str, str]]]:
    try:
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        TONO_MAP = {1: "Positivo", "1": "Positivo", 0: "Neutro", "0": "Neutro", -1: "Negativo", "-1": "Negativo"}
        resultados = [{"tono": TONO_MAP.get(p, str(p).title())} for p in predicciones]
        return resultados
    except Exception as e:
        st.error(f"❌ Error al procesar `pipeline_sentimiento.pkl`: {e}")
        st.warning("El pipeline debe ser un objeto Scikit-learn que implemente `.predict()` y devuelva 1, 0, o -1.")
        return None

# ======================================
# MEJORA 4: NUEVO CLASIFICADOR DE SUBTEMAS CON CONSOLIDACIÓN
# ======================================
class ClasificadorSubtemaV2:
    """Clasificador de subtemas con post-procesamiento inteligente"""
    
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
    
    def _generar_subtema_para_grupo(self, textos_muestra: List[str]) -> str:
        """Generación de subtema mejorada con instrucciones más precisas"""
        prompt = f"""Eres un analista de medios. Genera un SUBTEMA específico (3-5 palabras) que describa el tema común de estas noticias.

REGLAS ESTRICTAS:
- NO incluir: '{self.marca}', ciudades colombianas, gentilicios, ni alias de la marca
- SER ESPECÍFICO pero no redundante
- Usar lenguaje periodístico profesional
- Si son sobre un mismo evento, usar el nombre del evento
- Si son sobre un tipo de actividad, usar la categoría

Textos:
---
{chr(10).join([m[:400] for m in textos_muestra[:5]])}
---

Responde SOLO en JSON: {{"subtema":"..."}}"""

        try:
            resp = call_with_retries(
                openai.ChatCompletion.create, 
                model=OPENAI_MODEL_CLASIFICACION, 
                messages=[{"role": "user", "content": prompt}], 
                max_tokens=30, 
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            return limpiar_tema_geografico(limpiar_tema(data.get("subtema", "Sin tema")), self.marca, self.aliases)
        except Exception:
            return limpiar_tema(" ".join(string_norm_label(" ".join(textos_muestra)).split()[:4]) or "Actividad Empresarial")
    
    def _agrupar_subtemas_similares(self, subtemas: List[str], max_subtemas: int) -> Dict[str, str]:
        """Agrupa subtemas similares usando embeddings y similitud léxica"""
        subtema_counts = Counter(subtemas)
        subtemas_unicos = list(subtema_counts.keys())
        
        if len(subtemas_unicos) <= max_subtemas:
            return {st: st for st in subtemas_unicos}
        
        # Calcular embeddings
        emb_dict = {st: get_embedding(st) for st in subtemas_unicos if st != "Sin tema"}
        subtemas_validos = [st for st, emb in emb_dict.items() if emb is not None]
        
        if not subtemas_validos:
            return {st: st for st in subtemas_unicos}
        
        # Matriz de similitud combinada (embeddings + léxica)
        n = len(subtemas_validos)
        sim_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(i+1, n):
                # Similitud de embeddings
                emb_sim = cosine_similarity([emb_dict[subtemas_validos[i]]], [emb_dict[subtemas_validos[j]]])[0][0]
                
                # Similitud léxica (SequenceMatcher)
                lex_sim = SequenceMatcher(None, 
                                         string_norm_label(subtemas_validos[i]), 
                                         string_norm_label(subtemas_validos[j])).ratio()
                
                # Combinar (70% embeddings, 30% léxica)
                combined_sim = 0.7 * emb_sim + 0.3 * lex_sim
                sim_matrix[i][j] = combined_sim
                sim_matrix[j][i] = combined_sim
        
        # Clustering jerárquico
        # Calculamos la distancia como 1 - similitud para el clustering
        distance_matrix = 1 - sim_matrix
        
        # Asegurarnos que la matriz de distancias es válida para el clustering
        np.fill_diagonal(distance_matrix, 0)
        distance_matrix[distance_matrix < 0] = 0

        clustering = AgglomerativeClustering(
            n_clusters=max_subtemas, 
            metric='precomputed', 
            linkage='average'
        ).fit(distance_matrix)
        
        # Mapear cada subtema al representante de su cluster
        mapa_subtemas = {}
        for cluster_id in range(max_subtemas):
            indices_en_cluster = [i for i, label in enumerate(clustering.labels_) if label == cluster_id]
            if not indices_en_cluster:
                continue
            
            # Elegir el subtema más frecuente como representante
            subtemas_cluster = [subtemas_validos[i] for i in indices_en_cluster]
            representante = max(subtemas_cluster, key=lambda x: subtema_counts[x])
            
            for subtema in subtemas_cluster:
                mapa_subtemas[subtema] = representante
        
        # Mapear "Sin tema" y cualquier otro subtema no clusterizado a sí mismo
        for st in subtemas_unicos:
            if st not in mapa_subtemas:
                 mapa_subtemas[st] = st
        
        return mapa_subtemas
    
    def procesar_lote(self, df_columna_resumen: pd.Series, progress_bar, resumen_puro: pd.Series, titulos_puros: pd.Series) -> List[str]:
        """Procesamiento completo con post-agrupación"""
        textos, n = df_columna_resumen.tolist(), len(df_columna_resumen)
        progress_bar.progress(0.10, "🔍 Preparando agrupaciones para subtemas...")
        
        # Agrupación inicial (DSU como antes)
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i]); return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        
        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_SUBTEMAS), 
                  agrupar_por_titulo_similar(titulos_puros.tolist()), 
                  agrupar_por_resumen_puro(resumen_puro.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        
        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        
        # Generar subtemas iniciales
        mapa_idx_a_subtema, total_comp = {}, len(comp)
        for hechos, (cid, idxs) in enumerate(comp.items(), 1):
            muestra_textos = [textos[i] for i in idxs[:5]]
            subtema = self._generar_subtema_para_grupo(muestra_textos)
            for i in idxs: mapa_idx_a_subtema[i] = subtema
            progress_bar.progress(0.1 + 0.4 * hechos / max(total_comp, 1), f"🏷️ Subtemas iniciales: {hechos}/{total_comp}")
        
        # POST-PROCESAMIENTO: Agrupar subtemas similares
        subtemas_iniciales = [mapa_idx_a_subtema.get(i, "Sin tema") for i in range(n)]
        max_subtemas = NUM_SUBTEMAS_MAXIMOS_LARGE if n > 500 else NUM_SUBTEMAS_MAXIMOS
        
        progress_bar.progress(0.6, "🔄 Consolidando subtemas similares...")
        mapa_consolidacion = self._agrupar_subtemas_similares(subtemas_iniciales, max_subtemas)
        
        # Aplicar consolidación
        subtemas_finales = [mapa_consolidacion.get(st, st) for st in subtemas_iniciales]
        
        progress_bar.progress(1.0, f"✅ {len(set(subtemas_finales))} subtemas únicos generados")
        return subtemas_finales

# Se mantiene la clase original por la instrucción "no refactorices ni quites funciones",
# aunque su uso para subtemas es reemplazado por ClasificadorSubtemaV2.
class ClasificadorTemaDinamico:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca, self.aliases = marca, aliases or []

    def _generar_subtema_para_grupo(self, textos_muestra: List[str]) -> str:
        prompt = (f"Genere un subtema específico y preciso (2-6 palabras) para estas noticias. No incluya la marca '{self.marca}', ciudades o gentilicios de Colombia.\n"
                  f"Textos:\n---\n" + "\n---\n".join([m[:500] for m in textos_muestra]) + '\n---\nResponda solo en JSON: {"subtema":"..."}')
        try:
            resp = call_with_retries(openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": prompt}], max_tokens=40, temperature=0.05, response_format={"type": "json_object"})
            data = json.loads(resp.choices[0].message.content.strip())
            return limpiar_tema_geografico(limpiar_tema(data.get("subtema", "Sin tema")), self.marca, self.aliases)
        except Exception:
            return limpiar_tema(" ".join(string_norm_label(" ".join(textos_muestra)).split()[:4]) or "Actividad Empresarial")

    def procesar_lote(self, df_columna_resumen: pd.Series, progress_bar, resumen_puro: pd.Series, titulos_puros: pd.Series) -> List[str]:
        textos, n = df_columna_resumen.tolist(), len(df_columna_resumen)
        progress_bar.progress(0.10, "🔍 Preparando agrupaciones para subtemas...")
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i]); return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        dsu = DSU(n)
        for g in [agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TEMAS), agrupar_por_titulo_similar(titulos_puros.tolist()), agrupar_por_resumen_puro(resumen_puro.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        
        mapa_idx_a_subtema, total_comp = {}, len(comp)
        for hechos, (cid, idxs) in enumerate(comp.items(), 1):
            muestra_textos = [textos[i] for i in idxs[:5]]
            subtema = self._generar_subtema_para_grupo(muestra_textos)
            for i in idxs: mapa_idx_a_subtema[i] = subtema
            progress_bar.progress(0.1 + 0.5 * hechos / max(total_comp, 1), f"🏷️ Subtemas creados: {hechos}/{total_comp}")
        
        return [mapa_idx_a_subtema.get(i, "Sin tema") for i in range(n)]

def consolidar_subtemas_en_temas(subtemas: List[str], p_bar) -> List[str]:
    p_bar.progress(0.6, text=f"📊 Contando y filtrando subtemas...")
    subtema_counts = Counter(subtemas)
    
    subtemas_a_clusterizar = [st for st, count in subtema_counts.items() if st != "Sin tema" and count > 1]
    singletons = [st for st, count in subtema_counts.items() if st != "Sin tema" and count == 1]
    
    mapa_subtema_a_tema = {st: st for st in singletons}
    mapa_subtema_a_tema["Sin tema"] = "Sin tema"

    if not subtemas_a_clusterizar or len(subtemas_a_clusterizar) < NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No hay suficientes grupos de subtemas para consolidar. Usando subtemas como temas.")
        for st in subtemas_a_clusterizar:
            mapa_subtema_a_tema[st] = st
        return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

    p_bar.progress(0.7, f"🔄 Agrupando {len(subtemas_a_clusterizar)} subtemas frecuentes...")
    emb_subtemas = {st: get_embedding(st) for st in subtemas_a_clusterizar}
    subtemas_validos = [st for st, emb in emb_subtemas.items() if emb is not None]
    
    if len(subtemas_validos) < NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No hay suficientes subtemas con embeddings para consolidar.")
        for st in subtemas_a_clusterizar:
            mapa_subtema_a_tema[st] = st
        return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

    emb_matrix = np.array([emb_subtemas[st] for st in subtemas_validos])
    clustering = AgglomerativeClustering(n_clusters=NUM_TEMAS_PRINCIPALES, metric="cosine", linkage="average").fit(emb_matrix)
    
    del emb_subtemas; gc.collect()

    mapa_cluster_a_subtemas = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        mapa_cluster_a_subtemas[label].append(subtemas_validos[i])

    p_bar.progress(0.8, "🧠 Generando nombres para los temas principales...")
    mapa_temas_finales = {}
    for cluster_id, lista_subtemas in mapa_cluster_a_subtemas.items():
        prompt = (
            "Eres un analista de medios experto en categorizar contenido noticioso. A partir de la siguiente lista de subtemas detallados, genera un nombre de TEMA principal (2-4 palabras) que los agrupe de forma lógica y descriptiva.\n"
            "El tema debe ser útil para un informe ejecutivo. Evita términos vagos como 'Noticias', 'Anuncios' o 'Actividades'.\n"
            "Por ejemplo, si los subtemas son 'Apertura nueva sucursal', 'Resultados financieros Q3', un mal tema es 'Actividades de la empresa'. Un buen tema es 'Expansión y Resultados Financieros'.\n\n"
            f"Subtemas a agrupar: {', '.join(lista_subtemas[:12])}\n\n"
            "Responde únicamente con el nombre del TEMA principal."
        )
        try:
            resp = call_with_retries(openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": prompt}], max_tokens=20, temperature=0.2)
            tema_principal = limpiar_tema(resp.choices[0].message.content.strip().replace('"', ''))
        except Exception:
            tema_principal = max(lista_subtemas, key=len)
        
        mapa_temas_finales[cluster_id] = tema_principal
        for subtema in lista_subtemas:
            mapa_subtema_a_tema[subtema] = tema_principal
    
    if singletons and mapa_temas_finales:
        p_bar.progress(0.9, "✨ Asignando subtemas únicos a los temas principales...")
        emb_temas_finales = {name: get_embedding(name) for name in set(mapa_temas_finales.values())}
        valid_theme_names = [name for name, emb in emb_temas_finales.items() if emb]
        emb_theme_matrix = np.array([emb_temas_finales[name] for name in valid_theme_names])

        for singleton in singletons:
            emb_singleton = get_embedding(singleton)
            if emb_singleton is not None and len(valid_theme_names) > 0:
                sims = cosine_similarity([emb_singleton], emb_theme_matrix)
                best_match_idx = np.argmax(sims)
                mapa_subtema_a_tema[singleton] = valid_theme_names[best_match_idx]

    p_bar.progress(1.0, "✅ Consolidación de temas completada.")
    return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

def analizar_temas_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[str]]:
    try:
        pipeline = joblib.load(pkl_file)
        predicciones = pipeline.predict(textos)
        return [str(p) for p in predicciones]
    except Exception as e:
        st.error(f"❌ Error al procesar el `pipeline_tema.pkl`: {e}")
        st.warning("Asegúrese que el pipeline es un objeto Scikit-learn que implementa `.predict()`.")
        return None

# ======================================
# Lógica de Duplicados y Generación de Excel
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url = {}
    seen_broadcast = {}
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
                    winner_index = seen_online_url[key]
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[winner_index].get(key_map.get("idnoticia"), "")
                    continue 
                else:
                    seen_online_url[key] = i
            
            if medio_norm and mencion_norm:
                bucket_key = (medio_norm, mencion_norm)
                online_title_buckets[bucket_key].append(i)
        
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
                if key in seen_broadcast:
                    winner_index = seen_broadcast[key]
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[winner_index].get(key_map.get("idnoticia"), "")
                else:
                    seen_broadcast[key] = i
    
    for bucket_key, indices in online_title_buckets.items():
        if len(indices) < 2: continue
        
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx1, idx2 = indices[i], indices[j]
                if processed_rows[idx1].get("is_duplicate") or processed_rows[idx2].get("is_duplicate"): continue

                titulo1 = normalize_title_for_comparison(processed_rows[idx1].get(key_map.get("titulo")))
                titulo2 = normalize_title_for_comparison(processed_rows[idx2].get(key_map.get("titulo")))

                if titulo1 and titulo2 and SequenceMatcher(None, titulo1, titulo2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    if len(titulo1) < len(titulo2):
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
    key_map.update({ "titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "tonoiai": norm_key("Tono IA"), "tema": norm_key("Tema"), "subtema": norm_key("Subtema"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"), "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"), "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region") })
    
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
    
    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})

    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    
    for row in processed_rows:
        if row["is_duplicate"]:
            row.update({key_map["tonoiai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["subtema"]: "Duplicada"})
    
    return processed_rows, key_map

def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
    tkey, ln_key, ls_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key): return
    tipo = row.get(tkey, "")
    ln, ls = row.get(ln_key) or {"value": "", "url": None}, row.get(ls_key) or {"value": "", "url": None}
    has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
    
    if tipo in ["Radio", "Televisión"]: 
        row[ls_key] = {"value": "", "url": None}
    elif tipo == "Internet": 
        row[ln_key], row[ls_key] = ls, ln
    elif tipo in ["Prensa", "Revista"]:
        if not has_url(ln) and has_url(ls): 
            row[ln_key] = ls
        row[ls_key] = {"value": "", "url": None}

def generate_output_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"
    final_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Tono IA","Tema","Subtema","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    out_sheet.append(final_order)
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    if "Hyperlink_Custom" not in out_wb.style_names: out_wb.add_named_style(link_style)
    
    for row_data in all_processed_rows:
        # Limpieza de datos antes de escribir
        titulo_key = key_map.get("titulo")
        if titulo_key and titulo_key in row_data:
            row_data[titulo_key] = clean_title_for_output(row_data.get(titulo_key))
        
        resumen_key = key_map.get("resumen")
        if resumen_key and resumen_key in row_data:
            row_data[resumen_key] = corregir_texto(row_data.get(resumen_key))

        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            nk_header = norm_key(header)
            # Usar la clave del mapa para buscar el valor
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
# MEJORA 5: NUEVA FUNCIÓN DE RE-ANÁLISIS DE SUBTEMAS
# ======================================
def reanalizador_subtemas(df: pd.DataFrame, columna_subtema: str, marca: str, aliases: List[str], max_subtemas: int = 25) -> pd.DataFrame:
    """
    Re-analiza y consolida una columna de subtemas ya existente.
    Útil si ya tienes resultados pero quieres reducir la cantidad de subtemas.
    """
    st.info("🔄 Iniciando re-análisis y consolidación de subtemas...")
    progress = st.progress(0, text="Cargando clasificador...")
    
    clasificador = ClasificadorSubtemaV2(marca, aliases)
    
    # Obtener subtemas actuales
    subtemas_actuales = df[columna_subtema].astype(str).tolist()
    
    progress.progress(0.3, text="Analizando similitudes entre subtemas existentes...")
    
    # Agrupar subtemas similares
    mapa_consolidacion = clasificador._agrupar_subtemas_similares(subtemas_actuales, max_subtemas)
    
    progress.progress(0.7, text="Aplicando nueva agrupación consolidada...")
    
    # Aplicar el mapeo
    df[columna_subtema] = df[columna_subtema].map(lambda x: mapa_consolidacion.get(str(x), str(x)))
    
    progress.progress(1.0, f"✅ Reducido a {df[columna_subtema].nunique()} subtemas únicos")
    
    return df

# ======================================
# Proceso principal y UI
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases, tono_pkl_file, tema_pkl_file, analysis_mode):
    start_time = time.time()
    
    # Solo verificar API Key si es necesaria
    if "API" in analysis_mode:
        try:
            openai.api_key = st.secrets["OPENAI_API_KEY"]
            openai.aiosession.set(None)
        except Exception:
            st.error("❌ Error: OPENAI_API_KEY no encontrado en los Secrets de Streamlit. Es necesario para el modo de análisis seleccionado.")
            st.stop()

    with st.status("📋 **Paso 1/5:** Limpieza y duplicados", expanded=True) as s:
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="✅ **Paso 1/5:** Limpieza y duplicados completados", state="complete")

    with st.status("🗺️ **Paso 2/5:** Mapeos y Normalización", expanded=True) as s:
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

        # ================== ANÁLISIS DE TONO ==================
        with st.status("🎯 **Paso 3/5:** Análisis de Tono", expanded=True) as s:
            p_bar = st.progress(0)
            
            # Opción 1: Usar PKL si el modo lo permite y el archivo existe
            if ("PKL" in analysis_mode) and tono_pkl_file:
                st.write(f"🤖 Usando `pipeline_sentimiento.pkl` para {len(rows_to_analyze)} noticias...")
                p_bar.progress(0.5)
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
                p_bar.progress(1.0)
            
            # Opción 2: Usar API si el modo lo permite
            elif ("API" in analysis_mode):
                st.write(f"🤖 Usando IA para análisis de tono de {len(rows_to_analyze)} noticias...")
                # MEJORA: Usando el nuevo clasificador de tono contextual
                clasif_tono = ClasificadorTonoUltraV3(brand_name, brand_aliases)
                resultados_tono = await clasif_tono.procesar_lote_async(df_temp["resumen_api"], p_bar, df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
            
            # Opción 3: Omitir
            else:
                resultados_tono = [{"tono": "N/A"}] * len(rows_to_analyze)
                st.write("ℹ️ Análisis de Tono omitido según el modo seleccionado.")

            df_temp[key_map["tonoiai"]] = [res["tono"] for res in resultados_tono]
            
            tonos = df_temp[key_map["tonoiai"]].value_counts()
            positivos, negativos, neutros = tonos.get("Positivo", 0), tonos.get("Negativo", 0), tonos.get("Neutro", 0)
            st.markdown(f'**Resultados de Tono:** <span style="color:green;">{positivos} Positivos</span>, <span style="color:red;">{negativos} Negativos</span>, <span style="color:gray;">{neutros} Neutros</span>', unsafe_allow_html=True)
            s.update(label="✅ **Paso 3/5:** Tono Analizado", state="complete")

        # ================== ANÁLISIS DE TEMA Y SUBTEMA ==================
        with st.status("🏷️ **Paso 4/5:** Análisis de Tema", expanded=True) as s:
            p_bar = st.progress(0)
            
            # --- SUBTEMA (Solo con API) ---
            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A (Modo Solo PKL)"] * len(rows_to_analyze)
                st.write("ℹ️ El análisis de Subtema se omite en el modo 'Solo Modelos PKL'.")
            else:
                st.write(f"🤖 Generando y consolidando Subtemas con IA para {len(rows_to_analyze)} noticias...")
                # MEJORA: Usando el nuevo clasificador de subtemas con consolidación
                clasif_subtemas = ClasificadorSubtemaV2(brand_name, brand_aliases)
                subtemas = clasif_subtemas.procesar_lote(df_temp["resumen_api"], p_bar, df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
            df_temp[key_map["subtema"]] = subtemas

            # --- TEMA ---
            # Opción 1: Usar PKL si el modo lo permite y el archivo existe
            if ("PKL" in analysis_mode) and tema_pkl_file:
                st.write(f"🤖 Usando `pipeline_tema.pkl` para generar Temas principales...")
                temas_principales = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                if temas_principales is None: st.stop()
            
            # Opción 2: Usar API si el modo lo permite (y no es solo PKL)
            elif "Solo Modelos PKL" not in analysis_mode:
                st.write(f"🤖 Usando IA para consolidar Subtemas en Temas principales...")
                temas_principales = consolidar_subtemas_en_temas(subtemas, p_bar)
            
            # Opción 3: Omitir
            else:
                temas_principales = ["N/A"] * len(rows_to_analyze)
                st.write("ℹ️ Análisis de Tema omitido según el modo seleccionado.")
            
            df_temp[key_map["tema"]] = temas_principales
            
            st.success(f"✅ **{len(set(df_temp[key_map['tema']]))}** temas principales y **{len(set(df_temp[key_map['subtema']]))}** subtemas únicos identificados")
            s.update(label="✅ **Paso 4/5:** Temas Identificados", state="complete")
        
        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"): row.update(results_map.get(row["original_index"], {}))
    
    gc.collect()

    with st.status("📊 **Paso 5/5:** Generando informe final", expanded=True) as s:
        st.write("📝 Compilando resultados y generando Excel...")
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
            "process_duration": duration_str
        })
        s.update(label="✅ **Paso 5/5:** Proceso completado", state="complete")

# ======================================
# INICIO: Funciones para Análisis Rápido (OpenAI)
# ======================================
async def run_quick_analysis_async(df: pd.DataFrame, title_col: str, summary_col: str, brand_name: str, aliases: List[str]):
    df['texto_analisis'] = df[title_col].fillna('').astype(str) + ". " + df[summary_col].fillna('').astype(str)
    
    with st.status("🎯 **Paso 1/2:** Analizando Tono...", expanded=True) as s:
        p_bar = st.progress(0, "Iniciando análisis de tono...")
        # MEJORA: Usando el nuevo clasificador de tono contextual
        clasif_tono = ClasificadorTonoUltraV3(brand_name, aliases)
        resultados_tono = await clasif_tono.procesar_lote_async(df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Tono IA'] = [res["tono"] for res in resultados_tono]
        s.update(label="✅ **Paso 1/2:** Tono Analizado", state="complete")

    with st.status("🏷️ **Paso 2/2:** Analizando Tema...", expanded=True) as s:
        p_bar = st.progress(0, "Generando y consolidando subtemas...")
        # MEJORA: Usando el nuevo clasificador de subtemas con consolidación
        clasif_subtemas = ClasificadorSubtemaV2(brand_name, aliases)
        subtemas = clasif_subtemas.procesar_lote(df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Subtema'] = subtemas
        
        p_bar.progress(0.5, "Consolidando temas principales...")
        temas_principales = consolidar_subtemas_en_temas(subtemas, p_bar)
        df['Tema'] = temas_principales
        s.update(label="✅ **Paso 2/2:** Tema Analizado", state="complete")
        
    df.drop(columns=['texto_analisis'], inplace=True)
    return df

def generate_quick_analysis_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Analisis')
    return output.getvalue()

def render_quick_analysis_tab():
    st.header("Análisis Rápido con IA")
    st.info("Utiliza la API de OpenAI para un análisis avanzado de Tono, Tema y Subtema.")

    if 'quick_analysis_result' in st.session_state:
        st.success("🎉 Análisis Rápido Completado")
        st.dataframe(st.session_state.quick_analysis_result.head(10))
        
        excel_data = generate_quick_analysis_excel(st.session_state.quick_analysis_result)
        st.download_button(
            label="📥 **Descargar Resultados del Análisis Rápido**",
            data=excel_data,
            file_name=f"Analisis_Rapido_IA_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )
        if st.button("🔄 Realizar otro Análisis Rápido"):
            for key in ['quick_analysis_result', 'quick_analysis_df', 'quick_file_name']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        return

    if 'quick_analysis_df' not in st.session_state:
        st.markdown("#### Paso 1: Sube tu archivo Excel")
        quick_file = st.file_uploader("📂 **Sube tu archivo Excel**", type=["xlsx"], label_visibility="collapsed", key="quick_uploader")
        
        if quick_file:
            with st.spinner("Leyendo archivo..."):
                try:
                    st.session_state.quick_analysis_df = pd.read_excel(quick_file)
                    st.session_state.quick_file_name = quick_file.name
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ No se pudo leer el archivo. Error: {e}")
                    st.stop()
    else:
        st.success(f"✅ Archivo **'{st.session_state.quick_file_name}'** cargado correctamente.")
        st.markdown("#### Paso 2: Configura y ejecuta el análisis")
        
        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df
            columns = df.columns.tolist()
            
            st.markdown("##### ✏️ Selecciona las columnas a analizar")
            col1, col2 = st.columns(2)
            title_col = col1.selectbox("Columna de **Título**", options=columns, index=0)
            summary_index = 1 if len(columns) > 1 else 0
            summary_col = col2.selectbox("Columna de **Resumen/Contenido**", options=columns, index=summary_index)
            
            st.write("---")
            st.markdown("##### 🏢 Configuración de Marca")
            brand_name = st.text_input("**Marca Principal**", placeholder="Ej: Siemens")
            brand_aliases_text = st.text_area("**Alias y voceros** (separados por ;)", placeholder="Ej: Siemens Healthineers", height=80)
            
            submitted = st.form_submit_button("🚀 **Analizar con IA**", use_container_width=True, type="primary")

            if submitted:
                if not brand_name:
                    st.error("❌ Por favor, especifica el nombre de la marca.")
                else:
                    try:
                        openai.api_key = st.secrets["OPENAI_API_KEY"]
                        openai.aiosession.set(None)
                    except Exception:
                        st.error("❌ Error: OPENAI_API_KEY no encontrado en los Secrets de Streamlit.")
                        st.stop()

                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    df_to_process = st.session_state.quick_analysis_df.copy()
                    
                    with st.spinner("🧠 La IA está trabajando... Esto puede tardar unos minutos."):
                        result_df = asyncio.run(run_quick_analysis_async(df_to_process, title_col, summary_col, brand_name, aliases))
                    
                    st.session_state.quick_analysis_result = result_df
                    st.rerun()

        if st.button("⬅️ Cargar otro archivo"):
            for key in ['quick_analysis_df', 'quick_file_name', 'quick_analysis_result']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Análisis personalizable de Tono y Tema/Subtema</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido (IA)"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("input_form"):
                st.markdown("### 📂 Archivos de Entrada Obligatorios")
                col1, col2, col3 = st.columns(3)
                dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
                region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
                internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
                
                st.markdown("### 🏢 Configuración de Marca Obligatoria")
                brand_name = st.text_input("**Marca Principal**", placeholder="Ej: Bancolombia", key="main_brand_name")
                brand_aliases_text = st.text_area("**Alias y voceros** (separados por ;)", placeholder="Ej: Ban;Juan Carlos Mora", height=80, key="main_brand_aliases")
                
                st.markdown("### ⚙️ Modo de Análisis")
                analysis_mode = st.radio(
                    "Selecciona cómo quieres realizar el análisis:",
                    options=[
                        "Híbrido (PKL + API) (Recomendado)",
                        "Solo Modelos PKL",
                        "API de OpenAI"
                    ],
                    index=0,
                    key="analysis_mode_radio",
                    captions=[
                        "Combina tus modelos PKL con la IA. La opción más flexible y recomendada.",
                        "Usa tus modelos PKL para Tono y Tema. El Subtema se omitirá.",
                        "Usa la IA para Tono, Tema y Subtema. Potente y fácil si no tienes modelos propios."
                    ]
                )
                
                tono_pkl_file = None
                tema_pkl_file = None
                if "PKL" in analysis_mode:
                    st.markdown("#### 📥 Carga tus modelos personalizados (.pkl)")
                    col_pkl1, col_pkl2 = st.columns(2)
                    with col_pkl1:
                        tono_pkl_file = st.file_uploader("Sube `pipeline_sentimiento.pkl` para Tono", type=["pkl"])
                    with col_pkl2:
                        tema_pkl_file = st.file_uploader("Sube `pipeline_tema.pkl` para Tema", type=["pkl"])

                if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                    # Validaciones
                    error = False
                    if not all([dossier_file, region_file, internet_file, brand_name.strip()]):
                        st.error("❌ Faltan archivos obligatorios o el nombre de la marca.")
                        error = True
                    
                    if analysis_mode == "Solo Modelos PKL" and (not tono_pkl_file or not tema_pkl_file):
                        st.error("❌ Para el modo 'Solo Modelos PKL', debes subir **ambos** archivos .pkl.")
                        error = True
                    
                    if "Híbrido (PKL + API)" in analysis_mode and not tono_pkl_file and not tema_pkl_file:
                        st.error("❌ Para el modo 'Híbrido', debes subir **al menos un** archivo .pkl.")
                        error = True

                    if not error:
                        aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases, tono_pkl_file, tema_pkl_file, analysis_mode))
                        st.rerun()
        else:
            st.markdown("## 🎉 Análisis Completado Exitosamente")
            c1, c2, c3, c4 = st.columns(4)
            c1.markdown(f'<div class="metric-card"><div class="metric-value">{st.session_state.total_rows}</div><div class="metric-label">📰 Total Noticias</div></div>', unsafe_allow_html=True)
            c2.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #28a745;">{st.session_state.unique_rows}</div><div class="metric-label">✅ Únicas</div></div>', unsafe_allow_html=True)
            c3.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #ff7f0e;">{st.session_state.duplicates}</div><div class="metric-label">🔄 Duplicados</div></div>', unsafe_allow_html=True)
            c4.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #1f77b4;">{st.session_state.process_duration}</div><div class="metric-label">⏱️ Duración</div></div>', unsafe_allow_html=True)

            st.markdown('<div class="success-card">', unsafe_allow_html=True)
            st.download_button("📥 **DESCARGAR INFORME**", data=st.session_state.output_data, file_name=st.session_state.output_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
            
            # MEJORA 7: Botón de re-análisis de subtemas (CON CORRECCIÓN)
            if st.button("🔄 **Consolidar Subtemas del Resultado**", use_container_width=True):
                if 'output_data' in st.session_state:
                    with st.spinner("Re-analizando y consolidando subtemas..."):
                        # Cargar el Excel generado en memoria
                        df_result = pd.read_excel(io.BytesIO(st.session_state.output_data))
                        
                        # Determinar el número máximo de subtemas
                        max_subtemas = NUM_SUBTEMAS_MAXIMOS_LARGE if len(df_result) > 500 else NUM_SUBTEMAS_MAXIMOS

                        # Re-analizar
                        df_result = reanalizador_subtemas(
                            df=df_result, 
                            columna_subtema='Subtema',
                            marca=st.session_state.brand_name,
                            aliases=st.session_state.get('brand_aliases', []),
                            max_subtemas=max_subtemas
                        )
                        
                        # --- INICIO DE LA CORRECCIÓN ---
                        
                        # 1. Convertir el DataFrame a una lista de diccionarios. Las claves aún tienen formato de encabezado (ej. 'ID Noticia').
                        all_rows_from_df = df_result.to_dict('records')
                        
                        # 2. **Paso Clave:** Crear una nueva lista de diccionarios con las claves normalizadas (ej. 'idnoticia'), que es lo que `generate_output_excel` espera.
                        all_rows_normalized = [{norm_key(k): v for k, v in row.items()} for row in all_rows_from_df]

                        # 3. Crear un key_map simple y correcto para la función de generación.
                        #    Mapea la clave normalizada a sí misma, ya que ahora los datos tienen el formato correcto.
                        key_map = {norm_key(c): norm_key(c) for c in df_result.columns}

                        # 4. Regenerar el archivo Excel usando los datos con claves normalizadas.
                        st.session_state.output_data = generate_output_excel(all_rows_normalized, key_map)
                        
                        # --- FIN DE LA CORRECCIÓN ---
                        
                        st.success("✅ Subtemas re-analizados y consolidados. El archivo de descarga ha sido actualizado.")
                        time.sleep(2)
                        st.rerun()

            if st.button("🔄 **Nuevo Análisis Completo**", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        render_quick_analysis_tab()
    
    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v5.4.1 | Realizado por Johnathan Cortés</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
