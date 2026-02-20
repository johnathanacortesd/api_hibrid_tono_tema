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
    page_title="Análisis de Noticias con IA",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Modelos (Usando el modelo más actual, inteligente y económico de OpenAI)
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14" # Reemplaza a gpt-4.1-nano, es oficial y preciso

# Configuración de rendimiento y umbrales
CONCURRENT_REQUESTS = 50
SIMILARITY_THRESHOLD_TONO = 0.92
SIMILARITY_THRESHOLD_TITULOS = 0.95 
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 150 

# Configuración de agrupación
NUM_TEMAS_PRINCIPALES = 20  
UMBRAL_FUSION_CONTENIDO = 0.88 # Aumentado para evitar agrupar sin sentido

# Precios (Por 1 millón de tokens - Actualizado a gpt-4.1-nano-2025-04-14)
PRICE_INPUT_1M = 0.150
PRICE_OUTPUT_1M = 0.600
PRICE_EMBEDDING_1M = 0.020 

# Inicializar contadores de tokens de forma segura
if 'tokens_input' not in st.session_state: st.session_state['tokens_input'] = 0
if 'tokens_output' not in st.session_state: st.session_state['tokens_output'] = 0
if 'tokens_embedding' not in st.session_state: st.session_state['tokens_embedding'] = 0

# Listas Geográficas
CIUDADES_COLOMBIA = { "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo", "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu", "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico", "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila", "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío", "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca", "vaupés", "vaupes", "vichada"}
GENTILICIOS_COLOMBIA = {"bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos", "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos", "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros", "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"}

# Lexicos y patrones
STOPWORDS_ES = set("a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada".split())
POS_VARIANTS = [ r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)", r"nuev[oa]", r"apertur(a|ar|ara|o|an)", r"estren(a|o|ara|an|ando)", r"mejor(a|o|an|ando)", r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|colaboraci[oó]n", r"crecimi?ento|aument(a|o|an|ando)", r"gananci(a|as)|utilidad(es)?|benefici(o|os)", r"inversion", r"innova", r"exito(so|sa)?|logr(o|os|a|an)", r"reconoci(miento|do|da)|premi(o|os|ada)", r"lidera(zgo)?|lider", r"solucion(es)?|resuelve", r"sostenible|responsable", r"destaca"]
NEG_VARIANTS = [r"demanda|denuncia|sanciona|multa|investiga|critica", r"cae|baja|pierde|crisis|quiebra|default", r"fraude|escandalo|irregularidad", r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga", r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora", r"problema(s|tica|ico)?|dificultad(es)?", r"retras(o|a|ar|ado)", r"perdida(s)?|deficit", r"conflict(o|os)?|disputa(s)?", r"rechaz(a|o|ar|ado)", r"alarma(nte)?|alerta", r"riesgo(s)?|amenaza(s)?"]
CRISIS_KEYWORDS = re.compile(r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS = re.compile(r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)
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

# Filtro INTELIGENTE para Tema (Garantiza 2 a 5 palabras, sin verbos sueltos, sin marca)
def limpiar_tema_estricto(tema: str, marca: str = "", aliases: List[str] = None) -> str:
    if not tema: return "Sin tema"
    tema_lower = unidecode(tema.lower().strip().strip('"').strip("'"))
    
    # Remover marcas si existen
    if marca:
        all_brand_names = [marca.lower()] + [a.lower() for a in (aliases or []) if a]
        for b in all_brand_names:
            tema_lower = re.sub(rf'\b{re.escape(unidecode(b))}\b', '', tema_lower, flags=re.IGNORECASE)
            
    # Remover entidades geograficas
    for loc in CIUDADES_COLOMBIA.union(GENTILICIOS_COLOMBIA):
        tema_lower = re.sub(rf'\b{loc}\b', '', tema_lower, flags=re.IGNORECASE)
    
    invalid_words = ["en","de","del","la","el","y","o","con","sin","por","para","sobre", "noticias", "acerca"]
    palabras_crudas = tema_lower.split()
    
    # Limpiar conjunciones al final o al inicio
    while palabras_crudas and palabras_crudas[-1].lower() in invalid_words: palabras_crudas.pop()
    while palabras_crudas and palabras_crudas[0].lower() in invalid_words: palabras_crudas.pop(0)
    
    palabras = [p.strip().capitalize() for p in palabras_crudas if p.strip()]
    
    if len(palabras) < 2:
        return " ".join(palabras) if palabras else "Noticias Generales"
    
    # Límite ESTRICTO de 5 palabras
    tema_final = " ".join(palabras[:5])
    return tema_final

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
# Función de Embeddings (Seguimiento de Tokens)
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos: return []
    resultados = [None] * len(textos)
    
    for i in range(0, len(textos), batch_size):
        batch = textos[i:i + batch_size]
        batch_truncado = [t[:2000] if t else "" for t in batch]
        try:
            resp = call_with_retries(
                openai.Embedding.create,
                input=batch_truncado,
                model=OPENAI_MODEL_EMBEDDING
            )
            
            if isinstance(resp, dict): usage = resp.get('usage', {})
            else: usage = getattr(resp, 'usage', {})
            
            if usage:
                 total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                 st.session_state['tokens_embedding'] += total
            
            for j, emb_data in enumerate(resp["data"]):
                resultados[i + j] = emb_data["embedding"]
        except Exception:
            for j, texto in enumerate(batch):
                try:
                    resp = openai.Embedding.create(input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
                    if isinstance(resp, dict): usage = resp.get('usage', {})
                    else: usage = getattr(resp, 'usage', {})
                    if usage:
                        total = usage.get('total_tokens') if isinstance(usage, dict) else getattr(usage, 'total_tokens', 0)
                        st.session_state['tokens_embedding'] += total
                    resultados[i + j] = resp["data"][0]["embedding"]
                except:
                    resultados[i + j] = None
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
# CLASIFICADOR DE TONO (ASPECT-BASED SENTIMENT)
# ======================================
class ClasificadorTonoUltraV3:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca.strip() if marca else ""
        self.aliases = aliases or []
        self.brand_pattern = self._build_brand_regex(self.marca, aliases)
    
    def _build_brand_regex(self, marca: str, aliases: List[str]) -> str:
        if not marca: return ""
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        return r"\b(" + "|".join(patterns) + r")\b" if patterns else ""
    
    def _extract_brand_context_dynamic(self, texto: str) -> List[str]:
        if not self.brand_pattern: return [texto[:1000]] # Fallback si no hay marca
        texto_lower = unidecode(texto.lower())
        contextos = []
        matches = list(re.finditer(self.brand_pattern, texto_lower, re.IGNORECASE))
        if not matches: return [texto[:600]] 
        for i, match in enumerate(matches):
            window = 250 if i == 0 else 150
            start = max(0, match.start() - window)
            end = min(len(texto), match.end() + window)
            contextos.append(texto[start:end+1].strip())
        return list(dict.fromkeys(contextos))[:4]
    
    async def _llm_refuerzo_mejorado(self, contextos: List[str]) -> Dict[str, str]:
        entidad = self.marca if self.marca else "la entidad protagonista de la noticia"
        contextos_texto = "\n---\n".join(contextos[:3])
        
        # PROMPT MEJORADO: Precisión estricta en base al cliente
        prompt = f"""Eres un analista experto en reputación corporativa. 
Evalúa el sentimiento de la noticia ESTRICTAMENTE hacia '{entidad}'.
REGLA DE ORO: Si la noticia reporta un problema o crisis general ajena a '{entidad}', pero '{entidad}' ayuda, lanza un proyecto o lo soluciona, el tono hacia '{entidad}' es POSITIVO.

Positivo: Logros, lanzamientos, reconocimientos, premios, o respuesta activa a una crisis.
Negativo: Críticas, sanciones, pérdidas, escándalos contra '{entidad}'.
Neutro: Mención informativa o descriptiva.

Fragmentos:
---
{contextos_texto}
---
Responde SOLO en JSON: {{"tono":"Positivo|Negativo|Neutro"}}"""
        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate, 
                model=OPENAI_MODEL_CLASIFICACION, 
                messages=[{"role": "user", "content": prompt}], 
                max_tokens=20, 
                temperature=0.0, 
                response_format={"type": "json_object"}
            )
            
            if isinstance(resp, dict): usage = resp.get('usage', {})
            else: usage = getattr(resp, 'usage', {})
            
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
            return await self._llm_refuerzo_mejorado(contextos)

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar, resumen_puro: pd.Series, titulos_puros: pd.Series):
        textos, n = textos_concat.tolist(), len(textos_concat)
        progress_bar.progress(0.05, text="🔄 Agrupando noticias para consistencia de tono...")
        
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i]); return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
        dsu = DSU(n)
        
        # Agrupamos por similitud semántica y de títulos
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
            progress_bar.progress(0.1 + 0.85 * (i + 1) / len(tasks), text=f"🎯 Analizando tono: {i+1}/{len(tasks)}")
            
        resultados_por_grupo = {list(representantes.keys())[i]: res for i, res in enumerate(resultados_brutos)}
        
        # ASIGNAR EL MISMO TONO A NOTICIAS IGUALES/SIMILARES
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
        return [{"tono": TONO_MAP.get(p, str(p).title())} for p in predicciones]
    except Exception as e:
        st.error(f"❌ Error al procesar `sentimiento.pkl`: {e}"); return None

# ======================================
# CLASIFICADOR DE SUBTEMAS Y TEMAS (ESTRICTOS)
# ======================================
class ClasificadorSubtemaV3:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self.cache_subtemas = {}

    def _generar_subtema_con_cache(self, textos_muestra, titulos_muestra):
        cache_key = hashlib.md5("|".join(sorted([normalize_title_for_comparison(t) for t in titulos_muestra[:3]])).encode()).hexdigest()
        if cache_key in self.cache_subtemas: return self.cache_subtemas[cache_key]
        
        palabras_titulos = []
        for t in titulos_muestra[:5]: palabras_titulos.extend([w for w in string_norm_label(t).split() if w not in STOPWORDS_ES and len(w)>3])
        keywords = " ".join([w for w, c in Counter(palabras_titulos).most_common(5)])
        
        # PROMPT MEJORADO: Exigir 2 a 5 palabras estrictamente
        prompt = f"""Genera un TEMA periodístico (ENTRE 2 Y 5 PALABRAS MÁXIMO) para agrupar estas noticias.
        TÍTULOS: {chr(10).join([f'- {t[:100]}' for t in titulos_muestra[:5]])}
        RESTRICCIONES ESTRICTAS: 
        1. NO usar el nombre de marcas.
        2. NO usar ciudades ni países.
        3. NO usar verbos vagos. 
        4. SER CONCRETO y en formato Title Case (Ej: 'Resultados Financieros Trimestrales').
        JSON: {{"subtema":"..."}}"""
        
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create, 
                model=OPENAI_MODEL_CLASIFICACION, 
                messages=[{"role": "user", "content": prompt}], 
                max_tokens=25, 
                temperature=0.1, 
                response_format={"type": "json_object"}
            )
            
            if isinstance(resp, dict): usage = resp.get('usage', {})
            else: usage = getattr(resp, 'usage', {})
            
            if usage:
                pt = usage.get('prompt_tokens') if isinstance(usage, dict) else getattr(usage, 'prompt_tokens', 0)
                ct = usage.get('completion_tokens') if isinstance(usage, dict) else getattr(usage, 'completion_tokens', 0)
                st.session_state['tokens_input'] += pt
                st.session_state['tokens_output'] += ct

            subtema_raw = json.loads(resp.choices[0].message.content.strip()).get("subtema", "Noticias Generales")
            # Aplicar filtro inteligente Python para asegurar cumplimiento
            subtema_limpio = limpiar_tema_estricto(subtema_raw, self.marca, self.aliases)
            
            self.cache_subtemas[cache_key] = subtema_limpio; return subtema_limpio
        except: return "Actividad Corporativa"

    def procesar_lote(self, df_columna_resumen: pd.Series, progress_bar, resumen_puro: pd.Series, titulos_puros: pd.Series) -> List[str]:
        textos, titulos = df_columna_resumen.tolist(), titulos_puros.tolist()
        n = len(textos)
        
        progress_bar.progress(0.1, "⚡ Agrupando noticias por similitud temática...")
        
        class DSU:
            def __init__(self, n): self.p = list(range(n))
            def find(self, i):
                if self.p[i] == i: return i
                self.p[i] = self.find(self.p[i]); return self.p[i]
            def union(self, i, j): self.p[self.find(j)] = self.find(i)
            
        dsu = DSU(n)
        grupos_semanticos = agrupar_textos_similares(textos, 0.85) # Umbral estricto para no agrupar temas dispares
        for idxs in grupos_semanticos.values():
            for j in idxs[1:]: dsu.union(idxs[0], j)
            
        comp = defaultdict(list)
        for i in range(n): comp[dsu.find(i)].append(i)
        
        mapa_subtemas = {}
        total_grupos = len(comp)
        
        for k, (lid, idxs) in enumerate(comp.items()):
            if k % 10 == 0: progress_bar.progress(0.4 + 0.5 * (k/total_grupos), f"🏷️ Generando temas {k}/{total_grupos}")
            
            # ASIGNAR EL MISMO TEMA A NOTICIAS IGUALES/SIMILARES
            subtema = self._generar_subtema_con_cache([textos[i] for i in idxs], [titulos[i] for i in idxs])
            for i in idxs: mapa_subtemas[i] = subtema
            
        subtemas_brutos = [mapa_subtemas.get(i, "Noticias Generales") for i in range(n)]
        
        progress_bar.progress(1.0, "✅ Temas generados y asignados")
        return subtemas_brutos

def consolidar_subtemas_en_temas(subtemas: List[str], textos: List[str], p_bar) -> List[str]:
    # Como la agrupación de Subtemas ahora es tan estricta (2-5 palabras), 
    # podemos usar los mismos subtemas como temas para mantener la alta precisión.
    return subtemas

def analizar_temas_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> Optional[List[str]]:
    try:
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"❌ Error al procesar el `tema.pkl`: {e}"); return None

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
    
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]: row.update({key_map["tonoiai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["subtema"]: "Duplicada"})
    return processed_rows, key_map

def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
    tkey, ln_key, ls_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key): return
    tipo = row.get(tkey, "")
    ln, ls = row.get(ln_key) or {"value": "", "url": None}, row.get(ls_key) or {"value": "", "url": None}
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
# Proceso principal y UI
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases, tono_pkl_file, tema_pkl_file, analysis_mode):
    # Reset counters
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0
    
    start_time = time.time()
    if "API" in analysis_mode:
        try: 
            openai.api_key = st.secrets["OPENAI_API_KEY"]
            openai.aiosession.set(None) # Estilo antiguo aiohttp
        except Exception: st.error("❌ Error: OPENAI_API_KEY no encontrado."); st.stop()

    with st.status("📋 **Paso 1/5:** Limpieza y duplicados", expanded=True) as s:
        all_processed_rows, key_map = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="✅ **Paso 1/5:** Limpieza completada", state="complete")

    with st.status("🗺️ **Paso 2/5:** Mapeos y Normalización", expanded=True) as s:
        df_region = pd.read_excel(region_file); region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
        df_internet = pd.read_excel(internet_file); internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
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

        with st.status("🎯 **Paso 3/5:** Análisis de Tono", expanded=True) as s:
            p_bar = st.progress(0)
            if ("PKL" in analysis_mode) and tono_pkl_file:
                resultados_tono = analizar_tono_con_pkl(df_temp["resumen_api"].tolist(), tono_pkl_file)
                if resultados_tono is None: st.stop()
            elif ("API" in analysis_mode):
                clasif_tono = ClasificadorTonoUltraV3(brand_name, brand_aliases)
                resultados_tono = await clasif_tono.procesar_lote_async(df_temp["resumen_api"], p_bar, df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
            else: resultados_tono = [{"tono": "N/A"}] * len(rows_to_analyze)
            df_temp[key_map["tonoiai"]] = [res["tono"] for res in resultados_tono]
            s.update(label="✅ **Paso 3/5:** Tono Analizado", state="complete")

        with st.status("🏷️ **Paso 4/5:** Análisis de Tema (Preciso)", expanded=True) as s:
            p_bar = st.progress(0)
            if "Solo Modelos PKL" in analysis_mode:
                subtemas = ["N/A"] * len(rows_to_analyze)
            else:
                clasif_subtemas = ClasificadorSubtemaV3(brand_name, brand_aliases)
                subtemas = clasif_subtemas.procesar_lote(df_temp["resumen_api"], p_bar, df_temp[key_map["resumen"]], df_temp[key_map["titulo"]])
                
            df_temp[key_map["subtema"]] = subtemas
            
            if ("PKL" in analysis_mode) and tema_pkl_file:
                 temas_pkl = analizar_temas_con_pkl(df_temp["resumen_api"].tolist(), tema_pkl_file)
                 if temas_pkl: df_temp[key_map["tema"]] = temas_pkl
            else:
                 df_temp[key_map["tema"]] = subtemas # Mismo valor porque ahora es súper preciso

            s.update(label="✅ **Paso 4/5:** Clasificación Temática Completada", state="complete")
        
        results_map = df_temp.set_index("original_index").to_dict("index")
        for row in all_processed_rows:
            if not row.get("is_duplicate"): row.update(results_map.get(row["original_index"], {}))
    
    gc.collect()

    # Calcular Costos
    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    total_cost = cost_input + cost_output + cost_embedding
    cost_str = f"${total_cost:.4f} USD"

    with st.status("📊 **Paso 5/5:** Generando informe final", expanded=True) as s:
        duration_str = f"{time.time() - start_time:.0f}s"
        # Manejo seguro por si brand_name viene vacío (Solo PKL)
        safe_brand = brand_name.replace(' ', '_') if brand_name else "Multicliente"
        st.session_state["output_data"] = generate_output_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_IA_{safe_brand}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name": safe_brand, 
            "total_rows": len(all_processed_rows), 
            "unique_rows": len(rows_to_analyze), 
            "duplicates": len(all_processed_rows) - len(rows_to_analyze), 
            "process_duration": duration_str,
            "process_cost": cost_str
        })
        s.update(label="✅ **Paso 5/5:** Proceso completado", state="complete")

# ======================================
# Funciones para Análisis Rápido
# ======================================
async def run_quick_analysis_async(df: pd.DataFrame, title_col: str, summary_col: str, brand_name: str, aliases: List[str]):
    # Reset counters
    st.session_state['tokens_input'] = 0
    st.session_state['tokens_output'] = 0
    st.session_state['tokens_embedding'] = 0
    
    df['texto_analisis'] = df[title_col].fillna('').astype(str) + ". " + df[summary_col].fillna('').astype(str)
    
    with st.status("🎯 **Paso 1/2:** Analizando Tono hacia la marca...", expanded=True) as s:
        p_bar = st.progress(0, "Iniciando análisis de tono contextual...")
        clasif_tono = ClasificadorTonoUltraV3(brand_name, aliases)
        resultados_tono = await clasif_tono.procesar_lote_async(df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Tono IA'] = [res["tono"] for res in resultados_tono]
        s.update(label="✅ **Paso 1/2:** Tono Analizado", state="complete")

    with st.status("🏷️ **Paso 2/2:** Asignación Temática Precisa...", expanded=True) as s:
        p_bar = st.progress(0, "Generando temas concisos (2-5 palabras)...")
        clasif_subtemas = ClasificadorSubtemaV3(brand_name, aliases)
        subtemas = clasif_subtemas.procesar_lote(df["texto_analisis"], p_bar, df[summary_col].fillna(''), df[title_col].fillna(''))
        df['Subtema'] = subtemas
        df['Tema'] = subtemas
        s.update(label="✅ **Paso 2/2:** Clasificación Finalizada", state="complete")
        
    df.drop(columns=['texto_analisis'], inplace=True)
    
    # Calcular Costos
    cost_input = (st.session_state['tokens_input'] / 1_000_000) * PRICE_INPUT_1M
    cost_output = (st.session_state['tokens_output'] / 1_000_000) * PRICE_OUTPUT_1M
    cost_embedding = (st.session_state['tokens_embedding'] / 1_000_000) * PRICE_EMBEDDING_1M
    total_cost = cost_input + cost_output + cost_embedding
    st.session_state['quick_cost'] = f"${total_cost:.4f} USD"
    
    return df

def generate_quick_analysis_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Analisis')
    return output.getvalue()

def render_quick_analysis_tab():
    st.header("Análisis Rápido con IA")
    st.info("Utiliza la API de OpenAI para un análisis avanzado de Tono y Tema preciso.")
    if 'quick_analysis_result' in st.session_state:
        st.success("🎉 Análisis Rápido Completado")
        cost = st.session_state.get('quick_cost', "$0.00")
        st.metric(label="Costo Estimado", value=cost)
        
        st.dataframe(st.session_state.quick_analysis_result.head(10))
        excel_data = generate_quick_analysis_excel(st.session_state.quick_analysis_result)
        st.download_button(label="📥 **Descargar Resultados**", data=excel_data, file_name=f"Analisis_Rapido_IA.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
        if st.button("🔄 Nuevo Análisis"):
            for key in ['quick_analysis_result', 'quick_analysis_df', 'quick_file_name', 'quick_cost']: 
                if key in st.session_state: del st.session_state[key]
            st.rerun()
        return

    if 'quick_analysis_df' not in st.session_state:
        quick_file = st.file_uploader("📂 **Sube tu archivo Excel**", type=["xlsx"], label_visibility="collapsed", key="quick_uploader")
        if quick_file:
            with st.spinner("Leyendo archivo..."):
                try: st.session_state.quick_analysis_df = pd.read_excel(quick_file); st.session_state.quick_file_name = quick_file.name; st.rerun()
                except Exception as e: st.error(f"❌ Error: {e}"); st.stop()
    else:
        st.success(f"✅ Archivo **'{st.session_state.quick_file_name}'** cargado.")
        with st.form("quick_analysis_form"):
            df = st.session_state.quick_analysis_df; columns = df.columns.tolist()
            col1, col2 = st.columns(2)
            title_col = col1.selectbox("Columna **Título**", options=columns, index=0)
            summary_index = 1 if len(columns) > 1 else 0
            summary_col = col2.selectbox("Columna **Resumen**", options=columns, index=summary_index)
            st.write("---")
            brand_name = st.text_input("**Marca Principal**", placeholder="Ej: Siemens")
            brand_aliases_text = st.text_area("**Alias** (sep. por ;)", placeholder="Ej: Siemens Healthineers", height=80)
            if st.form_submit_button("🚀 **Analizar**", use_container_width=True, type="primary"):
                if not brand_name: st.error("❌ Falta nombre de marca.")
                else:
                    try: openai.api_key = st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
                    except Exception: st.error("❌ OPENAI_API_KEY no encontrada."); st.stop()
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    with st.spinner("🧠 Analizando..."):
                        st.session_state.quick_analysis_result = asyncio.run(run_quick_analysis_async(df.copy(), title_col, summary_col, brand_name, aliases))
                    st.rerun()
        if st.button("⬅️ Cargar otro"):
            for key in ['quick_analysis_df', 'quick_file_name', 'quick_analysis_result', 'quick_cost']: 
                if key in st.session_state: del st.session_state[key]
            st.rerun()

def main():
    load_custom_css()
    if not check_password(): return
    st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Clustering Semántico de Alta Precisión</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])
    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("input_form"):
                st.markdown("### 📂 Archivos de Entrada")
                col1, col2, col3 = st.columns(3)
                dossier_file = col1.file_uploader("**1. Dossier** (.xlsx)", type=["xlsx"])
                region_file = col2.file_uploader("**2. Región** (.xlsx)", type=["xlsx"])
                internet_file = col3.file_uploader("**3. Internet** (.xlsx)", type=["xlsx"])
                st.markdown("### ⚙️ Modo de Análisis")
                analysis_mode = st.radio("Selecciona modo:", options=["Híbrido (PKL + API)", "Solo Modelos PKL", "API de OpenAI"], index=0, key="analysis_mode_radio")
                
                st.markdown("### 🏢 Configuración de Marca (Opcional si usas solo PKL)")
                brand_name = st.text_input("**Marca Principal**", placeholder="Ej: Bancolombia", key="main_brand_name")
                brand_aliases_text = st.text_area("**Alias** (sep. por ;)", placeholder="Ej: Ban;Juan Carlos Mora", height=80, key="main_brand_aliases")
                
                if "PKL" in analysis_mode:
                    c1, c2 = st.columns(2)
                    tono_pkl_file = c1.file_uploader("`sentimiento.pkl`", type=["pkl"])
                    tema_pkl_file = c2.file_uploader("`tema.pkl`", type=["pkl"])
                else: tono_pkl_file, tema_pkl_file = None, None

                if st.form_submit_button("🚀 **INICIAR**", use_container_width=True, type="primary"):
                    if not all([dossier_file, region_file, internet_file]): 
                        st.error("❌ Faltan los archivos de Excel requeridos.")
                    elif "API" in analysis_mode and not brand_name.strip(): 
                        st.error("❌ El nombre de la marca es requerido si vas a usar la API.")
                    else:
                        aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases, tono_pkl_file, tema_pkl_file, analysis_mode))
                        st.rerun()
        else:
            st.markdown("## 🎉 Análisis Completado")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.markdown(f'<div class="metric-card"><div class="metric-value">{st.session_state.total_rows}</div><div class="metric-label">Total</div></div>', unsafe_allow_html=True)
            c2.markdown(f'<div class="metric-card"><div class="metric-value" style="color:green;">{st.session_state.unique_rows}</div><div class="metric-label">Únicas</div></div>', unsafe_allow_html=True)
            c3.markdown(f'<div class="metric-card"><div class="metric-value" style="color:orange;">{st.session_state.duplicates}</div><div class="metric-label">Duplicados</div></div>', unsafe_allow_html=True)
            c4.markdown(f'<div class="metric-card"><div class="metric-value" style="color:blue;">{st.session_state.process_duration}</div><div class="metric-label">Tiempo</div></div>', unsafe_allow_html=True)
            c5.markdown(f'<div class="metric-card"><div class="metric-value" style="color:red;">{st.session_state.get("process_cost", "$0.00")}</div><div class="metric-label">Costo Est.</div></div>', unsafe_allow_html=True)
            
            st.markdown('<div class="success-card">', unsafe_allow_html=True)
            st.download_button("📥 **DESCARGAR INFORME**", data=st.session_state.output_data, file_name=st.session_state.output_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
            
            if st.button("🔄 **Nuevo Análisis**", use_container_width=True):
                pwd = st.session_state.get("password_correct"); st.session_state.clear(); st.session_state.password_correct = pwd; st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
    with tab2: render_quick_analysis_tab()
    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.8rem;'><p>v7.5 | 🤖 Análisis Inteligente de Extracción de Aspectos</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
