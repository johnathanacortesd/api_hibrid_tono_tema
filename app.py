# ======================================
# Importaciones
# ======================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, NamedStyle, PatternFill
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
    page_title="Análisis de Noticias con IA (Gen 3)",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ACTUALIZACIÓN: Uso del modelo más reciente para mayor capacidad de razonamiento semántico
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-5-nano-2025-08-07" 

# Configuración de concurrencia y umbrales
CONCURRENT_REQUESTS = 50 # Aumentado ligeramente para GPT-5-nano
SIMILARITY_THRESHOLD_TONO = 0.91
SIMILARITY_THRESHOLD_TEMAS = 0.85 
SIMILARITY_THRESHOLD_SUBTEMAS_AGRUPACION = 0.86 # Umbral para agrupar textos ANTES de generar subtema
SIMILARITY_THRESHOLD_TITULOS = 0.95 

# MEJORA: Umbral para la consolidación semántica de etiquetas (Reducción de subtemas)
CONSOLIDATION_SEMANTIC_THRESHOLD = 0.82 

MAX_TOKENS_PROMPT_TXT = 4000
WINDOW_SENTENCES = 2 # Número de oraciones antes y después para el contexto

# Datos geográficos para limpieza
CIUDADES_COLOMBIA = { "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo", "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu", "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico", "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila", "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío", "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca", "vaupés", "vaupes", "vichada"}
GENTILICIOS_COLOMBIA = {"bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos", "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos", "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros", "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"}
STOPWORDS_ES = set(""" a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada """.split())

# ======================================
# Estilos CSS Modernos
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #2563eb; --secondary-color: #10b981; --bg-gradient: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%); --card-bg: #ffffff; --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); --radius: 1rem; }
        .stApp { background: var(--bg-gradient); }
        .main-header { background: linear-gradient(90deg, var(--primary-color), #1e40af); -webkit-background-clip: text; -webkit-text-fill-color: transparent; padding: 1.5rem 0; text-align: center; font-size: 3rem; font-weight: 900; letter-spacing: -0.05em; }
        .subtitle { text-align: center; color: #64748b; font-size: 1.2rem; font-weight: 500; margin-bottom: 2rem; }
        .metric-container { display: flex; gap: 1rem; justify-content: center; flex-wrap: wrap; margin-bottom: 2rem; }
        .metric-card { background: var(--card-bg); padding: 1.5rem; border-radius: var(--radius); box-shadow: var(--shadow); text-align: center; min-width: 160px; border: 1px solid #f1f5f9; transition: transform 0.2s; }
        .metric-card:hover { transform: translateY(-2px); }
        .metric-value { font-size: 2.5rem; font-weight: 800; color: #0f172a; line-height: 1; }
        .metric-label { font-size: 0.875rem; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.5rem; }
        .stButton > button { border-radius: 0.75rem; font-weight: 600; padding: 0.5rem 1rem; transition: all 0.2s; }
        .stButton > button:hover { box-shadow: 0 4px 12px rgba(37, 99, 235, 0.2); }
        div[data-testid="stStatusWidget"] { border-radius: var(--radius); }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ======================================
# Autenticacion y Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    st.markdown('<div class="main-header">🔐 Acceso Seguro Gemini-3</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("Contraseña:", type="password")
            if st.form_submit_button("Desbloquear Sistema", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "admin"):
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("❌ Credenciales inválidas")
    return False

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 4; delay = 0.5
    for attempt in range(max_retries):
        try: return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            if "RateLimit" in str(e): await asyncio.sleep(delay * 2)
            else: await asyncio.sleep(delay)
            delay *= 1.5

def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay); delay *= 2

def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def clean_text_basic(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {
        "fm": "Radio", "am": "Radio", "radio": "Radio",
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa",
        "revista": "Revista", "revistas": "Revista",
        "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"
    }
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")

@st.cache_data(ttl=7200) # Cache extendido
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto or len(texto.split()) < 2: return None
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        return resp["data"][0]["embedding"]
    except Exception: return None

# ======================================
# Agrupación de Textos (Clustering)
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    """Agrupación semántica optimizada para grandes volúmenes"""
    if not textos: return {}
    embs = [get_embedding(t) for t in textos]
    valid_indices = [i for i, e in enumerate(embs) if e is not None]
    if len(valid_indices) < 2: return {0: valid_indices} if valid_indices else {}
    
    emb_matrix = np.array([embs[i] for i in valid_indices])
    
    # AgglomerativeClustering es más estable que DBSCAN para similitud coseno directa
    clustering = AgglomerativeClustering(
        n_clusters=None, 
        distance_threshold=1 - umbral_similitud, 
        metric="cosine", 
        linkage="average"
    ).fit(emb_matrix)
    
    grupos = defaultdict(list)
    for i, label in enumerate(clustering.labels_): grupos[label].append(valid_indices[i])
    return {gid: g for gid, g in enumerate(grupos.values())}

# ======================================
# CLASIFICADOR DE TONO CONTEXTUAL V4 (GEMINI 3 EDITION)
# ======================================
class ClasificadorTonoUltraV4:
    """
    Versión 4: Enfoque 'Surgical Brand Focus'.
    Extrae oraciones completas en lugar de ventanas de caracteres.
    Prioriza la estructura sintáctica para determinar si la marca es Sujeto u Objeto.
    """
    
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        self.brand_regex_str = r"\b(" + "|".join(patterns) + r")\b" if patterns else r"(a^b)"
        self.brand_pattern = re.compile(self.brand_regex_str, re.IGNORECASE)

    def _extract_brand_sentences(self, texto: str) -> List[str]:
        """Extrae la oración donde está la marca + la anterior y la siguiente."""
        if not texto: return []
        # Dividir por oraciones de forma robusta
        sentences = re.split(r'(?<=[.!?])\s+', texto)
        indices_marca = [i for i, s in enumerate(sentences) if self.brand_pattern.search(unidecode(s.lower()))]
        
        contextos = []
        processed_indices = set()
        
        for idx in indices_marca:
            start = max(0, idx - 1)
            end = min(len(sentences), idx + 2)
            
            # Evitar duplicar segmentos si las menciones están muy cerca
            rango = tuple(range(start, end))
            if any(i in processed_indices for i in rango):
                continue
                
            fragmento = " ".join(sentences[start:end])
            contextos.append(fragmento)
            processed_indices.update(rango)
            
        if not contextos and texto: # Fallback si no se detecta (raro)
            return [texto[:600]]
        return contextos

    async def _llm_analisis_preciso(self, contextos: List[str]) -> Dict[str, str]:
        aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
        # Unimos contextos pero separados visualmente
        texto_analisis = "\n\n--- FRAGMENTO ---\n".join(contextos[:3]) 

        prompt = f"""Analiza el tono hacia la marca '{self.marca}' (alias: {aliases_str}) en los siguientes fragmentos.
        
REGLAS DE ORO (GPT-5 Logic):
1. **Rol de la Marca:** ¿Es la marca el *Sujeto* activo de un logro (Positivo) o el *Objeto* de una sanción/crítica (Negativo)?
2. **Contexto vs. Foco:** Si la noticia es mala (ej. "Sube el desempleo") pero la marca presenta una solución o ayuda, el tono para la marca es **Positivo**.
3. **Neutro:** Si solo se menciona como referencia, dato financiero sin adjetivos, o participación pasiva en eventos estándar.
4. **Crisis:** Si hay una crisis natural y la marca *responde* ayudando -> Positivo. Si la marca *causó* la crisis -> Negativo.

Fragmentos:
{texto_analisis}

Responde JSON: {{"tono": "Positivo|Negativo|Neutro", "razon": "Breve justificación en 1 frase"}}"""

        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            return {"tono": data.get("tono", "Neutro").title()}
        except Exception:
            return {"tono": "Neutro"}

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar):
        textos = textos_concat.tolist()
        n = len(textos)
        
        # Clustering inicial para reducir llamadas a API (Misiles guiados)
        progress_bar.progress(0.1, text="📡 Agrupando noticias idénticas para análisis...")
        grupos = agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO)
        
        # Mapa de representante -> lista de índices originales
        mapa_grupos = {}
        for gid, indices in grupos.items():
            # Elegimos el texto más largo del grupo como representante (más contexto)
            idx_rep = max(indices, key=lambda i: len(textos[i]))
            mapa_grupos[textos[idx_rep]] = indices
        
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        
        async def worker(texto_rep):
            async with semaphore:
                contextos = self._extract_brand_sentences(texto_rep)
                return await self._llm_analisis_preciso(contextos)

        resultados_brutos = []
        total_tasks = len(mapa_grupos)
        
        # Ejecución concurrente
        tasks = [worker(txt) for txt in mapa_grupos.keys()]
        for i, f in enumerate(asyncio.as_completed(tasks)):
            resultados_brutos.append(await f)
            if i % 5 == 0:
                progress_bar.progress(0.1 + 0.4 * (i / total_tasks), text=f"🧠 Analizando Tono: {i}/{total_tasks}")
        
        # Asignar resultados de vuelta
        resultados_finales = [None] * n
        keys = list(mapa_grupos.keys())
        for i, res in enumerate(resultados_brutos):
            indices = mapa_grupos[keys[i]]
            for idx in indices:
                resultados_finales[idx] = res
        
        return resultados_finales

# ======================================
# CLASIFICADOR DE SUBTEMA CON CONSOLIDACIÓN SEMÁNTICA V4
# ======================================
class ClasificadorSubtemaV4:
    """
    Versión 4: Enfoque 'Semantic Map-Reduce'.
    1. Genera subtemas específicos para grupos pequeños.
    2. (NUEVO) Toma la lista de TODOS los subtemas generados.
    3. (NUEVO) Clusteriza los *nombres* de los subtemas por significado.
    4. (NUEVO) Pide al LLM que cree una etiqueta canónica para cada cluster de subtemas.
    Resultado: Reducción masiva de duplicados semánticos ("Lanzamiento App" == "App Nueva") sin perder especificidad.
    """
    
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases
        self.brand_context = f"Marca: {marca}. Alias: {', '.join(aliases) if aliases else 'N/A'}."

    def _limpiar_subtema_generado(self, texto: str) -> str:
        t = texto.replace(self.marca, "").strip() # Quitar nombre marca
        for a in self.aliases: 
            if a: t = t.replace(a, "").strip()
        t = re.sub(r"^(el|la|los|las|un|una|de|del|en)\s+", "", t, flags=re.IGNORECASE)
        t = re.sub(r'[."]', '', t)
        return t.capitalize() if t else "Actividad General"

    async def _generar_subtema_batch(self, textos_representantes: List[str]) -> List[str]:
        """Genera subtemas para varios textos en una sola llamada (Ahorro de tokens)"""
        prompts_batch = []
        for i, txt in enumerate(textos_representantes):
            prompts_batch.append(f"Noticia {i+1}: {txt[:350]}")
        
        input_txt = "\n".join(prompts_batch)
        prompt = f"""{self.brand_context}
Genera un SUBTEMA muy corto (2-4 palabras) y ESPECÍFICO para cada noticia.
REGLAS:
- NO usar el nombre de la marca.
- NO usar palabras vagas como "Noticia", "Actualidad", "Informe".
- Si es un resultado financiero, pon "Resultados Financieros".
- Si es un nombramiento, pon "Nombramiento [Cargo]".

Input:
{input_txt}

Responde JSON: {{"items": ["Subtema 1", "Subtema 2", ...]}} ordenado igual que el input."""

        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate, 
                model=OPENAI_MODEL_CLASIFICACION, 
                messages=[{"role": "user", "content": prompt}], 
                max_tokens=300, 
                temperature=0.2,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content)
            return [self._limpiar_subtema_generado(s) for s in data.get("items", [])]
        except:
            return ["Tema General"] * len(textos_representantes)

    async def _consolidar_semantica_llm(self, lista_subtemas_unicos: List[str]) -> Dict[str, str]:
        """
        La joya de la corona: Toma subtemas dispersos y los unifica semánticamente.
        Ej: ["Apertura Tienda", "Inauguración Local", "Nueva Sede"] -> "Expansión de Sucursales"
        """
        if len(lista_subtemas_unicos) < 2: return {s: s for s in lista_subtemas_unicos}

        # 1. Clusterizar los strings de subtemas usando embeddings
        embeddings = [get_embedding(s) for s in lista_subtemas_unicos]
        valid_idxs = [i for i, e in enumerate(embeddings) if e is not None]
        if not valid_idxs: return {s: s for s in lista_subtemas_unicos}
        
        emb_matrix = np.array([embeddings[i] for i in valid_idxs])
        
        # Clustering agresivo para agrupar variaciones del mismo tema
        clustering = AgglomerativeClustering(
            n_clusters=None, 
            distance_threshold=1 - CONSOLIDATION_SEMANTIC_THRESHOLD, # 0.82
            metric="cosine", 
            linkage="average"
        ).fit(emb_matrix)
        
        clusters = defaultdict(list)
        for i, label in enumerate(clustering.labels_):
            real_idx = valid_idxs[i]
            clusters[label].append(lista_subtemas_unicos[real_idx])
        
        # 2. Preguntar al LLM el nombre canónico para cada cluster
        mapa_final = {s: s for s in lista_subtemas_unicos} # Default: a sí mismo
        
        async def procesar_cluster(subtemas_grupo):
            if len(subtemas_grupo) == 1: return subtemas_grupo[0], subtemas_grupo[0]
            
            prompt = f"""Unifica estos subtemas similares en UNO solo que sea representativo, breve y profesional.
            Subtemas: {', '.join(subtemas_grupo)}
            Responde JSON: {{"unificado": "..."}}"""
            
            try:
                r = await acall_with_retries(
                    openai.ChatCompletion.acreate,
                    model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=20,
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                uni = json.loads(r.choices[0].message.content).get("unificado", subtemas_grupo[0])
                return subtemas_grupo, uni
            except:
                return subtemas_grupo, subtemas_grupo[0]

        tasks = [procesar_cluster(grupo) for grupo in clusters.values()]
        resultados = await asyncio.gather(*tasks)
        
        for grupo_original, nombre_unificado in resultados:
            if isinstance(grupo_original, list):
                for sub in grupo_original:
                    mapa_final[sub] = nombre_unificado
            else:
                mapa_final[grupo_original] = nombre_unificado
                
        return mapa_final

    async def procesar_completo(self, textos: List[str], p_bar):
        n = len(textos)
        
        # PASO 1: Agrupación inicial de noticias muy parecidas (Reduce llamadas)
        p_bar.progress(0.5, "🧩 Agrupando noticias por similitud de contenido...")
        grupos_noticias = agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_SUBTEMAS_AGRUPACION)
        
        representantes = []
        mapa_grupo_indices = {} # idx_rep -> [indices originales]
        
        for idxs in grupos_noticias.values():
            rep_idx = idxs[0] # Tomamos el primero
            representantes.append(textos[rep_idx])
            mapa_grupo_indices[len(representantes)-1] = idxs

        # PASO 2: Generación de Subtemas iniciales (Batch processing)
        p_bar.progress(0.6, f"🏷️ Generando etiquetas para {len(representantes)} grupos...")
        subtemas_brutos = []
        batch_size = 10 # Procesar de a 10 para no saturar contexto
        
        for i in range(0, len(representantes), batch_size):
            batch = representantes[i:i+batch_size]
            res = await self._generar_subtema_batch(batch)
            subtemas_brutos.extend(res)
        
        # Asignar subtemas brutos a todas las noticias
        subtemas_temp = [""] * n
        for i, sub in enumerate(subtemas_brutos):
            indices_reales = mapa_grupo_indices[i]
            for idx in indices_reales:
                subtemas_temp[idx] = sub
                
        # PASO 3: Consolidación Semántica (Map-Reduce inteligente)
        p_bar.progress(0.8, "🧠 Consolidando y normalizando subtemas (Fase Gemini)...")
        unicos = list(set(subtemas_brutos))
        mapa_consolidacion = await self._consolidar_semantica_llm(unicos)
        
        subtemas_finales = [mapa_consolidacion.get(st, st) for st in subtemas_temp]
        
        p_bar.progress(0.9, "✅ Generando Temas Principales...")
        
        # PASO 4: Derivar Tema Principal desde el Subtema consolidado (Simple lookup)
        # Si ya están bien consolidados, el tema principal es fácil de inferir o agrupar
        temas_finales = self._generar_temas_desde_subtemas(subtemas_finales)
        
        return subtemas_finales, temas_finales

    def _generar_temas_desde_subtemas(self, subtemas: List[str]) -> List[str]:
        # Agrupación simple final para TEMA (más general)
        unicos = list(set(subtemas))
        if not unicos: return subtemas
        
        # Clusterizar temas generales
        embeddings = [get_embedding(s) for s in unicos]
        valid = [i for i, e in enumerate(embeddings) if e is not None]
        if len(valid) < 2: return subtemas
        
        matrix = np.array([embeddings[i] for i in valid])
        clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=1 - 0.75, metric="cosine", linkage="average").fit(matrix)
        
        # Determinar nombre del cluster
        mapa_tema = {}
        for i, label in enumerate(clustering.labels_):
            # Hack simple: el tema más corto suele ser el más general
            # O podríamos llamar al LLM otra vez, pero por eficiencia usamos heurística
            idx_real = valid[i]
            st_actual = unicos[idx_real]
            # Agrupamos temporalmente
            if label not in mapa_tema: mapa_tema[label] = []
            mapa_tema[label].append(st_actual)
            
        # Resolver nombres
        mapa_final_tema = {}
        for label, group in mapa_tema.items():
            # Elegir el nombre más representativo (el centroide o el más corto)
            # Aquí usamos el más corto como proxy de "Categoría"
            tema_nombre = sorted(group, key=len)[0]
            # Limpieza extra
            if len(tema_nombre.split()) > 4: tema_nombre = "Actualidad Corporativa"
            for item in group:
                mapa_final_tema[item] = tema_nombre.title()
                
        return [mapa_final_tema.get(st, st) for st in subtemas]

# ======================================
# Lógica de Negocio y Excel
# ======================================
def procesar_excel(wb, df_region, df_internet):
    sheet = wb.active
    headers = [c.value for c in sheet[1] if c.value]
    key_map = {norm_key(h): norm_key(h) for h in headers}
    # Mapeos estándar
    standard_keys = {
        "titulo": "Titulo", "resumen": "Resumen - Aclaracion", 
        "menciones": "Menciones - Empresa", "medio": "Medio", 
        "idnoticia": "ID Noticia", "tipodemedio": "Tipo de Medio", 
        "link_nota": "Link Nota", "link_streaming": "Link (Streaming - Imagen)"
    }
    for k, v in standard_keys.items():
        key_map[k] = norm_key(v)

    rows = []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        r_data = {}
        for i, h in enumerate(headers):
            cell = row[i]
            val = extract_link(cell) if norm_key(h) in [key_map["link_nota"], key_map["link_streaming"]] else cell.value
            r_data[norm_key(h)] = val
        rows.append(r_data)
    
    # Explosión de menciones y normalización
    processed_rows = []
    region_dict = dict(zip(df_region.iloc[:,0].astype(str).str.lower().str.strip(), df_region.iloc[:,1]))
    internet_dict = dict(zip(df_internet.iloc[:,0].astype(str).str.lower().str.strip(), df_internet.iloc[:,1]))

    for idx, r in enumerate(rows):
        menciones = str(r.get(key_map["menciones"], "")).split(";")
        medio_raw = str(r.get(key_map["medio"], "")).lower().strip()
        
        base_row = r.copy()
        base_row["original_index"] = idx
        base_row[key_map["tipodemedio"]] = normalizar_tipo_medio(str(base_row.get(key_map["tipodemedio"])))
        
        # Mapeos
        if medio_raw in internet_dict:
            base_row[key_map["medio"]] = internet_dict[medio_raw]
            base_row[key_map["tipodemedio"]] = "Internet"
        
        base_row["Region_Calc"] = region_dict.get(medio_raw, "N/A") # Columna interna

        # Corrección Links
        tm = base_row[key_map["tipodemedio"]]
        ln = base_row.get(key_map["link_nota"])
        ls = base_row.get(key_map["link_streaming"])
        
        if tm == "Internet" and isinstance(ls, dict) and ls.get("url"):
            base_row[key_map["link_nota"]] = ls
            base_row[key_map["link_streaming"]] = {"value": "", "url": None}
        elif tm in ["Radio", "Televisión"]:
            base_row[key_map["link_streaming"]] = {"value": "", "url": None}

        for m in menciones:
            if not m.strip(): continue
            new_r = base_row.copy()
            new_r[key_map["menciones"]] = m.strip()
            new_r["is_duplicate"] = False
            processed_rows.append(new_r)
            
    # Deduplicación simplificada (por URL/Titulo)
    seen_keys = {}
    for r in processed_rows:
        # Clave única: ID Noticia + Mención
        key = (str(r.get(key_map["idnoticia"])), r[key_map["menciones"]])
        if key in seen_keys:
            r["is_duplicate"] = True
            r["idduplicada"] = seen_keys[key]
        else:
            seen_keys[key] = str(r.get(key_map["idnoticia"]))
            
    return processed_rows, key_map

def generar_excel_final(rows, key_map):
    wb = Workbook()
    ws = wb.active
    ws.title = "Informe IA"
    
    cols_output = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Tono IA","Tema","Subtema","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    
    # Estilos
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="4F81BD")
    ws.append(cols_output)
    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        
    link_style = NamedStyle(name="Hyperlink", font=Font(color="0000FF", underline="single"))
    
    for r in rows:
        out_row = []
        for col in cols_output:
            if col == "Region": val = r.get("Region_Calc", "")
            else:
                nk = norm_key(col)
                k = key_map.get(nk, nk)
                val = r.get(k)
            
            if isinstance(val, dict) and "url" in val:
                out_row.append(val.get("value", "Link"))
            else:
                out_row.append(val)
        
        ws.append(out_row)
        
        # Aplicar hipervínculos
        curr_row = ws.max_row
        for i, col in enumerate(cols_output):
            nk = norm_key(col)
            k = key_map.get(nk, nk)
            val = r.get(k)
            if isinstance(val, dict) and val.get("url"):
                c = ws.cell(row=curr_row, column=i+1)
                c.hyperlink = val["url"]
                c.style = link_style

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()

# ======================================
# Pipeline Principal
# ======================================
async def run_pipeline(dossier_file, region_file, internet_file, marca, alias_list, mode, pkl_files=None):
    start_t = time.time()
    
    # 1. Carga y Limpieza
    with st.status("🛠️ Fase 1: Procesando Estructura...", expanded=True) as s:
        wb = load_workbook(dossier_file, data_only=True)
        df_reg = pd.read_excel(region_file)
        df_int = pd.read_excel(internet_file)
        rows, kmap = procesar_excel(wb, df_reg, df_int)
        
        df = pd.DataFrame(rows)
        # Crear columna combinada para análisis
        df["texto_full"] = df[kmap["titulo"]].fillna("").astype(str) + ". " + df[kmap["resumen"]].fillna("").astype(str)
        
        # Separar duplicados para no gastar API
        df_analisis = df[~df["is_duplicate"]].copy()
        s.update(label="✅ Estructura lista. Duplicados marcados.", state="complete")

    # 2. Análisis IA
    if "PKL" in mode and pkl_files:
        # Lógica Legacy PKL (si se requiere)
        pass 
    
    # Modo Híbrido o API (El potente)
    if not df_analisis.empty:
        # TONO
        with st.status("🧠 Fase 2: Análisis de Tono (Motor GPT-5)", expanded=True) as s:
            p_bar = st.progress(0)
            cls_tono = ClasificadorTonoUltraV4(marca, alias_list)
            res_tono = await cls_tono.procesar_lote_async(df_analisis["texto_full"], p_bar)
            df_analisis["Tono IA"] = [r["tono"] for r in res_tono]
            s.update(label="✅ Tono Analizado con precisión contextual.", state="complete")
            
        # TEMA/SUBTEMA
        if "Solo Modelos PKL" not in mode:
            with st.status("🗂️ Fase 3: Clasificación y Consolidación Semántica", expanded=True) as s:
                p_bar = st.progress(0)
                cls_sub = ClasificadorSubtemaV4(marca, alias_list)
                # Aquí ocurre la magia de la consolidación
                subtemas, temas = await cls_sub.procesar_completo(df_analisis["texto_full"].tolist(), p_bar)
                
                df_analisis["Subtema"] = subtemas
                df_analisis["Tema"] = temas
                s.update(label=f"✅ Clasificación completada. {len(set(subtemas))} subtemas únicos consolidados.", state="complete")
    
    # 3. Merge y Export
    df_final = df.set_index("original_index")
    df_analisis = df_analisis.set_index("original_index")
    
    df_final.update(df_analisis[["Tono IA", "Tema", "Subtema"]])
    
    # Rellenar duplicados con datos de la original
    # (Simplificado: se marcan como Duplicada en el excel final por lógica de negocio)
    final_rows = df_final.reset_index().to_dict("records")
    for r in final_rows:
        if r["is_duplicate"]:
            r["Tono IA"] = "Duplicada"
            r["Tema"] = "Duplicada"
            r["Subtema"] = "Duplicada"
            
    excel_bytes = generar_excel_final(final_rows, kmap)
    
    return excel_bytes, len(df), len(df_analisis), time.time() - start_t

# ======================================
# Interfaz Streamlit
# ======================================
def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">Gemini 3 News Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Sistema de Clasificación Semántica Avanzada (Gen 5)</div>', unsafe_allow_html=True)

    with st.container():
        col1, col2 = st.columns([1, 2])
        with col1:
            st.markdown("### 📂 Datos de Entrada")
            f_dossier = st.file_uploader("Dossier (.xlsx)", type="xlsx")
            f_region = st.file_uploader("Regiones (.xlsx)", type="xlsx")
            f_internet = st.file_uploader("Fuentes Online (.xlsx)", type="xlsx")
            
        with col2:
            st.markdown("### ⚙️ Configuración del Motor")
            c_marca = st.text_input("Marca Principal", placeholder="Ej: Ecopetrol")
            c_alias = st.text_area("Alias y Voceros (separar por ;)", placeholder="Ej: Ricardo Roa;Eco;Refinería", height=68)
            mode = st.radio("Modo de Análisis", ["Full AI (GPT-5 Precision)", "Híbrido (PKL + AI)"], horizontal=True)
            
            btn_run = st.button("🚀 Ejecutar Análisis Profundo", type="primary", use_container_width=True)

    if btn_run and f_dossier and f_region and f_internet and c_marca:
        try:
            # Configurar OpenAI
            if "OPENAI_API_KEY" in st.secrets:
                openai.api_key = st.secrets["OPENAI_API_KEY"]
            else:
                st.error("Falta la API Key de OpenAI en Secrets")
                st.stop()
                
            aliases = [a.strip() for a in c_alias.split(";") if a.strip()]
            
            # Ejecutar Async
            result_data, total, unicas, tiempo = asyncio.run(run_pipeline(
                f_dossier, f_region, f_internet, c_marca, aliases, mode
            ))
            
            st.markdown("---")
            st.success("🎉 Análisis Completado")
            
            mc1, mc2, mc3 = st.columns(3)
            mc1.markdown(f'<div class="metric-card"><div class="metric-value">{total}</div><div class="metric-label">Noticias Totales</div></div>', unsafe_allow_html=True)
            mc2.markdown(f'<div class="metric-card"><div class="metric-value">{unicas}</div><div class="metric-label">Analizadas (Únicas)</div></div>', unsafe_allow_html=True)
            mc3.markdown(f'<div class="metric-card"><div class="metric-value">{int(tiempo)}s</div><div class="metric-label">Tiempo Ejecución</div></div>', unsafe_allow_html=True)
            
            st.download_button(
                label="📥 Descargar Informe Mejorado",
                data=result_data,
                file_name=f"Informe_Gemini3_{c_marca}_{datetime.date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
        except Exception as e:
            st.error(f"Error crítico: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()
