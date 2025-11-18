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
    page_title="Análisis de Noticias con IA",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# MODELO SOLICITADO
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

# CONFIGURACIÓN
CONCURRENT_REQUESTS = 40
SIMILARITY_THRESHOLD_TONO = 0.92      
SIMILARITY_THRESHOLD_TITULOS = 0.95   
SIMILARITY_THRESHOLD_RESUMEN = 0.90   
CONSOLIDATION_SEMANTIC_THRESHOLD = 0.85 
MAX_TOKENS_PROMPT_TXT = 4000
NUM_TEMAS_PRINCIPALES = 25 

# DATOS GEOGRÁFICOS
CIUDADES_COLOMBIA = { "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo", "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu", "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico", "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila", "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío", "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca", "vaupés", "vaupes", "vichada"}
GENTILICIOS_COLOMBIA = {"bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos", "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos", "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros", "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"}
STOPWORDS_ES = set(""" a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada """.split())

# ======================================
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #1f77b4; --secondary-color: #2ca02c; --card-bg: #ffffff; --shadow-light: 0 4px 6px rgba(0,0,0,0.1); --border-radius: 12px; }
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
# Utilidades
# ======================================
def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str): return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match: text = text[match.start():]
    if text and not text.endswith("..."): text = text.rstrip(".") + "..."
    return text

def formato_oracion(texto: str) -> str:
    if not texto: return "Sin tema"
    t = texto.strip().lower()
    return t[0].upper() + t[1:] if len(t) > 0 else t

def limpiar_tema(tema: str) -> str:
    if not tema: return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    invalid_words = ["en","de","del","la","el","y","o","con","sin","por","para","sobre"]
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_words: palabras.pop()
    tema = " ".join(palabras)
    if len(tema.split()) > 8: tema = " ".join(tema.split()[:8])
    return formato_oracion(tema)

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
    frases_geograficas = ["en colombia", "de colombia", "del pais", "en el pais", "nacional", "colombiano", "territorio nacional"]
    for frase in frases_geograficas:
        tema_lower = re.sub(rf'\b{re.escape(frase)}\b', '', tema_lower, flags=re.IGNORECASE)
    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
    if not palabras: return "Sin tema"
    return limpiar_tema(" ".join(palabras))

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
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión",
        "diario": "Prensa", "prensa": "Prensa",
        "revista": "Revista", "revistas": "Revista",
        "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"
    }
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")

def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str): return ""
    return re.sub(r"\W+", " ", str(title).lower()).strip()

def clean_title_for_output(title: Any) -> str:
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()

# ======================================
# Conexión API
# ======================================
async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 4; delay = 0.5
    for attempt in range(max_retries):
        try: return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            await asyncio.sleep(delay); delay *= 1.5

def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay); delay *= 2

@st.cache_data(ttl=3600)
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto or len(texto.split()) < 2: return None
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        return resp["data"][0]["embedding"]
    except: return None

# ======================================
# LÓGICA DE AGRUPACIÓN UNIFICADA (DSU)
# ======================================
class DSU:
    def __init__(self, n): self.p = list(range(n))
    def find(self, i):
        if self.p[i] == i: return i
        self.p[i] = self.find(self.p[i])
        return self.p[i]
    def union(self, i, j):
        root_i, root_j = self.find(i), self.find(j)
        if root_i != root_j: self.p[root_j] = root_i

def agrupar_noticias_robustas(df: pd.DataFrame, col_titulo: str, col_resumen: str) -> Dict[int, List[int]]:
    n = len(df)
    dsu = DSU(n)
    titulos = df[col_titulo].fillna("").astype(str).tolist()
    resumenes = df[col_resumen].fillna("").astype(str).tolist()
    
    # 1. Por Título
    norm_t = [normalize_title_for_comparison(t) for t in titulos]
    bloques = defaultdict(list)
    for i, t in enumerate(norm_t):
        if len(t) > 5: bloques[t[:15]].append(i)
    
    for idxs in bloques.values():
        if len(idxs) < 2: continue
        for i in range(len(idxs)):
            for j in range(i+1, len(idxs)):
                if SequenceMatcher(None, norm_t[idxs[i]], norm_t[idxs[j]]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    dsu.union(idxs[i], idxs[j])

    # 2. Por Resumen
    norm_r = [normalize_title_for_comparison(r) for r in resumenes]
    bloques_r = defaultdict(list)
    for i, r in enumerate(norm_r):
        if len(r) > 10: bloques_r[r[:20]].append(i)

    for idxs in bloques_r.values():
        if len(idxs) < 2: continue
        for i in range(len(idxs)):
            for j in range(i+1, len(idxs)):
                if dsu.find(idxs[i]) == dsu.find(idxs[j]): continue
                if SequenceMatcher(None, norm_r[idxs[i]], norm_r[idxs[j]]).ratio() >= SIMILARITY_THRESHOLD_RESUMEN:
                    dsu.union(idxs[i], idxs[j])

    grupos_finales = defaultdict(list)
    for i in range(n):
        grupos_finales[dsu.find(i)].append(i)
    return grupos_finales

# ======================================
# CLASIFICADOR DE TONO V4
# ======================================
class ClasificadorTonoV4:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases
        names = [marca] + [a for a in aliases if a]
        self.pattern = re.compile(r"\b(" + "|".join(map(re.escape, names)) + r")\b", re.IGNORECASE)

    def _extraer_oraciones(self, texto: str) -> List[str]:
        if not texto: return []
        sentences = re.split(r'(?<=[.!?])\s+', texto)
        relevant = []
        for i, s in enumerate(sentences):
            if self.pattern.search(s):
                start = max(0, i-1)
                end = min(len(sentences), i+2)
                relevant.append(" ".join(sentences[start:end]))
        return relevant if relevant else [texto[:600]]

    async def analizar_grupo(self, texto_rep: str) -> str:
        contextos = self._extraer_oraciones(texto_rep)
        txt_prompt = "\n".join(contextos[:3])
        prompt = f"""Analiza el tono hacia '{self.marca}'.
        REGLAS:
        - Positivo: Logros, crecimiento, ayuda en crisis.
        - Negativo: Sanciones, caídas, críticas.
        - Neutro: Informe dato, mención pasiva.
        
        Texto: {txt_prompt}
        Responde JSON: {{"tono": "Positivo|Negativo|Neutro"}}"""
        
        try:
            r = await acall_with_retries(openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                                         messages=[{"role": "user", "content": prompt}], max_tokens=30, temperature=0, response_format={"type":"json_object"})
            return json.loads(r.choices[0].message.content).get("tono", "Neutro").title()
        except: return "Neutro"

# ======================================
# CLASIFICADOR SEMÁNTICO V4
# ======================================
class ClasificadorSemanticoV4:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca_context = f"Marca: {marca}. Alias: {','.join(aliases)}."
        self.marca = marca
        self.aliases = aliases

    async def generar_subtemas_batch(self, textos: List[str]) -> List[str]:
        prompt_txt = "\n".join([f"N{i+1}: {t[:350]}" for i, t in enumerate(textos)])
        prompt = f"""{self.marca_context}
        Genera un SUBTEMA (2-5 palabras) específico para cada noticia.
        NO uses el nombre de la marca. Formato: "Lanzamiento de app", "Caída en bolsa".
        Input:
        {prompt_txt}
        Responde JSON: {{"items": ["Subtema 1", "Subtema 2"...]}}"""
        
        try:
            r = await acall_with_retries(openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                                         messages=[{"role": "user", "content": prompt}], max_tokens=300, temperature=0.2, response_format={"type":"json_object"})
            raw_items = json.loads(r.choices[0].message.content).get("items", [])
            
            clean_items = []
            for item in raw_items:
                c = limpiar_tema_geografico(item, self.marca, self.aliases)
                clean_items.append(formato_oracion(c))
            
            if len(clean_items) > len(textos): clean_items = clean_items[:len(textos)]
            while len(clean_items) < len(textos): clean_items.append("Tema General")
            return clean_items
        except: return ["Tema General"] * len(textos)

    async def consolidar_subtemas(self, subtemas_unicos: List[str]) -> Dict[str, str]:
        if len(subtemas_unicos) < 2: return {s: s for s in subtemas_unicos}
        embs = [get_embedding(s) for s in subtemas_unicos]
        valid_idxs = [i for i, e in enumerate(embs) if e is not None]
        
        if not valid_idxs: return {s: s for s in subtemas_unicos}
        
        matrix = np.array([embs[i] for i in valid_idxs])
        clust = AgglomerativeClustering(n_clusters=None, distance_threshold=1-CONSOLIDATION_SEMANTIC_THRESHOLD, metric="cosine", linkage="average").fit(matrix)
        
        groups = defaultdict(list)
        for i, lbl in enumerate(clust.labels_): groups[lbl].append(subtemas_unicos[valid_idxs[i]])
        
        mapa = {s: s for s in subtemas_unicos}
        async def get_name(g):
            if len(g) == 1: return g, g[0]
            p = f"Unifica en una frase corta (Tipo oración): {', '.join(g)}. JSON: {{'res': '...'}}"
            try:
                r = await acall_with_retries(openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                                             messages=[{"role": "user", "content": p}], max_tokens=20, temperature=0.1, response_format={"type":"json_object"})
                return g, formato_oracion(json.loads(r.choices[0].message.content).get("res", g[0]))
            except: return g, g[0]
            
        tasks = [get_name(g) for g in groups.values()]
        results = await asyncio.gather(*tasks)
        for grp, name in results:
            for s in grp: mapa[s] = name
        return mapa

    def generar_temas_macro(self, subtemas: List[str]) -> List[str]:
        unicos = list(set(subtemas))
        if len(unicos) <= NUM_TEMAS_PRINCIPALES: return subtemas
        embs = [get_embedding(s) for s in unicos]
        valid_idxs = [i for i, e in enumerate(embs) if e is not None]
        if len(valid_idxs) < 2: return subtemas
        
        matrix = np.array([embs[i] for i in valid_idxs])
        n_clust = min(NUM_TEMAS_PRINCIPALES, len(valid_idxs))
        clust = AgglomerativeClustering(n_clusters=n_clust, metric="cosine", linkage="average").fit(matrix)
        
        mapa_tema = {}
        groups = defaultdict(list)
        for i, lbl in enumerate(clust.labels_): groups[lbl].append(unicos[valid_idxs[i]])
        
        for grp in groups.values():
            nombre = sorted(grp, key=len)[0]
            nombre = formato_oracion(nombre)
            for s in grp: mapa_tema[s] = nombre
        return [mapa_tema.get(s, s) for s in subtemas]

# ======================================
# Lógica PKL, Duplicados y Excel
# ======================================
def predict_pkl(textos: List[str], pkl_file: Any) -> List[str]:
    try:
        pipe = joblib.load(pkl_file)
        res = pipe.predict(textos)
        return [str(r) for r in res]
    except: return ["N/A"] * len(textos)

def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict) -> List[Dict]:
    processed = deepcopy(rows)
    seen_url, seen_broad, buckets = {}, {}, defaultdict(list)
    
    k_type = key_map.get("tipodemedio")
    k_men = key_map.get("menciones")
    k_med = key_map.get("medio")
    k_tit = key_map.get("titulo")
    k_id = key_map.get("idnoticia")
    k_link = key_map.get("link_nota")
    k_hora = key_map.get("hora")

    for i, r in enumerate(processed):
        if r.get("is_duplicate"): continue
        
        t_med = normalizar_tipo_medio(str(r.get(k_type)))
        men = norm_key(r.get(k_men))
        med = norm_key(r.get(k_med))
        
        if t_med == "Internet":
            l_inf = r.get(k_link, {})
            url = l_inf.get("url") if isinstance(l_inf, dict) else None
            if url and men:
                key = (url, men)
                if key in seen_url:
                    r["is_duplicate"] = True; r["idduplicada"] = processed[seen_url[key]].get(k_id,"")
                    continue
                else: seen_url[key] = i
            if med and men: buckets[(med, men)].append(i)
            
        elif t_med in ["Radio", "Televisión"]:
            h = str(r.get(k_hora,"")).strip()
            if men and med and h:
                key = (men, med, h)
                if key in seen_broad:
                    r["is_duplicate"] = True; r["idduplicada"] = processed[seen_broad[key]].get(k_id,"")
                else: seen_broad[key] = i

    for idxs in buckets.values():
        if len(idxs) < 2: continue
        for i in range(len(idxs)):
            for j in range(i+1, len(idxs)):
                i1, i2 = idxs[i], idxs[j]
                if processed[i1].get("is_duplicate") or processed[i2].get("is_duplicate"): continue
                t1 = normalize_title_for_comparison(processed[i1].get(k_tit))
                t2 = normalize_title_for_comparison(processed[i2].get(k_tit))
                if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    processed[i2]["is_duplicate"] = True
                    processed[i2]["idduplicada"] = processed[i1].get(k_id, "")
                    
    return processed

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

def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    standard = {"titulo":"Titulo","resumen":"Resumen - Aclaracion","menciones":"Menciones - Empresa","medio":"Medio","idnoticia":"ID Noticia","tipodemedio":"Tipo de Medio","link_nota":"Link Nota","link_streaming":"Link (Streaming - Imagen)","region":"Region","hora":"Hora"}
    for k,v in standard.items(): key_map[k] = norm_key(v)
    
    rows = []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)})
        
    split_rows = []
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

    return detectar_duplicados_avanzado(split_rows, key_map), key_map

def generar_output_final(rows: List[Dict], key_map: Dict) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "Resultado"
    cols = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Tono IA","Tema","Subtema","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ws.append(cols)
    link_style = NamedStyle("LinkStyle", font=Font(color="0000FF", underline="single"))
    
    for r in rows:
        kt, kr = key_map.get("titulo"), key_map.get("resumen")
        if kt: r[kt] = clean_title_for_output(r.get(kt))
        if kr: r[kr] = corregir_texto(r.get(kr))
        
        out = []
        links = {}
        for idx, h in enumerate(cols, 1):
            nk = norm_key(h)
            k = key_map.get(nk, nk)
            val = r.get(k)
            if isinstance(val, dict) and "url" in val:
                out.append(val.get("value", "Link"))
                if val.get("url"): links[idx] = val["url"]
            else: out.append(val)
        ws.append(out)
        for c, url in links.items():
            cell = ws.cell(row=ws.max_row, column=c)
            cell.hyperlink = url
            cell.style = link_style
            
    b = io.BytesIO(); wb.save(b); return b.getvalue()

# ======================================
# Pipeline Principal (CORREGIDO)
# ======================================
async def pipeline_analisis(df_input: pd.DataFrame, kmap: Dict, brand: str, aliases: List[str], mode: str, pkl_tono=None, pkl_tema=None):
    col_tit = kmap["titulo"]
    col_res = kmap["resumen"]
    df_input["txt_full"] = df_input[col_tit].fillna("").astype(str) + ". " + df_input[col_res].fillna("").astype(str)
    
    # 1. Agrupación (Usando posiciones relativas al DataFrame filtrado)
    # Nota: agrupar_noticias_robustas devuelve indices basados en range(len(df_input))
    grupos = agrupar_noticias_robustas(df_input, col_tit, col_res)
    
    representantes = [] 
    mapa_grupo_indices = defaultdict(list)
    
    # Mapa para traducir posición relativa (0,1,2...) a índice real del DataFrame (label index)
    pos_to_label = {i: idx for i, idx in enumerate(df_input.index)}
    
    for gid, indices_posicionales in grupos.items():
        # Elegir texto más largo como representante
        # Convertir indices posicionales a indices iloc para extraer datos
        idx_rep_pos = max(indices_posicionales, key=lambda i: len(df_input.iloc[i]["txt_full"]))
        
        representantes.append(df_input.iloc[idx_rep_pos]["txt_full"])
        mapa_grupo_indices[len(representantes)-1] = indices_posicionales

    results = {"Tono": [], "Tema": [], "Subtema": []}
    
    # 2. Tono
    if "PKL" in mode and pkl_tono:
        results["Tono"] = predict_pkl(representantes, pkl_tono)
    else:
        cls_t = ClasificadorTonoV4(brand, aliases)
        p_bar = st.progress(0, "Analizando Tono...")
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        async def run_t(txt):
            async with sem: return await cls_t.analizar_grupo(txt)
        results["Tono"] = await asyncio.gather(*[run_t(t) for t in representantes])
        p_bar.empty()

    # 3. Subtema
    cls_s = ClasificadorSemanticoV4(brand, aliases)
    if "Solo Modelos PKL" in mode:
        results["Subtema"] = ["N/A"] * len(representantes)
    else:
        p_bar = st.progress(0, "Generando Subtemas...")
        subs_raw = []
        for i in range(0, len(representantes), 20):
            batch = representantes[i:i+20]
            res = await cls_s.generar_subtemas_batch(batch)
            subs_raw.extend(res)
            p_bar.progress((i+1)/len(representantes))
        p_bar.empty()
        
        with st.spinner("Consolidando semánticamente..."):
            unicos = list(set(subs_raw))
            mapa = await cls_s.consolidar_subtemas(unicos)
            results["Subtema"] = [mapa.get(s, s) for s in subs_raw]

    # 4. Tema
    if "PKL" in mode and pkl_tema:
        results["Tema"] = predict_pkl(representantes, pkl_tema)
    elif "Solo Modelos PKL" in mode:
        results["Tema"] = ["N/A"] * len(representantes)
    else:
        results["Tema"] = cls_s.generar_temas_macro(results["Subtema"])

    # 5. Asignar (CORRECCIÓN DE KEYERROR)
    df_input["Tono IA"] = ""
    df_input["Tema"] = ""
    df_input["Subtema"] = ""
    
    for i, indices_pos in mapa_grupo_indices.items():
        # Convertir posiciones (0,1,2) a etiquetas reales del índice (23, 45, 67)
        indices_reales = [pos_to_label[p] for p in indices_pos]
        
        df_input.loc[indices_reales, "Tono IA"] = results["Tono"][i]
        df_input.loc[indices_reales, "Tema"] = results["Tema"][i]
        df_input.loc[indices_reales, "Subtema"] = results["Subtema"][i]
        
    return df_input

# ======================================
# Main
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    st.markdown('<div class="main-header">🔐 Portal de Acceso Seguro</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("pwd"):
            p = st.text_input("Contraseña:", type="password")
            if st.form_submit_button("Ingresar"):
                if p == st.secrets.get("APP_PASSWORD", "admin"):
                    st.session_state["password_correct"] = True
                    st.rerun()
                else: st.error("Error")
    return False

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Análisis personalizable de Tono y Tema/Subtema</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])

    with tab1:
        if not st.session_state.get("proc_done", False):
            with st.form("form1"):
                st.markdown("### 📂 Archivos")
                c1,c2,c3 = st.columns(3)
                f_dos = c1.file_uploader("Dossier (.xlsx)", type="xlsx")
                f_reg = c2.file_uploader("Regiones (.xlsx)", type="xlsx")
                f_int = c3.file_uploader("Internet (.xlsx)", type="xlsx")
                
                st.markdown("### 🏢 Configuración")
                brand = st.text_input("Marca", placeholder="Ej: Bancolombia")
                alias = st.text_area("Alias (;)", placeholder="Ban;Juan Carlos")
                mode = st.radio("Modo", ["Híbrido (PKL + API)", "Solo Modelos PKL", "API OpenAI"], index=0)
                
                pkl_tono, pkl_tema = None, None
                if "PKL" in mode:
                    cc1,cc2 = st.columns(2)
                    pkl_tono = cc1.file_uploader("PKL Tono", type="pkl")
                    pkl_tema = cc2.file_uploader("PKL Tema", type="pkl")
                
                if st.form_submit_button("🚀 INICIAR ANÁLISIS"):
                    if not all([f_dos, f_reg, f_int, brand]):
                        st.error("Faltan datos")
                    else:
                        try:
                            if "API" in mode: openai.api_key = st.secrets["OPENAI_API_KEY"]
                        except: st.error("No API Key"); st.stop()
                        
                        aliases = [x.strip() for x in alias.split(";") if x.strip()]
                        t0 = time.time()
                        
                        with st.status("Procesando...", expanded=True) as status:
                            wb = load_workbook(f_dos, data_only=True)
                            rows_raw, kmap = run_dossier_logic(wb.active)
                            
                            df_reg = pd.read_excel(f_reg)
                            reg_map = dict(zip(df_reg.iloc[:,0].astype(str).str.lower().str.strip(), df_reg.iloc[:,1]))
                            df_int = pd.read_excel(f_int)
                            int_map = dict(zip(df_int.iloc[:,0].astype(str).str.lower().str.strip(), df_int.iloc[:,1]))
                            
                            for r in rows_raw:
                                med = str(r.get(kmap["medio"],"")).lower().strip()
                                r[kmap["region"]] = reg_map.get(med, "N/A")
                                if med in int_map:
                                    r[kmap["medio"]] = int_map[med]
                                    r[kmap["tipodemedio"]] = "Internet"
                                fix_links_by_media_type(r, kmap)
                                
                            df_all = pd.DataFrame(rows_raw)
                            df_unique = df_all[~df_all["is_duplicate"]].copy()
                            
                            if not df_unique.empty:
                                df_res = asyncio.run(pipeline_analisis(
                                    df_unique, kmap, brand, aliases, mode, pkl_tono, pkl_tema
                                ))
                                
                                res_map = df_res.set_index("original_index")[["Tono IA", "Tema", "Subtema"]].to_dict("index")
                                for r in rows_raw:
                                    idx = r["original_index"]
                                    if not r["is_duplicate"] and idx in res_map:
                                        r[kmap["tonoiai"]] = res_map[idx]["Tono IA"]
                                        r[kmap["tema"]] = res_map[idx]["Tema"]
                                        r[kmap["subtema"]] = res_map[idx]["Subtema"]
                                    elif r["is_duplicate"]:
                                        r[kmap["tonoiai"]] = "Duplicada"
                                        r[kmap["tema"]] = "Duplicada"
                                        r[kmap["subtema"]] = "Duplicada"
                            
                            out_bytes = generar_output_final(rows_raw, kmap)
                            
                            if not df_unique.empty:
                                final_counts = df_res["Tono IA"].value_counts()
                                st.session_state["metrics_res"] = (final_counts.get("Positivo",0), final_counts.get("Negativo",0))
                            else:
                                st.session_state["metrics_res"] = (0, 0)
                                
                            st.session_state.update({
                                "proc_done": True, "out_data": out_bytes,
                                "stats": (len(rows_raw), len(df_unique), time.time()-t0),
                                "fname": f"Informe_{brand}_{datetime.date.today()}.xlsx",
                                "brand": brand, "aliases": aliases
                            })
                            status.update(label="¡Listo!", state="complete")
                            st.rerun()
                            
        else:
            tot, unq, dur = st.session_state.stats
            pos, neg = st.session_state.get("metrics_res", (0,0))
            
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Total", tot)
            c2.metric("Únicas", unq)
            c3.metric("Positivos", pos)
            c4.metric("Negativos", neg)
            st.caption(f"Tiempo: {dur:.1f}s")
            
            st.download_button("📥 Descargar Informe", st.session_state.out_data, st.session_state.fname, 
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
            
            if st.button("🔄 Refinar Subtemas"):
                with st.spinner("Refinando..."):
                    df_curr = pd.read_excel(io.BytesIO(st.session_state.out_data))
                    cls_ref = ClasificadorSemanticoV4(st.session_state.brand, st.session_state.aliases)
                    mask = df_curr.iloc[:, 16] != "Duplicada" # Index 16 aprox Tono IA o usar nombre col
                    if "Subtema" in df_curr.columns:
                        subs = df_curr.loc[mask, "Subtema"].astype(str).tolist()
                        unicos = list(set(subs))
                        mapa = asyncio.run(cls_ref.consolidar_subtemas(unicos))
                        df_curr.loc[mask, "Subtema"] = df_curr.loc[mask, "Subtema"].map(lambda x: mapa.get(str(x), str(x)))
                        
                        new_rows = df_curr.to_dict("records")
                        fixed_rows = []
                        for nr in new_rows:
                            fixed_rows.append({norm_key(k): v for k,v in nr.items()})
                        
                        std_map = {norm_key(c): norm_key(c) for c in df_curr.columns}
                        st.session_state.out_data = generar_output_final(fixed_rows, std_map)
                        st.success("¡Refinado!")
                        time.sleep(1)
                        st.rerun()

            if st.button("Nuevo Análisis"):
                for k in list(st.session_state.keys()):
                    if k != "password_correct": del st.session_state[k]
                st.rerun()

    with tab2:
        st.subheader("Análisis Rápido")
        qf = st.file_uploader("Excel", type="xlsx", key="qf")
        if qf:
            qdf = pd.read_excel(qf)
            c1,c2 = st.columns(2)
            qt = c1.selectbox("Título", qdf.columns)
            qr = c2.selectbox("Resumen", qdf.columns)
            qb = st.text_input("Marca", key="qb")
            qa = st.text_input("Alias", key="qa")
            
            if st.button("Analizar"):
                als = [x.strip() for x in qa.split(";") if x.strip()]
                kmap_q = {"titulo": qt, "resumen": qr}
                try:
                     if "OPENAI_API_KEY" in st.secrets: openai.api_key = st.secrets["OPENAI_API_KEY"]
                except: pass
                res_df = asyncio.run(pipeline_analisis(qdf.copy(), kmap_q, qb, als, "API OpenAI"))
                
                out = io.BytesIO()
                res_df.to_excel(out, index=False)
                st.download_button("Descargar", out.getvalue(), "Quick_Analysis.xlsx")
                st.dataframe(res_df.head())

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.8rem;'>Desarrollado con 🤖 por Johnathan Cortés</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
