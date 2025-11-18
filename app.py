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

# CONFIGURACIÓN DE MODELOS (Revertido a petición)
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

CONCURRENT_REQUESTS = 50
SIMILARITY_THRESHOLD_TONO = 0.91
SIMILARITY_THRESHOLD_SUBTEMAS_AGRUPACION = 0.86 
CONSOLIDATION_SEMANTIC_THRESHOLD = 0.82 
SIMILARITY_THRESHOLD_TITULOS = 0.95 
MAX_TOKENS_PROMPT_TXT = 4000

# Listas de limpieza
STOPWORDS_ES = set(""" a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada """.split())

# ======================================
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #1f77b4; --secondary-color: #2ca02c; --card-bg: #ffffff; --shadow-light: 0 4px 6px rgba(0,0,0,0.1); --border-radius: 12px; }
        .main-header { background: linear-gradient(135deg, var(--primary-color) 0%, #0d47a1 100%); color: white; padding: 2rem; border-radius: var(--border-radius); text-align: center; font-size: 2.5rem; font-weight: 800; margin-bottom: 1.5rem; box-shadow: var(--shadow-light); }
        .subtitle { text-align: center; color: #666; font-size: 1.1rem; margin: -1rem 0 2rem 0; }
        .metric-card { background: var(--card-bg); padding: 1.2rem; border-radius: var(--border-radius); box-shadow: var(--shadow-light); text-align: center; border: 1px solid #e0e0e0; }
        .metric-value { font-size: 2rem; font-weight: bold; color: var(--primary-color); }
        .metric-label { font-size: 0.9rem; color: #666; text-transform: uppercase; }
        .success-card { background: #f0fdf4; padding: 1.5rem; border-radius: var(--border-radius); border: 1px solid #22c55e; margin: 1rem 0; box-shadow: var(--shadow-light); }
        .stButton > button { border-radius: 8px; font-weight: 600; height: 3rem; }
        div[data-testid="stFileUploader"] { border: 1px dashed #ccc; border-radius: 10px; padding: 10px; }
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
                if password == st.secrets.get("APP_PASSWORD", "admin"): 
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
    return False

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

def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

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

@st.cache_data(ttl=3600)
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto or len(texto.split()) < 2: return None
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        return resp["data"][0]["embedding"]
    except Exception: return None

def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    if not textos: return {}
    embs = [get_embedding(t) for t in textos]
    valid_indices = [i for i, e in enumerate(embs) if e is not None]
    if len(valid_indices) < 2: return {0: valid_indices} if valid_indices else {}
    
    emb_matrix = np.array([embs[i] for i in valid_indices])
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
# CLASIFICADOR DE TONO (Surgical Focus)
# ======================================
class ClasificadorTonoUltraV4:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        names = [marca] + [a for a in (aliases or []) if a]
        patterns = [re.escape(unidecode(n.strip().lower())) for n in names if n.strip()]
        self.brand_regex_str = r"\b(" + "|".join(patterns) + r")\b" if patterns else r"(a^b)"
        self.brand_pattern = re.compile(self.brand_regex_str, re.IGNORECASE)

    def _extract_brand_sentences(self, texto: str) -> List[str]:
        if not texto: return []
        sentences = re.split(r'(?<=[.!?])\s+', texto)
        indices_marca = [i for i, s in enumerate(sentences) if self.brand_pattern.search(unidecode(s.lower()))]
        
        contextos = []
        processed = set()
        for idx in indices_marca:
            start, end = max(0, idx - 1), min(len(sentences), idx + 2)
            rango = tuple(range(start, end))
            if any(i in processed for i in rango): continue
            contextos.append(" ".join(sentences[start:end]))
            processed.update(rango)
            
        if not contextos and texto: return [texto[:600]]
        return contextos

    async def _llm_analisis_preciso(self, contextos: List[str]) -> Dict[str, str]:
        aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
        txt_analisis = "\n\n--- FRAGMENTO ---\n".join(contextos[:3])
        
        prompt = f"""Analiza el tono hacia la marca '{self.marca}' (alias: {aliases_str}).
REGLAS:
1. **Sujeto vs Objeto:** ¿La marca *logra* algo (Positivo) o es *criticada/sancionada* (Negativo)?
2. **Contexto:** Si el mercado cae pero la marca crece -> Positivo.
3. **Neutro:** Datos financieros planos, menciones corporativas estándar, o si la marca no es el foco.
4. **Crisis:** Si la marca responde/ayuda -> Positivo. Si causó el daño -> Negativo.

Fragmentos:
{txt_analisis}

Responde JSON: {{"tono": "Positivo|Negativo|Neutro"}}"""

        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}], max_tokens=40, temperature=0.0,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            return {"tono": data.get("tono", "Neutro").title()}
        except: return {"tono": "Neutro"}

    async def procesar_lote_async(self, textos_concat: pd.Series, progress_bar):
        textos = textos_concat.tolist(); n = len(textos)
        grupos = agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TONO)
        mapa_grupos = {textos[max(idxs, key=lambda i: len(textos[i]))]: idxs for idxs in grupos.values()}
        
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        async def worker(txt):
            async with semaphore:
                return await self._llm_analisis_preciso(self._extract_brand_sentences(txt))
        
        tasks = [worker(txt) for txt in mapa_grupos.keys()]
        resultados_brutos = []
        total = len(tasks)
        
        for i, f in enumerate(asyncio.as_completed(tasks)):
            resultados_brutos.append(await f)
            progress_bar.progress(0.1 + 0.4 * (i/max(total, 1)), f"🎯 Analizando Tono: {i}/{total}")
            
        resultados_finales = [None] * n
        for i, (rep, idxs) in enumerate(mapa_grupos.items()):
            for idx in idxs: resultados_finales[idx] = resultados_brutos[i]
        return resultados_finales

# ======================================
# CLASIFICADOR DE SUBTEMA CONSOLIDADO
# ======================================
class ClasificadorSubtemaV4:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca, self.aliases = marca, aliases
        self.brand_context = f"Marca: {marca}. Alias: {', '.join(aliases)}."

    async def _generar_subtema_batch(self, textos: List[str]) -> List[str]:
        """Genera subtemas y asegura que la salida tenga la misma longitud que la entrada."""
        batch_txt = "\n".join([f"Noticia {i+1}: {t[:350]}" for i, t in enumerate(textos)])
        prompt = f"""{self.brand_context}
Genera un SUBTEMA (2-5 palabras) ESPECÍFICO para cada noticia.
REGLAS: NO uses el nombre de la marca. Sé concreto.
Input:
{batch_txt}
Responde JSON: {{"items": ["Subtema 1", "Subtema 2"...]}}"""
        try:
            r = await acall_with_retries(openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                                         messages=[{"role": "user", "content": prompt}], max_tokens=300, temperature=0.2,
                                         response_format={"type": "json_object"})
            items = json.loads(r.choices[0].message.content).get("items", [])
            
            # Limpieza básica
            clean_items = []
            for it in items:
                c = it.replace(self.marca, "").strip().capitalize()
                for a in self.aliases: c = c.replace(a, "").strip()
                clean_items.append(c if c else "Actividad General")
            
            # SAFETY CHECK: Rellenar o recortar para coincidir con input
            if len(clean_items) < len(textos):
                clean_items.extend(["Tema General"] * (len(textos) - len(clean_items)))
            elif len(clean_items) > len(textos):
                clean_items = clean_items[:len(textos)]
                
            return clean_items
        except: return ["Tema General"] * len(textos)

    async def _consolidar_semantica(self, subtemas: List[str]) -> Dict[str, str]:
        if len(subtemas) < 2: return {s: s for s in subtemas}
        embs = [get_embedding(s) for s in subtemas]
        valid = [i for i, e in enumerate(embs) if e is not None]
        if not valid: return {s: s for s in subtemas}
        
        matrix = np.array([embs[i] for i in valid])
        clust = AgglomerativeClustering(n_clusters=None, distance_threshold=1-CONSOLIDATION_SEMANTIC_THRESHOLD, metric="cosine", linkage="average").fit(matrix)
        
        groups = defaultdict(list)
        for i, lbl in enumerate(clust.labels_): groups[lbl].append(subtemas[valid[i]])
        
        mapa = {s: s for s in subtemas}
        async def nombrar_cluster(g):
            if len(g) == 1: return g, g[0]
            p = f"Unifica estos temas en UNO solo breve: {', '.join(g)}. JSON: {{'nombre': '...'}}"
            try:
                r = await acall_with_retries(openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                                             messages=[{"role": "user", "content": p}], max_tokens=20, temperature=0.1, response_format={"type":"json_object"})
                return g, json.loads(r.choices[0].message.content).get("nombre", g[0])
            except: return g, g[0]
            
        tasks = [nombrar_cluster(g) for g in groups.values()]
        results = await asyncio.gather(*tasks)
        for grp, nombre in results:
            for s in grp: mapa[s] = nombre
        return mapa

    def _generar_temas_macro(self, subtemas: List[str]) -> List[str]:
        unicos = list(set(subtemas))
        if not unicos: return subtemas
        embs = [get_embedding(s) for s in unicos]
        valid = [i for i, e in enumerate(embs) if e is not None]
        if len(valid) < 2: return subtemas
        
        matrix = np.array([embs[i] for i in valid])
        clust = AgglomerativeClustering(n_clusters=None, distance_threshold=0.35, metric="cosine", linkage="average").fit(matrix)
        
        mapa_tema = {}
        temp_groups = defaultdict(list)
        for i, lbl in enumerate(clust.labels_): temp_groups[lbl].append(unicos[valid[i]])
        
        for grp in temp_groups.values():
            nombre_tema = sorted(grp, key=len)[0].title()
            if len(nombre_tema.split()) > 4: nombre_tema = "Actualidad Corporativa"
            for s in grp: mapa_tema[s] = nombre_tema
            
        return [mapa_tema.get(s, s) for s in subtemas]

    async def procesar_completo(self, textos: List[str], p_bar):
        # 1. Agrupar textos
        grupos = agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_SUBTEMAS_AGRUPACION)
        reps = [textos[idxs[0]] for idxs in grupos.values()]
        mapa_idxs = list(grupos.values())
        
        # 2. Generar Subtemas
        subtemas_brutos = []
        batch_size = 10
        for i in range(0, len(reps), batch_size):
            batch = reps[i:i+batch_size]
            res = await self._generar_subtema_batch(batch)
            # Proteccion adicional: asegurar extension exacta
            subtemas_brutos.extend(res)
            p_bar.progress(0.5 + 0.3 * (i/max(len(reps), 1)), f"🏷️ Generando etiquetas: {i}/{len(reps)}")
            
        # CRASH FIX: Truncar o rellenar si hubo desajuste
        if len(subtemas_brutos) > len(mapa_idxs):
            subtemas_brutos = subtemas_brutos[:len(mapa_idxs)]
        while len(subtemas_brutos) < len(mapa_idxs):
            subtemas_brutos.append("Tema General")
            
        # 3. Asignar a todos
        todos_subtemas = [""] * len(textos)
        for i, sub in enumerate(subtemas_brutos):
            for idx in mapa_idxs[i]: todos_subtemas[idx] = sub
            
        # 4. Consolidar
        p_bar.progress(0.9, "🧠 Consolidando semánticamente...")
        unicos = list(set(subtemas_brutos))
        mapa_cons = await self._consolidar_semantica(unicos)
        subtemas_finales = [mapa_cons.get(s, s) for s in todos_subtemas]
        
        # 5. Derivar Tema
        temas_finales = self._generar_temas_macro(subtemas_finales)
        
        return subtemas_finales, temas_finales

# ======================================
# Lógica Legacy PKL
# ======================================
def analizar_tono_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> List[Dict[str, str]]:
    try:
        pipeline = joblib.load(pkl_file)
        pred = pipeline.predict(textos)
        MAP = {1: "Positivo", "1": "Positivo", 0: "Neutro", "0": "Neutro", -1: "Negativo", "-1": "Negativo"}
        return [{"tono": MAP.get(p, str(p))} for p in pred]
    except Exception as e:
        st.error(f"Error PKL Tono: {e}"); return [{"tono": "N/A"}] * len(textos)

def analizar_temas_con_pkl(textos: List[str], pkl_file: io.BytesIO) -> List[str]:
    try:
        pipeline = joblib.load(pkl_file)
        return [str(p) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error PKL Tema: {e}"); return ["N/A"] * len(textos)

# ======================================
# Procesamiento de Archivos y Excel
# ======================================
def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    standard = {"titulo":"Titulo","resumen":"Resumen - Aclaracion","menciones":"Menciones - Empresa","medio":"Medio","idnoticia":"ID Noticia","tipodemedio":"Tipo de Medio","link_nota":"Link Nota","link_streaming":"Link (Streaming - Imagen)","region":"Region"}
    for k, v in standard.items(): key_map[k] = norm_key(v)
    
    rows = []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        r_d = {norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)}
        rows.append(r_d)

    processed, seen = [], {}
    for idx, r_cells in enumerate(rows):
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        if key_map["tipodemedio"] in base:
            base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        
        menciones = str(base.get(key_map["menciones"], "")).split(";")
        for m in menciones:
            if not m.strip(): continue
            new = deepcopy(base)
            new[key_map["menciones"]] = m.strip()
            new["original_index"] = idx
            new["is_duplicate"] = False
            
            k_dup = (str(new.get(key_map["idnoticia"])), new[key_map["menciones"]])
            if k_dup in seen:
                new["is_duplicate"] = True
                new["idduplicada"] = seen[k_dup]
            else:
                seen[k_dup] = str(new.get(key_map["idnoticia"]))
            processed.append(new)
            
    return processed, key_map

def fix_links_and_region(rows, key_map, region_file, internet_file):
    df_reg = pd.read_excel(region_file)
    reg_map = dict(zip(df_reg.iloc[:,0].astype(str).str.lower().str.strip(), df_reg.iloc[:,1]))
    df_int = pd.read_excel(internet_file)
    int_map = dict(zip(df_int.iloc[:,0].astype(str).str.lower().str.strip(), df_int.iloc[:,1]))
    
    for r in rows:
        med = str(r.get(key_map["medio"], "")).lower().strip()
        r[key_map["region"]] = reg_map.get(med, "N/A")
        if med in int_map:
            r[key_map["medio"]] = int_map[med]
            r[key_map["tipodemedio"]] = "Internet"
            
        tm = r.get(key_map["tipodemedio"])
        ln, ls = key_map["link_nota"], key_map["link_streaming"]
        link_n, link_s = r.get(ln), r.get(ls)
        
        if tm == "Internet" and isinstance(link_s, dict) and link_s.get("url"):
            r[ln] = link_s
            r[ls] = {"value": "", "url": None}
        elif tm in ["Radio", "Televisión"]:
            r[ls] = {"value": "", "url": None}

def generate_output_excel(rows, key_map):
    wb = Workbook(); ws = wb.active; ws.title = "Resultado"
    headers = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Tono IA","Tema","Subtema","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ws.append(headers)
    
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    
    for r in rows:
        out_row = []
        links = {}
        for idx, h in enumerate(headers, 1):
            nk = norm_key(h)
            k = key_map.get(nk, nk)
            val = r.get(k)
            if isinstance(val, dict) and "url" in val:
                out_row.append(val.get("value", "Link"))
                if val.get("url"): links[idx] = val["url"]
            else:
                out_row.append(val)
        ws.append(out_row)
        for col, url in links.items():
            c = ws.cell(row=ws.max_row, column=col)
            c.hyperlink = url
            c.style = link_style
            
    out = io.BytesIO(); wb.save(out); return out.getvalue()

# ======================================
# Pipeline Principal Async
# ======================================
async def run_full_process_async(dossier, region, internet, brand, aliases, mode, tono_pkl, tema_pkl):
    start = time.time()
    if "API" in mode or "Híbrido" in mode:
        try: openai.api_key = st.secrets["OPENAI_API_KEY"]
        except: st.error("❌ Configurar OPENAI_API_KEY en Secrets."); st.stop()

    # 1. Carga
    with st.status("📋 Paso 1/5: Limpieza y Duplicados", expanded=True) as s:
        wb = load_workbook(dossier, data_only=True)
        rows, kmap = run_dossier_logic(wb.active)
        s.update(label="✅ Paso 1/5: Completado", state="complete")
        
    # 2. Mapeos
    with st.status("🗺️ Paso 2/5: Mapeos y Normalización", expanded=True) as s:
        fix_links_and_region(rows, kmap, region, internet)
        s.update(label="✅ Paso 2/5: Completado", state="complete")

    # Preparar datos (SIN DUPLICADOS para análisis)
    rows_to_analyze = [r for r in rows if not r.get("is_duplicate")]
    
    if rows_to_analyze:
        df_temp = pd.DataFrame(rows_to_analyze)
        df_temp["txt_full"] = df_temp[kmap["titulo"]].fillna("").astype(str) + ". " + df_temp[kmap["resumen"]].fillna("").astype(str)
        
        # 3. Tono
        with st.status("🎯 Paso 3/5: Análisis de Tono", expanded=True) as s:
            p_bar = st.progress(0)
            if "PKL" in mode and tono_pkl:
                res = analizar_tono_con_pkl(df_temp["txt_full"].tolist(), tono_pkl)
            else:
                cls_tono = ClasificadorTonoUltraV4(brand, aliases)
                res = await cls_tono.procesar_lote_async(df_temp["txt_full"], p_bar)
            
            df_temp["Tono IA"] = [r["tono"] for r in res]
            
            # Mostrar métricas de lo analizado (Únicas)
            pos = sum(1 for r in res if r["tono"]=="Positivo")
            neg = sum(1 for r in res if r["tono"]=="Negativo")
            st.markdown(f"**Resultados (Noticias Únicas):** 🟢 {pos} | 🔴 {neg}")
            s.update(label="✅ Paso 3/5: Completado", state="complete")

        # 4. Tema/Subtema
        with st.status("🏷️ Paso 4/5: Temas y Subtemas", expanded=True) as s:
            p_bar = st.progress(0)
            if "Solo Modelos PKL" in mode and tema_pkl:
                temas = analizar_temas_con_pkl(df_temp["txt_full"].tolist(), tema_pkl)
                subtemas = ["N/A (PKL)"] * len(temas)
            else:
                cls_sub = ClasificadorSubtemaV4(brand, aliases)
                subtemas, temas = await cls_sub.procesar_completo(df_temp["txt_full"].tolist(), p_bar)
                
            df_temp["Tema"] = temas
            df_temp["Subtema"] = subtemas
            st.write(f"Subtemas únicos generados: {len(set(subtemas))}")
            s.update(label="✅ Paso 4/5: Completado", state="complete")
            
        # Merge Results
        res_map = df_temp.set_index("original_index").to_dict("index")
        for r in rows:
            if not r.get("is_duplicate") and r["original_index"] in res_map:
                match = res_map[r["original_index"]]
                r[kmap.get("tonoiai", "tonoia")] = match["Tono IA"]
                r[kmap.get("tema", "tema")] = match["Tema"]
                r[kmap.get("subtema", "subtema")] = match["Subtema"]
            elif r.get("is_duplicate"):
                r[kmap.get("tonoiai", "tonoia")] = "Duplicada"
                r[kmap.get("tema", "tema")] = "Duplicada"
                r[kmap.get("subtema", "subtema")] = "Duplicada"

    # 5. Generar
    with st.status("📊 Paso 5/5: Generando Informe", expanded=True) as s:
        out_data = generate_output_excel(rows, kmap)
        st.session_state.update({
            "output_data": out_data, "proc_complete": True,
            "fn": f"Informe_IA_{brand.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx",
            "metrics": (len(rows), len(rows_to_analyze), time.time()-start)
        })
        s.update(label="✅ Completado", state="complete")

# ======================================
# Funciones Análisis Rápido
# ======================================
async def run_quick_analysis(df, t_col, s_col, brand, aliases):
    df["txt"] = df[t_col].fillna("").astype(str) + ". " + df[s_col].fillna("").astype(str)
    
    with st.status("Analizando...", expanded=True):
        st.write("Analizando Tono...")
        ct = ClasificadorTonoUltraV4(brand, aliases)
        tr = await ct.procesar_lote_async(df["txt"], st.progress(0))
        df["Tono IA"] = [r["tono"] for r in tr]
        
        st.write("Analizando Temas...")
        cs = ClasificadorSubtemaV4(brand, aliases)
        sub, tem = await cs.procesar_completo(df["txt"].tolist(), st.progress(0))
        df["Subtema"] = sub
        df["Tema"] = tem
        
    return df.drop(columns=["txt"])

# ======================================
# Main UI
# ======================================
def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">📰 Sistema de Análisis de Noticias con IA</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Análisis personalizable de Tono y Tema/Subtema</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido (IA)"])

    with tab1:
        if not st.session_state.get("proc_complete", False):
            with st.form("input_form"):
                st.markdown("### 📂 Archivos de Entrada Obligatorios")
                c1, c2, c3 = st.columns(3)
                dossier = c1.file_uploader("**1. Dossier** (.xlsx)", type=["xlsx"])
                region = c2.file_uploader("**2. Región** (.xlsx)", type=["xlsx"])
                internet = c3.file_uploader("**3. Internet** (.xlsx)", type=["xlsx"])
                
                st.markdown("### 🏢 Configuración")
                brand = st.text_input("Marca Principal", placeholder="Ej: Bancolombia")
                aliases_txt = st.text_area("Alias (separados por ;)", placeholder="Ej: Ban;Juan Carlos Mora")
                
                st.markdown("### ⚙️ Modo de Análisis")
                mode = st.radio("Selecciona modo:", 
                                ["Híbrido (PKL + API) (Recomendado)", "Solo Modelos PKL", "API de OpenAI"], index=0)
                
                pkl_tono, pkl_tema = None, None
                if "PKL" in mode:
                    st.info("📂 Carga de Modelos PKL requerida para este modo")
                    cp1, cp2 = st.columns(2)
                    pkl_tono = cp1.file_uploader("Modelo Tono (.pkl)", type=["pkl"])
                    pkl_tema = cp2.file_uploader("Modelo Tema (.pkl)", type=["pkl"])

                if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                    if not all([dossier, region, internet, brand]):
                        st.error("❌ Faltan archivos o marca.")
                    else:
                        aliases = [a.strip() for a in aliases_txt.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(dossier, region, internet, brand, aliases, mode, pkl_tono, pkl_tema))
                        st.rerun()
        else:
            tot, unq, dur = st.session_state.metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Noticias", tot)
            c2.metric("Analizadas (Únicas)", unq)
            c3.metric("Duración", f"{dur:.1f}s")
            
            st.download_button("📥 **DESCARGAR INFORME**", st.session_state.output_data, st.session_state.fn, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)
            if st.button("🔄 Nuevo Análisis"):
                st.session_state.clear()
                st.rerun()

    with tab2:
        st.header("Análisis Rápido")
        qf = st.file_uploader("Sube Excel", type=["xlsx"], key="qf")
        if qf:
            df = pd.read_excel(qf)
            c1, c2 = st.columns(2)
            tc = c1.selectbox("Columna Título", df.columns)
            sc = c2.selectbox("Columna Resumen", df.columns)
            
            qb = st.text_input("Marca", key="qb")
            qa = st.text_input("Alias (;)", key="qa")
            
            if st.button("Analizar con IA"):
                if not qb: st.error("Falta Marca")
                else:
                    als = [x.strip() for x in qa.split(";") if x.strip()]
                    res = asyncio.run(run_quick_analysis(df.copy(), tc, sc, qb, als))
                    
                    out = io.BytesIO()
                    res.to_excel(out, index=False)
                    st.download_button("Descargar", out.getvalue(), "Analisis_Rapido.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    st.dataframe(res.head())

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.8rem;'>Desarrollado con 🤖 por Johnathan Cortés</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
