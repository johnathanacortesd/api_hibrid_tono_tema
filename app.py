# ======================================
# FENAVI - Análisis de Noticias con Scraping
# Pipeline: Scraping → Resumen IA → Tono → Subtema → Tema
# ======================================

import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, NamedStyle
import datetime
import io
import openai
import re
import time
import json
import zipfile
import xml.etree.ElementTree as ET
import html
import os
from collections import defaultdict, Counter
from unidecode import unidecode
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import gc

# ======================================
# Config
# ======================================
st.set_page_config(
    page_title="FENAVI - Analisis de Noticias IA",
    page_icon="🐔",
    layout="wide",
    initial_sidebar_state="collapsed"
)

MODEL_CLASIF = "gpt-4.1-nano-2025-04-14"
CACHE_SCRAPING = "/root/.hermes/scraping_cache.json"

# ======================================
# CSS
# ======================================
def load_css():
    st.markdown("""
<style>
:root {--bg:#f8f9fa;--s1:#fff;--border:#dadce0;--accent:#f97316;--accent2:#ea580c;
--accent-bg:#fff7ed;--accent-bdr:#fed7aa;--green:#059669;--green-bg:#ecfdf5;--green-bdr:#a7f3d0;
--red:#dc2626;--blue:#1a73e8;--r:8px;--r2:12px;--r3:16px;
--shadow:0 1px 3px rgba(60,64,67,.12),0 1px 2px rgba(60,64,67,.06);}
html,body,[data-testid="stApp"]{background:var(--bg)!important}
.block-container{padding-top:1rem!important;padding-bottom:1rem!important}
.app-header{background:var(--s1);border:1px solid var(--border);border-radius:var(--r3);
padding:1rem 1.5rem;margin-bottom:1rem;display:flex;align-items:center;gap:1rem;
box-shadow:var(--shadow);position:relative;overflow:hidden}
.app-header::after{content:'';position:absolute;top:0;left:0;right:0;height:3px;
background:linear-gradient(90deg,#f97316,#fb923c,#fdba74)}
.header-icon{width:40px;height:40px;background:linear-gradient(135deg,#f97316,#ea580c);
border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.2rem;
color:#fff;flex-shrink:0}
.header-title{font-size:1.25rem;font-weight:700}
.header-sub{font-size:.75rem;color:#5f6368}
.sec-label{font-size:.72rem;font-weight:700;color:#3c4043;letter-spacing:.08em;
text-transform:uppercase;padding:.3rem 0 .2rem;border-bottom:2px solid #e8eaed;margin:1rem 0 .5rem}
.stButton>button{border-radius:100px!important;font-weight:500!important}
.stButton>button[kind="primary"]{background:#f97316!important;border:none!important;color:#fff!important}
[data-testid="stFileUploader"]{background:var(--s1)!important;border:1.5px dashed var(--border)!important;
border-radius:var(--r2)!important}
[data-testid="stForm"]{background:var(--s1)!important;border:1px solid var(--border)!important;
border-radius:var(--r3)!important;padding:1.2rem!important;box-shadow:var(--shadow)!important}
.metric-card{background:var(--s1);border:1px solid var(--border);border-radius:var(--r2);
padding:.8rem;text-align:center;box-shadow:var(--shadow)}
.metric-val{font-size:1.5rem;font-weight:700;line-height:1}
.metric-lbl{font-size:.62rem;color:#5f6368;text-transform:uppercase;letter-spacing:.08em}
.success-banner{background:linear-gradient(135deg,#ecfdf5,#d1fae5);border:1px solid var(--green-bdr);
border-left:4px solid var(--green);border-radius:var(--r2);padding:.8rem 1.2rem;margin:.5rem 0;
display:flex;align-items:center;gap:.8rem}
@media(max-width:768px){.metrics-grid{grid-template-columns:repeat(2,1fr)}}
</style>
""", unsafe_allow_html=True)

# ======================================
# Scraping con Playwright
# ======================================
def load_scraping_cache():
    """Load scraping cache from disk."""
    if os.path.exists(CACHE_SCRAPING):
        try:
            with open(CACHE_SCRAPING) as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_scraping_cache(cache):
    """Save scraping cache to disk."""
    try:
        os.makedirs(os.path.dirname(CACHE_SCRAPING), exist_ok=True)
        with open(CACHE_SCRAPING, 'w') as f:
            json.dump(cache, f, ensure_ascii=False)
    except:
        pass

def extract_urls_from_xlsx(xlsx_bytes):
    """Extract GlobalNews URLs mapped to row numbers from XLSX."""
    zf = zipfile.ZipFile(io.BytesIO(xlsx_bytes))
    
    # Read relationships
    try:
        rels_xml = zf.read("xl/worksheets/_rels/sheet1.xml.rels").decode("utf-8")
        rels_root = ET.fromstring(rels_xml)
        rid_to_url = {}
        for rel in rels_root:
            rid = rel.get("Id")
            target = rel.get("Target", "")
            if "Validar.aspx" in target:
                rid_to_url[rid] = html.unescape(target)
    except:
        return {}
    
    # Read hyperlinks section
    try:
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        root = ET.fromstring(sheet_xml)
        ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        rel_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
        
        hlinks = root.find(".//s:hyperlinks", ns)
        ref_to_url = {}
        if hlinks is not None:
            for hl in hlinks:
                ref = hl.get("ref")  # e.g. "W2"
                rid = hl.get(rel_ns + "id")
                if rid and rid in rid_to_url:
                    ref_to_url[ref] = rid_to_url[rid]
        
        return ref_to_url
    except:
        return {}

def url_to_direct(url):
    """Convert Validar.aspx URL to direct news2 URL."""
    n_m = re.search(r'[?&]n=(\d+)', url)
    u_m = re.search(r'[?&]u=([a-f0-9-]+)', url, re.IGNORECASE)
    c_m = re.search(r'[?&]c=(\d+)', url)
    if not n_m or not u_m:
        return None
    n_id = n_m.group(1)
    u_id = u_m.group(1)
    c_id = c_m.group(1) if c_m else "1"
    return "http://news2.globalnews.com.co/?accessNewsCode={}|{}|{}&mode=image".format(u_id, n_id, c_id)

def scrape_all_news(urls_data, cache, progress_bar=None, status_text=None):
    """Scrape all news using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        st.error("Playwright no instalado. Ejecuta: pip install playwright && playwright install chromium")
        return {}
    
    results = {}
    total = len(urls_data)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()
        
        for i, (row_num, url, news_id) in enumerate(urls_data):
            if status_text:
                status_text.text("Scraping noticia {}/{}...".format(i + 1, total))
            if progress_bar:
                progress_bar.progress((i + 1) / total)
            
            # Check cache
            cache_key = news_id
            if cache_key in cache:
                results[row_num] = cache[cache_key]
                continue
            
            # Convert URL
            direct_url = url_to_direct(url)
            if not direct_url:
                results[row_num] = None
                continue
            
            # Try to scrape
            text = None
            for attempt in range(2):
                try:
                    mode = "image" if attempt == 0 else "text"
                    test_url = direct_url.replace("mode=image", "mode=" + mode)
                    page.goto(test_url, wait_until="domcontentloaded", timeout=15000)
                    page.wait_for_timeout(3000)
                    
                    # Get text content
                    body_text = page.evaluate("() => document.body.innerText")
                    if body_text and len(body_text) > 200:
                        # Try to extract just the news content
                        idx1 = body_text.find('Imagen Resumen')
                        if idx1 >= 0:
                            # Find end of news content
                            end_markers = ['Nube de palabras', 'ANFENAVI', 'Comentarios']
                            end_idx = len(body_text)
                            for marker in end_markers:
                                m_idx = body_text.find(marker, idx1 + 100)
                                if m_idx > 0 and m_idx < end_idx:
                                    end_idx = m_idx
                            text = body_text[idx1 + 15:end_idx].strip()
                        else:
                            text = body_text[:5000]
                    
                    if text and len(text) > 200:
                        break
                except Exception as e:
                    text = None
            
            if text:
                results[row_num] = text
                cache[cache_key] = text
            else:
                results[row_num] = None
        
        browser.close()
    
    save_scraping_cache(cache)
    return results

# ======================================
# DSU (Disjoint Set Union) for clustering
# ======================================
class DSU:
    def __init__(self, n):
        self.p = list(range(n))
        self.rank = [0] * n
    
    def find(self, i):
        path = []
        while self.p[i] != i:
            path.append(i)
            i = self.p[i]
        for node in path:
            self.p[node] = i
        return i
    
    def union(self, i, j):
        ri, rj = self.find(i), self.find(j)
        if ri == rj:
            return
        if self.rank[ri] < self.rank[rj]:
            ri, rj = rj, ri
        self.p[rj] = ri
        if self.rank[ri] == self.rank[rj]:
            self.rank[ri] += 1
    
    def groups(self, n):
        c = defaultdict(list)
        for i in range(n):
            c[self.find(i)].append(i)
        return dict(c)

# ======================================
# OpenAI Helpers
# ======================================
def get_client():
    if "openai_client" not in st.session_state:
        api_key = st.secrets.get("OPENAI_API_KEY", "")
        if not api_key:
            st.error("OPENAI_API_KEY no configurado en secrets.toml")
            st.stop()
        st.session_state.openai_client = openai.OpenAI(api_key=api_key)
    return st.session_state.openai_client

def generate_resumen_tono(texto, titulo, medio, fecha, cliente, voceros=""):
    """Generate client-focused summary + tone for one news article."""
    client = get_client()
    
    system_prompt = """Eres un analista de medios experto que trabaja para {} (Federacion Nacional de Avicultores de Colombia).
Analizas noticias de prensa para identificar su relevancia para el sector avicola colombiano.

Debes analizar cada noticia y responder SOLO con un JSON valido con esta estructura EXACTA:
{{
  "resumen_cliente": "Resumen de 2-3 parrafos enfocado en el impacto de esta noticia para {cliente} y el sector avicola. Menciona datos clave, cifras, voceros y entidades citadas. Si la noticia no menciona avicultura/huevos/pollos, analiza el impacto POTENCIAL.",
  "tono": "Positivo" o "Negativo" o "Neutro"
}}

CRITERIOS TONO:
- Positivo: favorece al sector avicola (crecimiento consumo, buena imagen, innovacion benefica, precios estables)
- Negativo: perjudica al sector (crisis, sobreproduccion, caida precios, mala imagen, problemas sanitarios)
- Neutro: informativo general, tendencia del mercado sin impacto directo claro""".format(cliente=cliente)

    user_prompt = """NOTICIA:
Titular: {}
Medio: {}
Fecha: {}
Voceros relevantes: {}

TEXTO COMPLETO:
{}

Responde SOLO con JSON valido.""".format(
        titulo or "N/A",
        medio or "N/A",
        fecha or "N/A",
        voceros or "No especificados",
        texto[:4500]
    )
    
    resp = client.chat.completions.create(
        model=MODEL_CLASIF,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        max_tokens=1000,
        response_format={"type": "json_object"}
    )
    
    return json.loads(resp.choices[0].message.content)

def generate_subtemas_llm(grupos_data):
    """Generate subtema labels for groups of news."""
    client = get_client()
    results = {}
    
    for gid, grupo in grupos_data.items():
        titulos = "\n".join("- " + (g.get("titulo") or "") for g in grupo[:8])
        resumenes = "\n".join("- " + (g.get("resumen") or "")[:200] for g in grupo[:5])
        
        prompt = """Eres editor jefe de un periodico. Genera UN subtema periodistico (4-6 palabras) 
como FRASE NOMINAL para agrupar estas noticias similares.

TITULOS:
{}

RESUMENES:
{}

REGLAS ESTRICTAS:
- Frase nominal con preposicion (de/del/para/sobre/en/por)
- SIN marcas privadas, SIN nombres propios, SIN ciudades
- SIN verbos conjugados
- Especifico al tema real de las noticias
- Tildes y ñ correctas
- Empieza con sustantivo, no con cargo o persona
JSON: {{"subtema":"..."}}""".format(titulos, resumenes)
        
        try:
            resp = client.chat.completions.create(
                model=MODEL_CLASIF,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=100,
                response_format={"type": "json_object"}
            )
            sub = json.loads(resp.choices[0].message.content).get("subtema", "Sin clasificar")
            results[gid] = capitalizar_etiqueta(sub)
        except Exception as e:
            # Fallback: use most common words from titles
            all_words = []
            for g in grupo:
                for w in re.findall(r'[a-z]+', unidecode(str(g.get("titulo", "")).lower())):
                    if len(w) > 4 and w not in {'este', 'esta', 'estos', 'estas', 'para', 'sobre', 'entre'}:
                        all_words.append(w)
            if len(all_words) >= 2:
                top = [w for w, _ in Counter(all_words).most_common(3)]
                results[gid] = capitalizar_etiqueta("{} de {}".format(top[0].title(), top[1].title()))
            else:
                results[gid] = "Sin clasificar"
    
    return results

def generate_temas_llm(subtemas_unicos):
    """Generate tema categories grouping subtemas."""
    client = get_client()
    
    subs_str = "\n".join("- " + s for s in subtemas_unicos)
    
    prompt = """Eres editor jefe de un periodico. Agrupa estos subtemas en CATEGORIAS GENERALES (temas).

SUBTEMAS:
{}

Responde SOLO con JSON:
{{
  "agrupaciones": [
    {{"tema": "Categoria general 2-3 palabras", "subtemas_incluidos": ["subtema1", "subtema2"]}},
    ...
  ]
}}

REGLAS:
- Cada tema es GENERAL y ABSTRACTO (como seccion de periodico)
- 2-3 palabras maximo por tema
- Cada subtema debe estar en exactamente un tema
- Ejemplos validos: "Consumo y tendencias", "Mercado y precios", "Produccion avicola", "Innovacion", "Politicas publicas", "Imagen corporativa", "Internacional", "Sostenibilidad"
- Tildes y ñ correctas""".format(subs_str)
    
    try:
        resp = client.chat.completions.create(
            model=MODEL_CLASIF,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1000,
            response_format={"type": "json_object"}
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get("agrupaciones", [])
    except Exception as e:
        st.warning("Error generando temas: {}".format(str(e)[:200]))
        return []

def capitalizar_etiqueta(texto):
    """Capitalize label properly."""
    if not texto or not texto.strip():
        return "Sin clasificar"
    texto = texto.strip()
    # Remove trailing punctuation
    texto = texto.rstrip('.')
    return texto[0].upper() + texto[1:] if texto else "Sin clasificar"

# ======================================
# Embeddings
# ======================================
def obtener_embeddings(textos, batch_size=50):
    """Get embeddings from OpenAI API."""
    client = get_client()
    resultados = []
    
    for i in range(0, len(textos), batch_size):
        batch = textos[i:i + batch_size]
        batch = [(t or "")[:8000] for t in batch]
        try:
            resp = client.embeddings.create(
                input=batch,
                model="text-embedding-3-small"
            )
            for d in resp.data:
                resultados.append(d.embedding)
        except Exception as e:
            st.warning("Error embeddings: {}".format(str(e)[:100]))
            for _ in batch:
                resultados.append(None)
    
    return resultados

# ======================================
# Pipeline Principal
# ======================================
def run_pipeline(df_source, urls_map, scrape_cache, cliente, voceros, progress_container):
    """
    Complete pipeline:
    1. Scraping -> texto real
    2. LLM -> resumen cliente + tono (por noticia)
    3. Clustering embeddings -> subtemas
    4. LLM -> temas desde subtemas
    """
    
    step = 0
    total_steps = 4
    
    # ---- STEP 0: Prepare data ----
    # Map refs to rows
    ref_to_row = {}
    for ref, url in urls_map.items():
        col_match = re.match(r'([A-Z]+)(\d+)', ref)
        if col_match and col_match.group(1) == "W":  # Column W = Link Streaming
            row_num = int(col_match.group(2))
            ref_to_row[row_num] = url
    
    # Build news items list
    n_rows = len(df_source)
    news_items = []
    
    for idx in range(n_rows):
        xlsx_row = idx + 2  # xlsx is 1-indexed + header row
        news_id = str(df_source.iloc[idx].get("ID Noticia", ""))
        titulo = str(df_source.iloc[idx].get("Título", ""))
        medio = str(df_source.iloc[idx].get("Medio", ""))
        fecha = str(df_source.iloc[idx].get("Fecha", ""))
        autor = str(df_source.iloc[idx].get("Autor - Conductor", ""))
        resumen_orig = str(df_source.iloc[idx].get("Resumen - Aclaracion", ""))
        
        # Get URL if exists
        url = ref_to_row.get(xlsx_row)
        
        news_items.append({
            "idx": idx,
            "xlsx_row": xlsx_row,
            "id": news_id,
            "titulo": titulo,
            "medio": medio,
            "fecha": fecha,
            "autor": autor,
            "resumen_orig": resumen_orig if resumen_orig and resumen_orig != "nan" else "",
            "url": url,
            "texto_scrapeado": None,
            "resumen_cliente": None,
            "tono": None,
            "subtema": None,
            "tema": None,
            "has_scraped": False,
        })
    
    # ---- STEP 1: Scraping ----
    step = 1
    progress_container.progress((step) / total_steps, "Paso 1: Scraping de noticias...")
    
    # Build scraping list: (xlsx_row, url, news_id)
    to_scrape = []
    for item in news_items:
        if item["url"]:
            to_scrape.append((item["xlsx_row"], item["url"], item["id"]))
    
    scraped = {}
    if to_scrape:
        progress_bar = progress_container.empty()
        status_text = progress_container.empty()
        
        scraped = scrape_all_news(to_scrape, scrape_cache, progress_bar, status_text)
        
        # Assign scraped text
        n_scraped = 0
        for item in news_items:
            if item["xlsx_row"] in scraped and scraped[item["xlsx_row"]]:
                item["texto_scrapeado"] = scraped[item["xlsx_row"]]
                item["has_scraped"] = True
                n_scraped += 1
        
        st.success("Scraping completado: {} de {} noticias con texto scrapeado".format(
            n_scraped, len(to_scrape)
        ))
    else:
        st.warning("No se encontraron URLs para scrapear")
    
    gc.collect()
    
    # ---- STEP 2: LLM -> resumen + tono ----
    step = 2
    news_scraped = [n for n in news_items if n["has_scraped"]]
    progress_container.progress(step / total_steps, "Paso 2: Generando resumenes y tonos (LLM)...")
    
    llm_cache_path = "/root/.hermes/llm_resumen_cache.json"
    llm_cache = {}
    if os.path.exists(llm_cache_path):
        try:
            with open(llm_cache_path) as f:
                llm_cache = json.load(f)
        except:
            pass
    
    for i, item in enumerate(news_scraped):
        progress_container.progress(
            step / total_steps + (0.3 / total_steps) * (i / max(len(news_scraped), 1)),
            "Analizando {}/{} con IA...".format(i + 1, len(news_scraped))
        )
        
        cache_key = "rt_" + item["id"]
        if cache_key in llm_cache:
            rt = llm_cache[cache_key]
        else:
            rt = generate_resumen_tono(
                item["texto_scrapeado"], item["titulo"], item["medio"],
                item["fecha"], cliente, voceros
            )
            llm_cache[cache_key] = rt
        
        item["resumen_cliente"] = rt.get("resumen_cliente", "")
        item["tono"] = rt.get("tono", "Neutro")
    
    # For non-scraped items, use original
    for item in news_items:
        if not item["has_scraped"]:
            item["resumen_cliente"] = item["resumen_orig"] or "No se pudo scrapear la noticia"
            item["tono"] = "Neutro"
    
    # Save LLM cache
    try:
        os.makedirs(os.path.dirname(llm_cache_path), exist_ok=True)
        with open(llm_cache_path, 'w') as f:
            json.dump(llm_cache, f, ensure_ascii=False)
    except:
        pass
    
    st.success("Resumenes y tonos generados para {} noticias".format(len(news_items)))
    gc.collect()
    
    # ---- STEP 3: Clustering -> subtemas ----
    step = 3
    progress_container.progress(step / total_steps, "Paso 3: Clasificando subtemas...")
    
    # Build embedding texts
    textos_cluster = []
    for item in news_items:
        texto = "{}. {}".format(item["titulo"] or "", item["resumen_cliente"][:500] if item["resumen_cliente"] else "")
        textos_cluster.append(texto)
    
    # Dedup by title first
    titulos_norm = [unidecode((item["titulo"] or "").strip().lower()) for item in news_items]
    dsu = DSU(len(news_items))
    
    # Group identical titles
    title_groups = defaultdict(list)
    for i, t in enumerate(titulos_norm):
        if t:
            title_groups[t].append(i)
    for idxs in title_groups.values():
        for j in idxs[1:]:
            dsu.union(idxs[0], j)
    
    # Semantic clustering
    embs = obtener_embeddings(textos_cluster)
    valid_idx = [(i, e) for i, e in enumerate(embs) if e is not None]
    
    if len(valid_idx) >= 2:
        idxs_list, M = zip(*valid_idx)
        M = np.array(M)
        
        # Adaptive threshold
        n = len(valid_idx)
        umbral = 0.92 if n <= 5 else 0.85 if n <= 10 else 0.82 if n <= 20 else 0.78
        
        sim_matrix = cosine_similarity(M)
        labels = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=1 - umbral,
            metric="precomputed",
            linkage="complete" if n <= 10 else "average"
        ).fit(1 - sim_matrix).labels_
        
        cluster_groups = defaultdict(list)
        for k, lbl in enumerate(labels):
            cluster_groups[lbl].append(idxs_list[k])
        
        # Apply clustering to DSU
        for lbl, members in cluster_groups.items():
            for j in members[1:]:
                dsu.union(members[0], j)
    
    # Generate subtema labels per group
    grupos = dsu.groups(len(news_items))
    grupos_data = {}
    
    for gid, members in grupos.items():
        grupo_noticias = [news_items[m] for m in members]
        resumenes_grupo = []
        for gn in grupo_noticias:
            texto = gn["resumen_cliente"] or gn["titulo"] or ""
            resumenes_grupo.append({
                "titulo": gn["titulo"],
                "resumen": texto[:300]
            })
        grupos_data[gid] = resumenes_grupo
    
    subtemas_por_grupo = generate_subtemas_llm(grupos_data)
    
    # Assign subtemas
    for gid, members in grupos.items():
        sub = subtemas_por_grupo.get(gid, "Sin clasificar")
        for m in members:
            news_items[m]["subtema"] = sub
    
    n_subtemas = len(set(item["subtema"] for item in news_items if item["subtema"]))
    st.success("Subtemas generados: {} grupos".format(n_subtemas))
    gc.collect()
    
    # ---- STEP 4: Temas desde subtemas ----
    step = 4
    progress_container.progress(step / total_steps, "Paso 4: Generando temas...")
    
    subtemas_unicos = sorted(set(item["subtema"] for item in news_items if item["subtema"]))
    
    agrupaciones = generate_temas_llm(subtemas_unicos)
    
    # Build subtema -> tema map
    subtema_to_tema = {}
    temas_asignados = set()
    
    for agr in agrupaciones:
        tema = agr.get("tema", "")
        if not tema:
            continue
        tema = capitalizar_etiqueta(tema)
        temas_asignados.add(tema)
        for sub_incluido in agr.get("subtemas_incluidos", []):
            subtema_to_tema[sub_incluido] = tema
    
    # Handle unassigned subtemas
    for sub in subtemas_unicos:
        if sub not in subtema_to_tema:
            subtema_to_tema[sub] = sub  # Use subtema as tema
    
    # Assign temas
    for item in news_items:
        sub = item.get("subtema", "Sin clasificar")
        item["tema"] = subtema_to_tema.get(sub, sub)
    
    n_temas = len(set(item["tema"] for item in news_items if item["tema"]))
    st.success("Temas generados: {} categorias".format(n_temas))
    
    return news_items

# ======================================
# Excel Output
# ======================================
def generate_output_excel(news_items, df_source):
    """Generate output XLSX with the structure."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Hoja1"
    
    # Output columns
    ORDER = [
        "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Region",
        "Seccion - Programa", "Titulo", "Autor - Conductor", "Nro. Pagina",
        "Dimension", "Duracion - Nro. Caracteres", "CPE", "Audiencia", "Tier",
        "Tono", "Tono IA", "Tema", "Subtema", "Link Nota",
        "Resumen - Aclaracion", "Resumen para el cliente", "Link (Streaming - Imagen)",
        "Menciones - Empresa", "ID duplicada"
    ]
    
    ws.append(ORDER)
    
    # Style hyperlinks
    hl_style = NamedStyle(name="HL", font=Font(color="0000FF", underline="single"))
    if "HL" not in wb.style_names:
        wb.add_named_style(hl_style)
    
    NUM_COLS = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    
    # Source columns
    src_headers = {}
    for col_idx in range(df_source.shape[1]):
        src_headers[norm_key(df_source.columns[col_idx])] = col_idx
    
    for item in news_items:
        row_data = []
        links = {}
        
        src_idx = item["idx"]
        
        for ci, header in enumerate(ORDER, 1):
            nk = norm_key(header)
            
            # AI-generated fields
            if nk == "tono" or nk == "tonoia":
                val = item.get("tono", "Neutro")
            elif nk == "tema":
                val = item.get("tema", "")
            elif nk == "subtema":
                val = item.get("subtema", "")
            elif "resumen para el cliente" in header.lower():
                val = item.get("resumen_cliente", "")
            else:
                # Get from source
                val = None
                for src_nk, src_col in src_headers.items():
                    if src_nk == nk:
                        val = df_source.iloc[src_idx, src_col]
                        break
            
            cv = None
            if header in NUM_COLS:
                try:
                    cv = float(val) if val is not None and str(val).strip() != "" else None
                except:
                    cv = str(val) if val is not None else None
            elif isinstance(val, dict) and val.get("url"):
                cv = val.get("value", "Link")
                links[ci] = val["url"]
            elif val is not None:
                cv = str(val)
            
            row_data.append(cv)
        
        ws.append(row_data)
        for ci, url in links.items():
            cell = ws.cell(row=ws.max_row, column=ci)
            cell.hyperlink = url
            cell.style = "HL"
    
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()

def norm_key(text):
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

# ======================================
# Streamlit UI
# ======================================
def main():
    load_css()
    
    st.markdown("""
    <div class="app-header">
        <div class="header-icon">🐔</div>
        <div style="flex:1">
            <div class="header-title">FENAVI - Analisis de Noticias IA</div>
            <div class="header-sub">Scraping → Resumen cliente → Tono → Subtema → Tema</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Check OpenAI key
    api_key = st.secrets.get("OPENAI_API_KEY", "")
    if not api_key:
        st.error("OPENAI_API_KEY no configurado en .streamlit/secrets.toml")
        st.stop()
    
    # Config form
    with st.form("config_form"):
        st.markdown('<div class="sec-label">Configuracion</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            cliente = st.text_input("Nombre del cliente", value="FENAVI")
            voceros = st.text_input("Voceros (separados por ;)", placeholder="Gonzalo Moreno;Ana Maria Lopez")
        with c2:
            modelo = st.selectbox("Modelo LLM", ["gpt-4o-mini", "gpt-4o"], index=0)
            modelo_select = modelo  # Will be used to set MODEL_CLASIF
        
        st.markdown('<div class="sec-label">Archivo de entrada</div>', unsafe_allow_html=True)
        uploaded = st.file_uploader("Sube el Excel del dossier", type=["xlsx"])
        
        submitted = st.form_submit_button("▶ Iniciar analisis", use_container_width=True, type="primary")
    
    if submitted and uploaded:
        if not cliente.strip():
            st.error("Indica el nombre del cliente.")
            st.stop()
        
        # Update model
        global MODEL_CLASIF
        MODEL_CLASIF = modelo_select
        
        # Load data
        with st.status("Cargando archivo...", expanded=True) as s:
            try:
                file_bytes = uploaded.read()
                df_source = pd.read_excel(io.BytesIO(file_bytes))
                urls_map = extract_urls_from_xlsx(file_bytes)
                
                s.update(
                    label="Archivo cargado: {} columnas, {} filas, {} URLs encontradas".format(
                        len(df_source.columns), len(df_source), len(urls_map)
                    ),
                    state="complete"
                )
            except Exception as e:
                st.error("Error cargando archivo: {}".format(e))
                st.stop()
        
        # Load scraping cache
        scrape_cache = load_scraping_cache()
        
        # Run pipeline
        progress_container = st.empty()
        
        with st.status("Procesando noticias...", expanded=True) as status:
            voceros_str = voceros if voceros else ""
            
            try:
                news_items = run_pipeline(
                    df_source, urls_map, scrape_cache,
                    cliente.strip(), voceros_str,
                    progress_container
                )
                status.update(label="Analisis completado", state="complete")
            except Exception as e:
                st.error("Error en el pipeline: {}".format(str(e)))
                import traceback
                st.code(traceback.format_exc())
                st.stop()
        
        # Results
        n_total = len(news_items)
        n_scraped = sum(1 for n in news_items if n.get("has_scraped"))
        tonos = Counter(n.get("tono", "") for n in news_items)
        subtemas = Counter(n.get("subtema", "") for n in news_items if n.get("subtema"))
        temas = Counter(n.get("tema", "") for n in news_items if n.get("tema"))
        
        st.success("Analisis completado para {} noticias".format(n_total))
        
        # Metrics
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.markdown('<div class="metric-card"><div class="metric-val">{}</div><div class="metric-lbl">Total</div></div>'.format(n_total), unsafe_allow_html=True)
        c2.markdown('<div class="metric-card"><div class="metric-val" style="color:#059669">{}</div><div class="metric-lbl">Scrapeadas</div></div>'.format(n_scraped), unsafe_allow_html=True)
        c3.markdown('<div class="metric-card"><div class="metric-val" style="color:#1a73e8">{}</div><div class="metric-lbl">Subtemas</div></div>'.format(len(subtemas)), unsafe_allow_html=True)
        c4.markdown('<div class="metric-card"><div class="metric-val" style="color:#f97316">{}</div><div class="metric-lbl">Temas</div></div>'.format(len(temas)), unsafe_allow_html=True)
        tone_display = "P:{} N:{} Ne:{}".format(
            tonos.get("Positivo", 0), tonos.get("Negativo", 0), tonos.get("Neutro", 0)
        )
        c5.markdown('<div class="metric-card"><div class="metric-val" style="font-size:1rem">{}</div><div class="metric-lbl">Tonos</div></div>'.format(tone_display), unsafe_allow_html=True)
        
        # Preview table
        st.markdown('<div class="sec-label">Vista previa</div>', unsafe_allow_html=True)
        preview = []
        for item in news_items[:20]:
            preview.append({
                "ID": item["id"],
                "Medio": (item["medio"] or "")[:30],
                "Titulo": (item["titulo"] or "")[:60],
                "Tono": item.get("tono", ""),
                "Subtema": (item.get("subtema", "") or "")[:50],
                "Tema": (item.get("tema", "") or "")[:40],
                "Scrapeado": "Si" if item.get("has_scraped") else "No",
            })
        st.dataframe(pd.DataFrame(preview), use_container_width=True, height=400)
        
        # Show subtemas
        st.markdown('<div class="sec-label">Subtemas detectados</div>', unsafe_allow_html=True)
        for sub, count in subtemas.most_common():
            st.markdown("- **{}** ({} noticias)".format(sub, count))
        
        st.markdown('<div class="sec-label">Temas detectados</div>', unsafe_allow_html=True)
        for tema, count in temas.most_common():
            st.markdown("- **{}** ({} noticias)".format(tema, count))
        
        # Download
        output_data = generate_output_excel(news_items, df_source)
        filename = "Informe_IA_{}_{}.xlsx".format(
            cliente.replace(" ", "_"),
            datetime.datetime.now().strftime("%Y%m%d_%H%M")
        )
        
        st.download_button(
            "Descargar XLSX",
            data=output_data,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

if __name__ == "__main__":
    main()
