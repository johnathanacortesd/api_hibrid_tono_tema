"""
Análisis de Noticias · Tono + Tema + Subtema
=============================================
Pipeline mejorado:
  1. Embeddings coseno -> clustering para agrupar noticias reales
  2. Un label por grupo (no por articulo) -> consistencia total
  3. Agrupacion jerarquica: subtemas -> temas coherentes
  4. Tono relativo al protagonista (no generico)
  5. Deduplicacion estricta de near-duplicates

Modelo: gpt-4.1-nano-2025-04-14 (default) o gpt-5-nano-2025-08-07
Embedding: text-embedding-3-small
"""

import streamlit as st
import pandas as pd
from collections import defaultdict
from difflib import SequenceMatcher
from copy import deepcopy
import datetime
import io
import openai
import re
import time
import json
import os
from unidecode import unidecode
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import gc

# ==============================================================================
# Configuracion
# ==============================================================================
st.set_page_config(
    page_title="Analisis de Noticias . IA",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

EMBEDDING_MODEL    = "text-embedding-3-small"
CLASSIFIER_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4.1-nano-2025-04-14")
CONCURRENT_BATCH   = 8
MAX_RETRIES        = 3

UMBRAL_DEDUPLICACION          = 0.94
UMBRAL_SUBTEMA_CORPUS_GRANDE  = 0.80
UMBRAL_TEMA_CORPUS_GRANDE     = 0.68
UMBRAL_SUBTEMA_CORPUS_PEQUENO = 0.88
UMBRAL_TEMA_CORPUS_PEQUENO    = 0.80
DISTANCIA_MINIMA_GRUPOS       = 0.15

STOPWORDS_ES = frozenset(
    "a ante bajo cabe con contra de desde durante en entre hacia hasta "
    "mediante para por segun sin so sobre tras y o u e la el los las un "
    "una unos unas lo al del se su sus le les mi mis tu tus que cual "
    "cuales quien quienes cuyo cuya cuyos cuyas como cuando donde es son "
    "fue fueron era eran sera seran seria serian he ha han hay hubo "
    "estoy esta estan estaba estamos este esta estos estas ese esa esos "
    "esas aquel aquella aquellos aquellas cada todo toda todos todas "
    "si sobre muy tan tambien ya mas pero porque aunque mientras "
    "hasta donde".split()
)

# Patrones de tono (heuristica para fallback rapido)
POS_PATTERNS = re.compile(
    r"\b(lanza|inaugura|estrena|nuevo|nueva|alianza|acuerdo|crece|ganancia|"
    r"inversion|exito|lidera|premio|reconoce|destaca|supera|mejora|expansion|"
    r"aliados|avanza|fortalece|beneficia|impulsa|innova|oportunidad|"
    r"celebra|triunfa|logro|record|eficiencia|compromiso|bienestar|solucion)\b",
    re.IGNORECASE
)
NEG_PATTERNS = re.compile(
    r"\b(crisis|denuncia|sancion|multa|cae|pierde|fraude|escandalo|falla|"
    r"suspende|cierra|renuncia|huelga|ataque|hackeo|boicot|reclamo|perdida|"
    r"deficit|conflicto|disputa|rechaza|problema|riesgo|tragedia|emergencia|"
    r"demanda|investiga|critica|desastre|inundacion|deslizamiento|"
    r"damnificados|deterioro|irregularidad|corrupcion|evasion)\b",
    re.IGNORECASE
)
CRISIS_KEYWORDS = re.compile(
    r"\b(crisis|emergencia|desastre|deslizamiento|inundacion|afectaciones|"
    r"damnificados|tragedia|alerta maxima|zozobra)\b", re.IGNORECASE
)

# ==============================================================================
# Utilidades de texto
# ==============================================================================

def limpiar_texto(texto):
    if not texto:
        return ""
    texto = texto.lower().strip()
    texto = unidecode(texto)
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
    tokens_list = [t for t in texto.split() if len(t) > 2 and t not in STOPWORDS_ES]
    return ' '.join(tokens_list)


_token_cache = {}

def tokens(texto):
    if texto not in _token_cache:
        _token_cache[texto] = limpiar_texto(texto).split()
    return _token_cache[texto]


def extraer_protagonista(texto):
    """Heuristica: entidad principal del titulo."""
    t = tokens(texto)
    excluye = frozenset(
        'nuevo nueva nuevo anuncia lanza presenta llega abre inicia logro '
        'confirma destaca revela anuncia apertura inicio llega gobierno '
        'alcaldia ministerio empresa ciudad'.split()
    )
    candidatos = [w for w in t if w not in excluye and len(w) > 2][:3]
    return ' '.join(candidatos).title() if candidatos else ''


# ==============================================================================
# Similitud
# ==============================================================================

def similitud_por_texto(toks1, toks2):
    if not toks1 or not toks2:
        return 0.0
    s1, s2 = set(toks1), set(toks2)
    return len(s1 & s2) / len(s1 | s2)


def es_duplicado(titulo1, titulo2, resumen1, resumen2):
    sim_titulo = SequenceMatcher(None, titulo1.lower(), titulo2.lower()).ratio()
    sim_resumen = SequenceMatcher(None, resumen1.lower(), resumen2.lower()).ratio()
    sim_tokens = similitud_por_texto(tokens(titulo1), tokens(titulo2))
    if sim_titulo >= 0.85 and sim_tokens >= 0.75:
        return True
    if sim_titulo >= 0.78 and sim_resumen >= 0.60 and sim_tokens >= 0.65:
        return True
    return False


# ==============================================================================
# Tono: entidad-relativo
# ==============================================================================

def clasificar_tono(titulo, resumen, texto_completo=""):
    """
    Clasifica el tono RELATIVO AL PROTAGONISTA de la noticia.
    - Identifica el protagonista principal
    - Determina si la noticia le es POSITIVA, NEGATIVA, NEUTRAL o CRISIS
    - Detecta si la accion es PROACTIVA o REACTIVA
    - Evalia el sentimiento
    """
    protagonista = extraer_protagonista(titulo)
    texto_busq = (titulo + " " + resumen + " " + texto_completo).lower()

    crisis = bool(CRISIS_KEYWORDS.search(texto_busq))
    pos_score = len(POS_PATTERNS.findall(texto_busq))
    neg_score = len(NEG_PATTERNS.findall(texto_busq))

    if crisis:
        return {
            "tono": "CRISIS",
            "protagonista": protagonista or "No identificado",
            "accion": "REACTIVA",
            "sentimiento": "Desfavorable"
        }

    proactivas = re.findall(
        r"\b(lanza|inaugura|estrena|anuncia|crea|construye|abre|inicia|"
        r"implementa|desarrolla|firma|aliados|invierte|expande)\b",
        texto_busq, re.IGNORECASE
    )
    reactivas = re.findall(
        r"\b(responde|atiende|enfrenta|reacciona|declara|aclarar|"
        r"defiende|justifica|mitiga|soluciona|revoca|sanciona)\b",
        texto_busq, re.IGNORECASE
    )

    if not proactivas and not reactivas:
        accion_str = "NEUTRA"
    elif len(proactivas) >= len(reactivas):
        accion_str = "PROACTIVA"
    else:
        accion_str = "REACTIVA"

    if pos_score > 0 and neg_score == 0:
        tono = "POSITIVO"
        sentido = "Favorable"
    elif neg_score > 0 and pos_score == 0:
        tono = "NEGATIVO"
        sentido = "Desfavorable"
    elif pos_score > 0 and neg_score > 0:
        tono = "MIXTO"
        sentido = "Mixto"
    else:
        tono = "NEUTRAL"
        sentido = "Neutral"

    return {
        "tono": tono,
        "protagonista": protagonista or "No identificado",
        "accion": accion_str,
        "sentimiento": sentido
    }


# ==============================================================================
# Embeddings
# ==============================================================================

@st.cache_data(show_spinner=False)
def obtener_embeddings(textos):
    if not textos:
        return []
    api_key = os.getenv("OPENAI_API_KEY") or st.session_state.get("openai_api_key")
    if not api_key:
        return []

    client = openai.OpenAI(api_key=api_key)
    embeddings = []
    batch_size = 200

    for i in range(0, len(textos), batch_size):
        lote = textos[i:i + batch_size]
        for intento in range(MAX_RETRIES):
            try:
                resp = client.embeddings.create(model=EMBEDDING_MODEL, input=lote)
                embeddings.extend([d.embedding for d in resp.data])
                break
            except Exception:
                if intento < MAX_RETRIES - 1:
                    time.sleep(1.5 ** intento)
                else:
                    embeddings.extend([[0.0] * 1536 for _ in lote])

    return embeddings


# ==============================================================================
# Clustering -> Subtemas
# ==============================================================================

def _umbral_para(n):
    if n <= 5:
        return UMBRAL_SUBTEMA_CORPUS_PEQUENO
    elif n <= 15:
        return (UMBRAL_SUBTEMA_CORPUS_PEQUENO + UMBRAL_SUBTEMA_CORPUS_GRANDE) / 2
    return UMBRAL_SUBTEMA_CORPUS_GRANDE


def agrupar_subtemas(embeddings, titulos, resumenes):
    """
    Clustering de subtemas.
    Retorna: {cluster_id: {indices, label, tema, representante}}
    """
    n = len(embeddings)
    if n == 0:
        return {}
    if n == 1:
        return {0: {
            'indices': [0], 'label': '', 'tema': '',
            'representante': titulos[0]
        }}

    matriz = np.array(embeddings)
    sim_matrix = cosine_similarity(matriz)

    # Paso 0: Deduplicacion estricta
    duplicados = {}
    canonicals = set()
    for i in range(n):
        for j in range(i + 1, n):
            if j in duplicados:
                continue
            if es_duplicado(
                titulos[i], titulos[j],
                resumenes[i] if i < len(resumenes) else "",
                resumenes[j] if j < len(resumenes) else ""
            ):
                duplicados[j] = i
                canonicals.add(i)
        if i not in duplicados:
            canonicals.add(i)

    # Paso 1: Clustering Agglomerative
    umbral = _umbral_para(n)
    distancia = max(0.01, 1.0 - umbral)

    try:
        clustering = AgglomerativeClustering(
            n_clusters=None,
            metric='precomputed',
            linkage='average',
            distance_threshold=distancia
        )
        dist_matrix = 1.0 - sim_matrix
        dist_matrix = np.clip(dist_matrix, 0.0, 2.0)
        labels = clustering.fit_predict(dist_matrix)
    except Exception:
        labels = np.arange(n)

    # Fusionar duplicados al grupo del canonical
    for j, i in duplicados.items():
        labels[j] = labels[i]

    # Paso 2: Construir grupos
    grupos = defaultdict(lambda: {
        'indices': [], 'label': '', 'tema': '', 'representante': ''
    })
    for idx, label in enumerate(labels):
        grupos[int(label)]['indices'].append(idx)

    # Paso 3: Elegir representante y label por grupo
    for gid, g in grupos.items():
        idxs = g['indices']
        if len(idxs) > 1:
            sub_sim = sim_matrix[np.ix_(idxs, idxs)]
            centroide_sim = sub_sim.mean(axis=1)
            rep_idx = idxs[int(np.argmax(centroide_sim))]
        else:
            rep_idx = idxs[0]

        label_raw = titulos[rep_idx]
        label_raw = re.sub(
            r'^(nuevo|nueva|anuncia|lanza|presenta|inaugura|llega|abre|inicia|'
            r'logra|alcanza|supera|confirma|destaca|lanza|apertura)\s+',
            '', label_raw, flags=re.IGNORECASE
        ).strip()[:120]

        g['representante'] = label_raw or titulos[rep_idx]
        g['label'] = label_raw or titulos[rep_idx][:100]

    return dict(grupos)


# ==============================================================================
# Agrupacion jerarquica: Subtemas -> Temas
# ==============================================================================

def agrupar_temas(grupos, embeddings, titulos):
    if not grupos:
        return grupos

    n_grupos = len(grupos)
    if n_grupos <= 1:
        for gid in grupos:
            grupos[gid]['tema'] = grupos[gid]['label']
        return grupos

    gids = sorted(grupos.keys())
    rep_indices = [grupos[g]['indices'][0] for g in gids]
    reps_embeddings = np.array([embeddings[i] for i in rep_indices])

    sim_grupos = cosine_similarity(reps_embeddings)

    n_total = len(embeddings)
    if n_total <= 10:
        umbral_tema = UMBRAL_TEMA_CORPUS_PEQUENO
    else:
        umbral_tema = UMBRAL_TEMA_CORPUS_GRANDE

    dist_tema = max(0.01, 1.0 - umbral_tema)

    try:
        clustering = AgglomerativeClustering(
            n_clusters=None,
            metric='precomputed',
            linkage='average',
            distance_threshold=dist_tema
        )
        dist_matrix = np.clip(1.0 - sim_grupos, 0.0, 2.0)
        tema_labels = clustering.fit_predict(dist_matrix)
    except Exception:
        tema_labels = list(range(n_grupos))

    tema_map = defaultdict(list)
    for i, tl in enumerate(tema_labels):
        tema_map[tl].append(gids[i])

    tema_nombres = {}
    for tid, subtemas_ids in tema_map.items():
        mayor = max(subtemas_ids, key=lambda g: len(grupos[g]['indices']))
        nombre_tema = _generalizar_tema(grupos[mayor]['label'])
        tema_nombres[tid] = nombre_tema

    tema_idx_map = {}
    for i, tl in enumerate(tema_labels):
        tema_idx_map[gids[i]] = tema_nombres[tl]

    for gid in grupos:
        grupos[gid]['tema'] = tema_idx_map.get(gid, grupos[gid]['label'])

    return grupos


def _generalizar_tema(label_subtema):
    """Convierte un label de subtema en tema general."""
    texto = label_subtema.lower()

    temas_map = {
        'Transporte y Movilidad': [
            'transporte', 'movilidad', 'transito', 'vias', 'camionero',
            'metro', 'bus', 'transmilenio', 'carretera', 'autopista',
            'aeropuerto', 'vuelo', 'parcial vial'
        ],
        'Salud': [
            'salud', 'hospital', 'clinica', 'medico', 'pandemia',
            'vacuna', 'eps', 'cuidado', 'enfermedad', 'dengue'
        ],
        'Seguridad y Orden': [
            'seguridad', 'policia', 'criminal', 'homicidio',
            'robo', 'hurto', 'violencia', 'delito', 'sicariato'
        ],
        'Infraestructura': [
            'infraestructura', 'construccion', 'obra',
            'edificio', 'puente', 'hospital', 'escuela',
            'acueducto', 'alcantarillado'
        ],
        'Economa y Finanzas': [
            'economia', 'financiero', 'inversion', 'empleo',
            'comercio', 'empresa', 'negocio', 'industria',
            'inflacion', 'peso', 'dolar', 'presupuesto'
        ],
        'Educacion': [
            'educacion', 'colegio', 'universidad', 'estudiante',
            'docente', 'profesor', 'clase', 'matricula', 'icetex'
        ],
        'Medio Ambiente': [
            'ambiente', 'ecologia', 'deforestacion',
            'contaminacion', 'rio', 'paramo', 'naturaleza',
            'clima', 'emergencia',
        ],
        'Tecnologia': [
            'tecnologia', 'digital', 'app', 'plataforma',
            'internet', 'innovacion', 'startup', 'software'
        ],
        'Cultura y Deporte': [
            'cultura', 'arte', 'musica', 'festivales',
            'deporte', 'turismo', 'gastronomia', 'patrimonio'
        ],
        'Politica y Gobierno': [
            'politica', 'concejo', 'alcalde', 'gobernador',
            'senado', 'congreso', 'eleccion', 'votacion',
            'partido', 'gobierno', 'decreto', 'ley'
        ],
    }

    for tema, keywords in temas_map.items():
        if any(kw in texto for kw in keywords):
            return tema

    palabras = [w for w in label_subtema.split()
                if len(w) > 3 and w.lower() not in STOPWORDS_ES]
    if len(palabras) >= 3:
        return ' '.join(palabras[:4]).title()

    return label_subtema.title()


# ==============================================================================
# LLM Labeling (batch con structured output)
# ==============================================================================

SYSTEM_LABEL_PROMPT = (
    "Eres un analista experto de noticias colombianas.\n"
    "Para cada articulo, genera etiquetas coherentes en JSON.\n\n"
    "REGLAS CLAVE:\n"
    "1. SUBTEMA: Describe el EVENTO especifico (max 8 palabras).\n"
    "2. TEMA: Categoria general (transporte, salud, seguridad, economia, etc).\n"
    "3. TONO: El tono DEBE ser relativo al PROTAGONISTA "
    "(entidad/organismo principal de la noticia), no de la noticia en general.\n"
    "   - Si una entidad esta actuando para resolver un problema "
    "-> POSITIVO para esa entidad\n"
    "   - Si una entidad es criticada o sancionada "
    "-> NEGATIVO para esa entidad\n"
    "   - Si no hay accion ni juicio -> NEUTRAL\n"
    "4. Identifica quien es el PROTAGONISTA "
    "(la entidad de quien habla la noticia).\n\n"
    "Formato JSON EXACTO como una lista de objetos:\n"
    "[\n"
    '  {"subtema": "...", "tema": "...", "tono": "...", '
    '"protagonista": "...", "accion": "...", "sentimiento": "..."},\n'
    '  ...\n'
    "]\n\n"
    "tono: POSITIVO, NEGATIVO, NEUTRAL, CRISIS o MIXTO\n"
    "accion: PROACTIVA, REACTIVA, NEUTRA, INFORMATIVA\n"
    "sentimiento: Favorable, Desfavorable, Neutral o Mixto"
)


def obtener_labels_llm(articulos, api_key):
    if not articulos:
        return []

    client = openai.OpenAI(api_key=api_key)
    resultados = []
    batch_size = 15

    for start in range(0, len(articulos), batch_size):
        batch = articulos[start:start + batch_size]

        user_parts = []
        for i, art in enumerate(batch, 1):
            user_parts.append(
                "ARTICULO {}:\n".format(i) +
                "Ttulo: {}\n".format(art.get('titulo', '')) +
                "Resumen: {}\n".format(art.get('resumen', ''))
            )
        user_msg = "\n".join(user_parts)

        for intento in range(MAX_RETRIES):
            try:
                resp = client.chat.completions.create(
                    model=CLASSIFIER_MODEL,
                    temperature=0.1,
                    messages=[
                        {"role": "system", "content": SYSTEM_LABEL_PROMPT},
                        {"role": "user", "content": user_msg}
                    ],
                    response_format={"type": "json_object"}
                )
                content = resp.choices[0].message.content.strip()
                data = json.loads(content)

                if isinstance(data, dict) and "articulos" in data:
                    items = data["articulos"]
                elif isinstance(data, list):
                    items = data
                else:
                    items = [data]

                resultados.extend(items[:len(batch)])
                break

            except Exception:
                if intento < MAX_RETRIES - 1:
                    time.sleep(2 ** intento)
                else:
                    for _ in batch:
                        resultados.append({
                            "subtema": "",
                            "tema": "",
                            "tono": "NEUTRAL",
                            "protagonista": "",
                            "accion": "NEUTRA",
                            "sentimiento": "Neutral"
                        })

    return resultados


# ==============================================================================
# Pipeline completo
# ==============================================================================

TONE_COLORS = {
    "POSITIVO": "\U0001f7e2",
    "NEGATIVO": "\U0001f534",
    "NEUTRAL": "\U0001f7e1",
    "CRISIS": "\U0001f7e0",
    "MIXTO": "\U0001f535"
}


def procesar_noticias(df, use_llm=False, api_key="", progress_callback=None):
    n = len(df)

    # 1. Preparar textos
    titulos = df.get('titulo', df.get('title', pd.Series([''] * n))).astype(str).tolist()
    resumenes = df.get('resumen', df.get('summary', pd.Series([''] * n))).astype(str).tolist()
    textos_embed = [t + ". " + r for t, r in zip(titulos, resumenes)]

    # 2. Embeddings
    if progress_callback:
        progress_callback("generando embeddings", 10)
    embeddings = obtener_embeddings(textos_embed)

    if not embeddings:
        resultados = []
        for i in range(n):
            tono_info = clasificar_tono(titulos[i], resumenes[i])
            resultados.append({
                'titulo': titulos[i],
                'resumen': resumenes[i],
                'subtema': titulos[i][:80],
                'tema': 'Sin clasificar',
                **tono_info
            })
        return pd.DataFrame(resultados)

    if progress_callback:
        progress_callback("agrupando subtemas", 30)

    # 3. Clustering de subtemas
    grupos = agrupar_subtemas(embeddings, titulos, resumenes)

    if progress_callback:
        progress_callback("agrupando temas", 45)

    # 4. Agrupacion jerarquica subtemas -> temas
    grupos = agrupar_temas(grupos, embeddings, titulos)

    # 5. Asignar labels a cada artculo
    if progress_callback:
        progress_callback("clasificando tono", 55)

    resultados = []
    for gid, g in grupos.items():
        for idx in g['indices']:
            titulo = titulos[idx] if idx < len(titulos) else ''
            resumen = resumenes[idx] if idx < len(resumenes) else ''
            tono_info = clasificar_tono(titulo, resumen)

            resultados.append({
                'row_idx': idx,
                'titulo': titulo,
                'resumen': resumen,
                'subtema': g['label'],
                'subtema_group_id': gid,
                'tema': g['tema'],
                'cluster_size': len(g['indices']),
                **tono_info
            })

    # 6. Refinar con LLM (opcional)
    if use_llm and api_key:
        if progress_callback:
            progress_callback("refinando con LLM", 65)

        articulos_llm = [
            {"titulo": r['titulo'], "resumen": r['resumen'],
             "subtema": r['subtema'], "tema": r['tema']}
            for r in resultados
        ]

        labels_llm = obtener_labels_llm(articulos_llm, api_key)

        if labels_llm and len(labels_llm) == len(resultados):
            for i, lbl in enumerate(labels_llm):
                if lbl.get('subtema'):
                    resultados[i]['subtema'] = lbl['subtema']
                if lbl.get('tema'):
                    resultados[i]['tema'] = lbl['tema']
                if lbl.get('tono'):
                    resultados[i]['tono'] = lbl['tono'].upper()
                if lbl.get('protagonista'):
                    resultados[i]['protagonista'] = lbl['protagonista']
                if lbl.get('accion'):
                    resultados[i]['accion'] = lbl['accion'].upper()
                if lbl.get('sentimiento'):
                    resultados[i]['sentimiento'] = lbl['sentimiento']

    # 7. DataFrame final
    df_result = pd.DataFrame(resultados)

    col_order = [
        'titulo', 'resumen', 'protagonista', 'tono', 'accion',
        'sentimiento', 'subtema', 'tema', 'cluster_size'
    ]
    existentes = [c for c in col_order if c in df_result.columns]
    df_result = df_result[existentes]

    return df_result


# ==============================================================================
# Exportar Excel
# ==============================================================================

TONE_BG = {
    "POSITIVO": "dcfce7",
    "NEGATIVO": "fee2e2",
    "NEUTRAL": "fef3c7",
    "CRISIS": "ffedd5",
    "MIXTO": "dbeafe"
}


def exportar_excel(df):
    output = io.BytesIO()

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter

        wb = Workbook()
        ws = wb.active
        ws.title = "Analisis Noticias"

        header_fill = PatternFill('solid', fgColor='f97316')
        header_font = Font(name='Calibri', bold=True, color='FFFFFF', size=11)
        header_align = Alignment(horizontal='center', vertical='center', wrapText=True)

        thin_border = Border(
            left=Side(style='thin', color='E5E7EB'),
            right=Side(style='thin', color='E5E7EB'),
            top=Side(style='thin', color='E5E7EB'),
            bottom=Side(style='thin', color='E5E7EB')
        )

        for col_idx, col_name in enumerate(df.columns, 1):
            cell = ws.cell(row=1, column=col_idx, value=col_name)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_align
            cell.border = thin_border

        for row_idx, row in df.iterrows():
            for col_idx, col_name in enumerate(df.columns, 1):
                val = row[col_name] if col_name in row.index else ''
                cell = ws.cell(row=row_idx + 2, column=col_idx, value=val)
                cell.border = thin_border
                cell.alignment = Alignment(wrapText=True, vertical='top')
                cell.font = Font(name='Calibri', size=10)

                if col_name == 'tono' and str(val) in TONE_BG:
                    cell.fill = PatternFill('solid', fgColor=TONE_BG[str(val)])

        anchos = {
            'titulo': 55, 'resumen': 70, 'protagonista': 22,
            'tono': 12, 'accion': 12, 'sentimiento': 14,
            'subtema': 45, 'tema': 28, 'cluster_size': 10
        }
        for col_idx, col_name in enumerate(df.columns, 1):
            letter = get_column_letter(col_idx)
            ws.column_dimensions[letter].width = anchos.get(col_name, 18)

        wb.save(output)
        output.seek(0)
    except ImportError:
        output = io.BytesIO()
        output.write(df.to_csv(index=False).encode('utf-8'))
        output.seek(0)

    return output


# ==============================================================================
# Streamlit UI
# ==============================================================================

def load_custom_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
:root {
    --bg:#f8f9fa; --s1:#ffffff; --s2:#f1f3f4; --s3:#e8eaed;
    --border:#dadce0; --text:#202124; --text2:#3c4043; --text3:#5f6368;
    --accent:#f97316; --accent2:#ea580c; --accent-bg:#fff7ed; --accent-bdr:#fed7aa;
    --green:#059669; --green-bg:#ecfdf5;
    --red:#dc2626; --blue:#1a73e8;
    --r:8px; --r2:12px; --r3:16px;
    --shadow:0 1px 3px rgba(60,64,67,0.1),0 1px 2px rgba(60,64,67,0.08);
}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Inter',sans-serif!important;font-size:14px!important}
.block-container{padding-top:1rem!important;padding-bottom:1.5rem!important}
.app-header{background:var(--s1);border:1px solid var(--border);border-radius:var(--r3);padding:1rem 1.5rem;margin-bottom:1rem;display:flex;align-items:center;gap:1rem;box-shadow:var(--shadow);position:relative;overflow:hidden}
.app-header::after{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#f97316,#fb923c,#fdba74)}
.app-header-icon{width:40px;height:40px;background:linear-gradient(135deg,#f97316,#ea580c);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.2rem;color:white;flex-shrink:0}
.app-header-title{font-size:1.25rem;font-weight:700;color:var(--text);letter-spacing:-0.01em}
.app-header-sub{font-size:0.7rem;color:var(--text3)}
.metrics-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:0.6rem;margin:0.8rem 0}
.metric-card{background:var(--s1);border:1px solid var(--border);border-radius:var(--r2);padding:0.8rem;text-align:center;box-shadow:var(--shadow)}
.metric-val{font-size:1.5rem;font-weight:700;line-height:1}
.metric-lbl{font-size:0.62rem;color:var(--text3);text-transform:uppercase;letter-spacing:0.08em;font-weight:500}
.stButton>button{border-radius:100px!important;font-weight:600!important}
[data-testid="stDataFrame"]{border-radius:var(--r2)!important;overflow:hidden!important}
</style>
""", unsafe_allow_html=True)


def show_metrics(df):
    totales = len(df)
    subtemas = df['subtema'].nunique() if 'subtema' in df.columns else 0
    temas = df['tema'].nunique() if 'tema' in df.columns else 0
    pos = len(df[df['tono'] == 'POSITIVO']) if 'tono' in df.columns else 0
    neg = len(df[df['tono'] == 'NEGATIVO']) if 'tono' in df.columns else 0
    crisis = len(df[df['tono'] == 'CRISIS']) if 'tono' in df.columns else 0

    css = """
    <div class="metrics-grid">
        <div class="metric-card" style="border-top:3px solid #5f6368">
            <div class="metric-val">{total}</div><div class="metric-lbl">Noticias</div></div>
        <div class="metric-card" style="border-top:3px solid var(--green)">
            <div class="metric-val">{subtemas}</div><div class="metric-lbl">Subtemas</div></div>
        <div class="metric-card" style="border-top:3px solid var(--blue)">
            <div class="metric-val">{temas}</div><div class="metric-lbl">Temas</div></div>
        <div class="metric-card" style="border-top:3px solid #059669">
            <div class="metric-val">[green] {pos}[/green]</div><div class="metric-lbl">Positivos</div></div>
        <div class="metric-card" style="border-top:3px solid var(--red)">
            <div class="metric-val">[red] {neg}[/red]</div><div class="metric-lbl">Negativos</div></div>
        <div class="metric-card" style="border-top:3px solid #f59e0b">
            <div class="metric-val">[orange] {crisis}[/orange]</div><div class="metric-lbl">Crisis</div></div>
    </div>""".format(total=totales, subtemas=subtemas, temas=temas,
                      pos=pos, neg=neg, crisis=crisis)
    st.markdown(css, unsafe_allow_html=True)


def main():
    load_custom_css()

    st.markdown("""
<div class="app-header">
    <div class="app-header-icon">[orange]⬡[/orange]</div>
    <div>
        <div class="app-header-title">Analisis de Noticias . IA</div>
        <div class="app-header-sub">Tono (relativo al protagonista) + Tema + Subtema con clustering consistente</div>
    </div>
</div>
""", unsafe_allow_html=True)

    # Sidebar: API Key
    with st.sidebar:
        st.markdown("### Configuracion")
        api_key = st.text_input(
            "OpenAI API Key", type="password",
            value=os.getenv("OPENAI_API_KEY", "")
        )
        if api_key:
            st.session_state["openai_api_key"] = api_key

        modelo = st.selectbox(
            "Modelo clasificador",
            ["gpt-4.1-nano-2025-04-14", "gpt-5-nano-2025-08-07"],
            index=0
        )
        st.session_state["modelo"] = modelo

        st.markdown("---")
        st.markdown("#### Como funciona")
        st.markdown("""
1. **Clustering primero**: agrupa noticias similares por embeddings
2. **Un label por grupo**: todas las noticias del mismo grupo comparten subtema
3. **Jerarquia**: los subtemas se agrupan en temas coherentes
4. **Tono al protagonista**: detecta de quien habla la noticia y asigna el tono relativo a esa entidad
""")

    if not api_key:
        st.warning("Ingresa tu OpenAI API Key en el sidebar para generar embeddings y clasificar.")
        return

    # Input
    col1, col2 = st.columns([2, 1])

    with col1:
        uploaded = st.file_uploader(
            "Subir archivo (CSV o Excel con columnas titulo/resumen)",
            type=['csv', 'xlsx', 'xls']
        )
    with col2:
        st.markdown("**Formato esperado:**")
        st.markdown("""
Columnas reconocidas:
- `titulo` (o `title`)
- `resumen` (o `summary`)
- `texto_completo` (opcional)

Tambien puedes pegar texto:
""")
        pegar = st.text_area(
            "O pega noticias aqui (titulo | resumen por linea)",
            height=120
        )

    # Opciones
    use_llm = st.toggle(
        "Refinar labels con LLM (mas preciso, consume tokens)",
        value=False
    )

    # Procesar
    if st.button("Analizar noticias", type="primary", use_container_width=True):
        df_raw = None

        if uploaded is not None:
            ext = uploaded.name.split('.')[-1].lower()
            try:
                if ext == 'csv':
                    df_raw = pd.read_csv(uploaded)
                else:
                    df_raw = pd.read_excel(uploaded)
            except Exception as e:
                st.error("Error leyendo archivo: " + str(e))
                return

        elif pegar:
            rows = []
            for line in pegar.strip().split('\n'):
                if '|' in line:
                    parts = line.split('|', 1)
                    rows.append({
                        'titulo': parts[0].strip(),
                        'resumen': parts[1].strip() if len(parts) > 1 else ''
                    })
                else:
                    rows.append({'titulo': line.strip(), 'resumen': ''})
            df_raw = pd.DataFrame(rows) if rows else None

        if df_raw is None or len(df_raw) == 0:
            st.warning("No hay datos para procesar.")
            return

        st.info("Procesando " + str(len(df_raw)) + " noticias...")

        progress_bar = st.progress(0)
        status_text = st.empty()

        def update_progress(stage, pct):
            status_text.text(stage + "...")
            progress_bar.progress(pct)

        try:
            df_result = procesar_noticias(
                df_raw,
                use_llm=use_llm,
                api_key=api_key,
                progress_callback=update_progress
            )

            progress_bar.progress(100)
            status_text.text("Analisis completo!")
            time.sleep(1)
            status_text.empty()
            progress_bar.empty()

            st.success(str(len(df_result)) + " noticias clasificadas correctamente")
            show_metrics(df_result)

            st.markdown("---")

            # Tabs por tema
            temas_unicos = df_result['tema'].unique() if 'tema' in df_result.columns else []

            if len(temas_unicos) > 1:
                tab_cols = st.columns(min(len(temas_unicos), 4))

                for i, tema in enumerate(temas_unicos):
                    df_tema = df_result[df_result['tema'] == tema]
                    with tab_cols[i % len(tab_cols)]:
                        with st.expander("**" + tema + "** (" + str(len(df_tema)) + ")", expanded=True):
                            for _, row in df_tema.iterrows():
                                tono_emoji = TONE_COLORS.get(row.get('tono', ''), '')
                                st.markdown(tono_emoji + " **" + str(row.get('titulo', '')) + "**")
                                if row.get('subtema'):
                                    st.caption("  " + str(row['subtema']))
                                st.markdown("")

            st.markdown("---")
            st.subheader("Tabla completa")
            st.dataframe(df_result, use_container_width=True, height=500)

            excel_data = exportar_excel(df_result)
            st.download_button(
                "Descargar Excel",
                data=excel_data,
                file_name="noticias_analizadas_" + str(datetime.date.today()) + ".xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        except Exception as e:
            st.error("Error en el analisis: " + str(e))
            import traceback
            st.code(traceback.format_exc())


if __name__ == "__main__":
    main()
