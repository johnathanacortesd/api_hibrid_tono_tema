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
    page_title="Análisis de Noticias · IA",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

OPENAI_MODEL_EMBEDDING     = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

CONCURRENT_REQUESTS          = 50
SIMILARITY_THRESHOLD_TONO    = 0.92
SIMILARITY_THRESHOLD_TITULOS = 0.93

UMBRAL_SUBTEMA = 0.82
UMBRAL_TEMA    = 0.76
NUM_TEMAS_MAX  = 20

UMBRAL_DEDUP_LABEL = 0.78
UMBRAL_FUSION_INTERGRUPO = 0.84
MAX_ITER_FUSION = 5

PRICE_INPUT_1M     = 0.10
PRICE_OUTPUT_1M    = 0.40
PRICE_EMBEDDING_1M = 0.02

if 'tokens_input'     not in st.session_state: st.session_state['tokens_input']     = 0
if 'tokens_output'    not in st.session_state: st.session_state['tokens_output']    = 0
if 'tokens_embedding' not in st.session_state: st.session_state['tokens_embedding'] = 0

CIUDADES_COLOMBIA = {
    "bogotá","bogota","medellín","medellin","cali","barranquilla","cartagena",
    "cúcuta","cucuta","bucaramanga","pereira","manizales","armenia","ibagué",
    "ibague","villavicencio","montería","monteria","neiva","pasto","valledupar",
    "popayán","popayan","tunja","florencia","sincelejo","riohacha","yopal",
    "santa marta","santamarta","quibdó","quibdo","leticia","mocoa","mitú","mitu",
    "puerto carreño","inírida","inirida","san josé del guaviare","antioquia",
    "atlántico","atlantico","bolívar","bolivar","boyacá","boyaca","caldas",
    "caquetá","caqueta","casanare","cauca","cesar","chocó","choco","córdoba",
    "cordoba","cundinamarca","guainía","guainia","guaviare","huila","la guajira",
    "magdalena","meta","nariño","narino","norte de santander","putumayo",
    "quindío","quindio","risaralda","san andrés","san andres","santander",
    "sucre","tolima","valle del cauca","vaupés","vaupes","vichada",
}
GENTILICIOS_COLOMBIA = {
    "bogotano","bogotanos","bogotana","bogotanas","capitalino","capitalinos",
    "capitalina","capitalinas","antioqueño","antioqueños","antioqueña",
    "antioqueñas","paisa","paisas","medellense","medellenses","caleño",
    "caleños","caleña","caleñas","valluno","vallunos","valluna","vallunas",
    "vallecaucano","vallecaucanos","barranquillero","barranquilleros",
    "cartagenero","cartageneros","costeño","costeños","costeña","costeñas",
    "cucuteño","cucuteños","bumangués","santandereano","santandereanos",
    "boyacense","boyacenses","tolimense","tolimenses","huilense","huilenses",
    "nariñense","nariñenses","pastuso","pastusas","cordobés","cordobeses",
    "cauca","caucano","caucanos","chocoano","chocoanos","casanareño",
    "casanareños","caqueteño","caqueteños","guajiro","guajiros","llanero",
    "llaneros","amazonense","amazonenses","colombiano","colombianos",
    "colombiana","colombianas",
}

STOPWORDS_ES = set("""
a ante bajo cabe con contra de desde durante en entre hacia hasta mediante
para por segun sin so sobre tras y o u e la el los las un una unos unas lo
al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este
esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual
cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue
fueron era eran sera seran seria serian he ha han habia habian hay hubo habra
habria estoy esta estan estaba estaban estamos estan estar estare estaria
estuvieron estarian estuvo asi ya mas menos tan tanto cada muy todo toda todos
todas ser haber hacer tener poder deber ir dar ver saber querer llegar pasar
encontrar creer decir poner salir volver seguir llevar sentir cambiar
""".split())

# Palabras que NO pueden terminar una frase temática (dejan la frase incompleta)
_TRAILING_INCOMPLETE = {
    "de","del","la","el","los","las","un","una","unos","unas","al","su","sus",
    "en","con","sin","por","para","sobre","ante","bajo","contra","desde",
    "entre","hacia","hasta","mediante","tras","y","o","u","e","lo","que","se",
    "como","donde","cuando","cual","cuyo","cuya","cuyos","cuyas",
    "este","esta","estos","estas","ese","esa","esos","esas",
    "aquel","aquella","aquellos","aquellas","cada","todo","toda","todos","todas",
    "otro","otra","otros","otras","nuevo","nueva","nuevos","nuevas",
    "gran","grandes","mayor","mayores","menor","menores","mejor","mejores",
    "peor","peores","primer","primera","segundo","segunda","tercer","tercera",
    "más","mas","muy","tan","tanto","tanta","tantos","tantas",
    "mi","mis","tu","tus","nuestro","nuestra","nuestros","nuestras",
    "a","ha","he","ser","estar","haber","hacer","tener","poder","deber",
    "ir","dar","ver","saber","querer","llegar","pasar","decir","poner",
}

POS_VARIANTS = [
    r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)",
    r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)",
    r"apertur(a|ar|ara|o|an)",r"estren(a|o|ara|an|ando)",r"habilit(a|o|ara|an|ando)",
    r"disponible",r"mejor(a|o|an|ando)",r"optimiza|amplia|expande",
    r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?",
    r"crecimi?ento|aument(a|o|an|ando)",r"gananci(a|as)|utilidad(es)?|benefici(o|os)",
    r"expansion|crece|crecer",r"inversion|invierte|invertir",
    r"innova(cion|dor|ndo)|moderniza",r"exito(so|sa)?|logr(o|os|a|an|ando)",
    r"reconoci(miento|do|da)|premi(o|os|ada)",r"lidera(zgo)?|lider",
    r"consolida|fortalece",r"oportunidad(es)?|potencial",r"solucion(es)?|resuelve",
    r"eficien(te|cia)",r"calidad|excelencia",r"satisfaccion|complace",
    r"confianza|credibilidad",r"sostenible|responsable",r"compromiso|apoya|apoyar",
    r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)",
    r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)",
    r"destaca(r|do|da|ndo)?",r"supera(r|ndo|cion)?",r"record|hito|milestone",
    r"avanza(r|do|da|ndo)?",r"benefici(a|o|ando|ar|ando)",r"importante(s)?",
    r"prioridad",r"bienestar",r"garantizar",r"seguridad",r"atencion",
    r"expres(o|ó|ando)",r"señala(r|do|ando)",r"ratific(a|o|ando|ar)",
]
NEG_VARIANTS = [
    r"demanda|denuncia|sanciona|multa|investiga|critica",
    r"cae|baja|pierde|crisis|quiebra|default",
    r"fraude|escandalo|irregularidad",
    r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga",
    r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora",
    r"problema(s|tica|ico)?|dificultad(es)?",r"retras(o|a|ar|ado)",
    r"perdida(s)?|deficit",
    r"conflict(o|os)?|disputa(s)?",r"rechaz(a|o|ar|ado)",r"negativ(o|a|os|as)",
    r"preocupa(cion|nte|do)?",r"alarma(nte)?|alerta",r"riesgo(s)?|amenaza(s)?",
]
CRISIS_KEYWORDS = re.compile(
    r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|"
    r"damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS = re.compile(
    r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|"
    r"responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|"
    r"gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)
POS_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]

_TILDE_MAP = {
    "regulacion":"regulación","regulaciones":"regulaciones",
    "innovacion":"innovación","innovaciones":"innovaciones",
    "tecnologia":"tecnología","tecnologias":"tecnologías",
    "tecnologica":"tecnológica","tecnologico":"tecnológico",
    "educacion":"educación","gestion":"gestión",
    "administracion":"administración","informacion":"información",
    "comunicacion":"comunicación","comunicaciones":"comunicaciones",
    "operacion":"operación","operaciones":"operaciones",
    "inversion":"inversión","inversiones":"inversiones",
    "expansion":"expansión","adquisicion":"adquisición",
    "adquisiciones":"adquisiciones","fusion":"fusión",
    "fusiones":"fusiones","transicion":"transición",
    "transformacion":"transformación","digitalizacion":"digitalización",
    "automatizacion":"automatización","modernizacion":"modernización",
    "optimizacion":"optimización","implementacion":"implementación",
    "evaluacion":"evaluación","planificacion":"planificación",
    "organizacion":"organización","atencion":"atención",
    "produccion":"producción","construccion":"construcción",
    "distribucion":"distribución","exportacion":"exportación",
    "importacion":"importación","comercializacion":"comercialización",
    "negociacion":"negociación","negociaciones":"negociaciones",
    "participacion":"participación","colaboracion":"colaboración",
    "asociacion":"asociación","integracion":"integración",
    "relacion":"relación","relaciones":"relaciones",
    "situacion":"situación","condicion":"condición",
    "condiciones":"condiciones","solucion":"solución",
    "soluciones":"soluciones","prevencion":"prevención",
    "proteccion":"protección","fiscalizacion":"fiscalización",
    "sancion":"sanción","sanciones":"sanciones",
    "investigacion":"investigación","investigaciones":"investigaciones",
    "accion":"acción","acciones":"acciones",
    "direccion":"dirección","decision":"decisión",
    "decisiones":"decisiones","eleccion":"elección",
    "elecciones":"elecciones","votacion":"votación",
    "aprobacion":"aprobación","legislacion":"legislación",
    "reclamacion":"reclamación","reclamaciones":"reclamaciones",
    "obligacion":"obligación","obligaciones":"obligaciones",
    "inflacion":"inflación","tributacion":"tributación",
    "financiera":"financiera","financiero":"financiero",
    "economica":"económica","economico":"económico",
    "economia":"economía","credito":"crédito",
    "creditos":"créditos","prestamo":"préstamo",
    "prestamos":"préstamos","interes":"interés",
    "comision":"comisión","comisiones":"comisiones",
    "politica":"política","politicas":"políticas",
    "politico":"político","publica":"pública",
    "publico":"público","estrategia":"estrategia",
    "estrategica":"estratégica","estrategico":"estratégico",
    "logistica":"logística","analisis":"análisis",
    "diagnostico":"diagnóstico","indice":"índice",
    "vehiculo":"vehículo","vehiculos":"vehículos",
    "electrico":"eléctrico","electrica":"eléctrica",
    "energia":"energía","energetica":"energética",
    "petroleo":"petróleo","mineria":"minería",
    "agricola":"agrícola","biologica":"biológica",
    "ecologica":"ecológica","inclusion":"inclusión",
    "exclusion":"exclusión","pension":"pensión",
    "pensiones":"pensiones","jubilacion":"jubilación",
    "compensacion":"compensación","remuneracion":"remuneración",
    "contratacion":"contratación","capacitacion":"capacitación",
    "formacion":"formación","certificacion":"certificación",
    "habilitacion":"habilitación","autorizacion":"autorización",
    "concesion":"concesión","licitacion":"licitación",
    "migracion":"migración","poblacion":"población",
    "recaudacion":"recaudación","asignacion":"asignación",
    "corporacion":"corporación","fundacion":"fundación",
    "institucion":"institución","instituciones":"instituciones",
    "region":"región","unico":"único","unica":"única",
    "ultimo":"último","ultima":"última","proximo":"próximo",
    "basico":"básico","basica":"básica","historico":"histórico",
    "historica":"histórica","medico":"médico","medica":"médica",
    "farmaceutica":"farmacéutica","clinica":"clínica",
    "numero":"número","telefono":"teléfono","telefonia":"telefonía",
    "movil":"móvil","moviles":"móviles","codigo":"código",
    "informatica":"informática","electronica":"electrónica",
    "robotica":"robótica","ciberseguridad":"ciberseguridad",
    "trafico":"tráfico","transito":"tránsito","aereo":"aéreo",
    "maritimo":"marítimo","turistica":"turística",
    "turistico":"turístico","gastronomia":"gastronomía",
    "academica":"académica","academico":"académico",
    "pedagogica":"pedagógica","cientifica":"científica",
    "cientifico":"científico","juridica":"jurídica",
    "juridico":"jurídico","constitucion":"constitución",
    "resolucion":"resolución","notificacion":"notificación",
    "programacion":"programación","actualizacion":"actualización",
    "verificacion":"verificación","validacion":"validación",
    "liquidacion":"liquidación","facturacion":"facturación",
    "evasion":"evasión","corrupcion":"corrupción",
    "deforestacion":"deforestación","contaminacion":"contaminación",
    "conservacion":"conservación","restauracion":"restauración",
    "rehabilitacion":"rehabilitación","renovacion":"renovación",
    "ampliacion":"ampliación","inauguracion":"inauguración",
    "celebracion":"celebración","clasificacion":"clasificación",
    "eliminacion":"eliminación","motivacion":"motivación",
    "satisfaccion":"satisfacción","reputacion":"reputación",
    "disposicion":"disposición",
}

def corregir_tildes(texto: str) -> str:
    if not texto: return texto
    palabras = texto.split()
    resultado = []
    for p in palabras:
        low = p.lower()
        if low in _TILDE_MAP:
            c = _TILDE_MAP[low]
            if p[0].isupper(): c = c[0].upper() + c[1:]
            resultado.append(c)
        else:
            resultado.append(p)
    return " ".join(resultado)


# ======================================
# CSS — v15 Visual Overhaul
# ======================================
def load_custom_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
    --bg: #f4f4f1;
    --s1: #ffffff;
    --s2: #f0efeb;
    --s3: #e6e5e0;
    --border: #dddbd4;
    --border2: #c5c2b8;
    --border-focus: #818cf8;
    --text: #111111;
    --text2: #4a4a4a;
    --text3: #8a8a85;
    --text4: #b5b5ae;
    --accent: #6366f1;
    --accent2: #4f46e5;
    --accent3: #4338ca;
    --accent-bg: #eef2ff;
    --accent-bg2: #e0e7ff;
    --accent-bdr: #c7d2fe;
    --green: #059669;
    --green2: #047857;
    --green-bg: #ecfdf5;
    --green-bdr: #a7f3d0;
    --red: #dc2626;
    --red-bg: #fef2f2;
    --amber: #d97706;
    --amber-bg: #fffbeb;
    --blue: #2563eb;
    --blue-bg: #eff6ff;
    --r: 10px;
    --r2: 14px;
    --r3: 20px;
    --r4: 24px;
    --shadow-xs: 0 1px 2px rgba(0,0,0,0.04);
    --shadow-sm: 0 1px 3px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.03);
    --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.06), 0 2px 4px -2px rgba(0,0,0,0.04);
    --shadow-lg: 0 10px 25px -5px rgba(0,0,0,0.08), 0 8px 10px -6px rgba(0,0,0,0.04);
    --shadow-xl: 0 20px 40px -10px rgba(0,0,0,0.1);
    --transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

html, body, [data-testid="stApp"] {
    background: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 16px;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

#MainMenu, footer, header { visibility: hidden }
.stDeployButton { display: none }

.app-header {
    background: var(--s1);
    border: 1px solid var(--border);
    border-radius: var(--r4);
    padding: 2rem 2.5rem;
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    gap: 1.5rem;
    box-shadow: var(--shadow-md);
    position: relative;
    overflow: hidden;
}
.app-header::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 4px;
    background: linear-gradient(90deg, var(--accent), var(--accent2), var(--accent3));
}
.app-header-icon {
    width: 56px; height: 56px;
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent3) 100%);
    border-radius: 16px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.6rem; color: white; flex-shrink: 0;
    box-shadow: 0 4px 12px rgba(99,102,241,0.3);
}
.app-header-text { flex: 1 }
.app-header-title {
    font-size: 1.6rem; font-weight: 800;
    color: var(--text);
    letter-spacing: -0.03em;
    line-height: 1.2;
}
.app-header-version {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem; color: var(--text3);
    letter-spacing: 0.06em; margin-top: 0.3rem;
}
.app-header-badge {
    background: linear-gradient(135deg, var(--accent-bg) 0%, var(--accent-bg2) 100%);
    border: 1px solid var(--accent-bdr);
    color: var(--accent2);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem; font-weight: 600;
    padding: 0.35rem 0.9rem;
    border-radius: 24px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    white-space: nowrap;
}

[data-testid="stTabs"] [data-testid="stTabsList"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r2) !important;
    padding: 5px !important;
    gap: 5px !important;
    box-shadow: var(--shadow-sm) !important;
}
[data-testid="stTabs"] button[data-baseweb="tab"] {
    font-family: 'Inter' !important;
    font-size: 0.95rem !important;
    font-weight: 500 !important;
    color: var(--text2) !important;
    border-radius: var(--r) !important;
    padding: 0.65rem 1.5rem !important;
    border: none !important;
    background: transparent !important;
    transition: var(--transition) !important;
}
[data-testid="stTabs"] button[data-baseweb="tab"]:hover {
    background: var(--s2) !important;
    color: var(--text) !important;
}
[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
    background: var(--accent-bg) !important;
    color: var(--accent2) !important;
    border: 1px solid var(--accent-bdr) !important;
    font-weight: 700 !important;
}

.metrics-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 1rem;
    margin: 1.5rem 0;
}
.metric-card {
    background: var(--s1);
    border: 1px solid var(--border);
    border-radius: var(--r3);
    padding: 1.5rem 1rem;
    text-align: center;
    transition: var(--transition);
    box-shadow: var(--shadow-sm);
    position: relative;
    overflow: hidden;
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 4px;
    border-radius: var(--r3) var(--r3) 0 0;
}
.metric-card.m-total::before  { background: linear-gradient(90deg, #6b7280, #9ca3af) }
.metric-card.m-unique::before { background: linear-gradient(90deg, var(--green), var(--green2)) }
.metric-card.m-dup::before    { background: linear-gradient(90deg, #f59e0b, var(--amber)) }
.metric-card.m-time::before   { background: linear-gradient(90deg, #3b82f6, var(--blue)) }
.metric-card.m-cost::before   { background: linear-gradient(90deg, var(--accent), var(--accent3)) }
.metric-card:hover {
    border-color: var(--border2);
    transform: translateY(-3px);
    box-shadow: var(--shadow-lg);
}
.metric-val {
    font-size: 2rem;
    font-weight: 800;
    line-height: 1;
    margin-bottom: 0.5rem;
    letter-spacing: -0.02em;
}
.metric-lbl {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    color: var(--text3);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-weight: 500;
}

[data-testid="stForm"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r4) !important;
    padding: 2.5rem !important;
    box-shadow: var(--shadow-md) !important;
}

.sec-label {
    font-size: 0.8rem;
    font-weight: 700;
    color: var(--text2);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding-bottom: 0.6rem;
    border-bottom: 2px solid var(--s3);
    margin: 2rem 0 1rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
}
.sec-label::before {
    content: '';
    display: inline-block;
    width: 4px; height: 16px;
    background: linear-gradient(180deg, var(--accent), var(--accent3));
    border-radius: 2px;
}

.upload-zone {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 1.2rem;
    margin: 0.8rem 0 0.4rem;
}
.upload-zone-card {
    background: linear-gradient(135deg, var(--s1) 0%, var(--s2) 100%);
    border: 2px dashed var(--border);
    border-radius: var(--r3);
    padding: 1.8rem 1.2rem 1rem;
    text-align: center;
    transition: var(--transition);
    position: relative;
    overflow: hidden;
}
.upload-zone-card::before {
    content: '';
    position: absolute;
    inset: 0;
    border-radius: var(--r3);
    background: linear-gradient(135deg,
        rgba(99,102,241,0.03) 0%,
        rgba(79,70,229,0.06) 100%);
    opacity: 0;
    transition: opacity 0.3s;
}
.upload-zone-card:hover {
    border-color: var(--accent);
    border-style: solid;
    transform: translateY(-2px);
    box-shadow: var(--shadow-lg);
}
.upload-zone-card:hover::before { opacity: 1 }
.upload-zone-icon {
    width: 52px; height: 52px;
    border-radius: 14px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 1.5rem;
    margin-bottom: 0.8rem;
    position: relative;
    z-index: 1;
}
.upload-zone-icon.uz-dossier { background: #eef2ff; color: #6366f1; }
.upload-zone-icon.uz-region  { background: #ecfdf5; color: #059669; }
.upload-zone-icon.uz-internet { background: #eff6ff; color: #2563eb; }
.upload-zone-title {
    font-size: 1rem;
    font-weight: 700;
    color: var(--text);
    margin-bottom: 0.3rem;
    position: relative;
    z-index: 1;
}
.upload-zone-desc {
    font-size: 0.82rem;
    color: var(--text3);
    line-height: 1.4;
    position: relative;
    z-index: 1;
}
.upload-zone-hint {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: var(--text4);
    margin-top: 0.6rem;
    padding-top: 0.6rem;
    border-top: 1px solid var(--s3);
    position: relative;
    z-index: 1;
}

[data-testid="stFileUploader"] {
    background: linear-gradient(135deg, var(--s2) 0%, var(--s1) 100%) !important;
    border: 2px dashed var(--border) !important;
    border-radius: var(--r2) !important;
    padding: 1rem !important;
    transition: var(--transition) !important;
    min-height: 70px !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: var(--accent) !important;
    border-style: solid !important;
    background: var(--accent-bg) !important;
    box-shadow: 0 0 0 4px rgba(99,102,241,0.08) !important;
}
[data-testid="stFileUploader"] section {
    padding: 0.5rem !important;
}
[data-testid="stFileUploader"] section > div {
    font-size: 0.85rem !important;
    color: var(--text2) !important;
}
[data-testid="stFileUploader"] section small {
    font-size: 0.78rem !important;
    color: var(--text3) !important;
}
[data-testid="stFileUploader"] button {
    background: var(--accent-bg) !important;
    border: 1px solid var(--accent-bdr) !important;
    color: var(--accent2) !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    border-radius: var(--r) !important;
    padding: 0.4rem 1rem !important;
    transition: var(--transition) !important;
}
[data-testid="stFileUploader"] button:hover {
    background: var(--accent) !important;
    color: white !important;
    border-color: var(--accent) !important;
}

[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea {
    background: var(--s1) !important;
    border: 1.5px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: var(--r) !important;
    font-family: 'Inter' !important;
    font-size: 1rem !important;
    font-weight: 400 !important;
    padding: 0.75rem 1rem !important;
    transition: var(--transition) !important;
}
[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    border-color: var(--border-focus) !important;
    box-shadow: 0 0 0 4px rgba(99,102,241,0.1) !important;
    outline: none !important;
}
[data-testid="stTextInput"] input::placeholder,
[data-testid="stTextArea"] textarea::placeholder {
    color: var(--text4) !important;
    font-size: 0.95rem !important;
}
label[data-testid="stWidgetLabel"] p {
    color: var(--text2) !important;
    font-size: 0.92rem !important;
    font-weight: 600 !important;
    margin-bottom: 0.3rem !important;
}

.stButton > button,
[data-testid="stDownloadButton"] > button {
    background: var(--s1) !important;
    border: 1.5px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: var(--r) !important;
    font-family: 'Inter' !important;
    font-weight: 600 !important;
    font-size: 0.95rem !important;
    transition: var(--transition) !important;
    padding: 0.65rem 1.5rem !important;
    box-shadow: var(--shadow-xs) !important;
}
.stButton > button:hover,
[data-testid="stDownloadButton"] > button:hover {
    border-color: var(--accent) !important;
    color: var(--accent2) !important;
    background: var(--accent-bg) !important;
    box-shadow: var(--shadow-md) !important;
    transform: translateY(-1px) !important;
}
.stButton > button[kind="primary"],
[data-testid="stDownloadButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent3) 100%) !important;
    border: none !important;
    color: #fff !important;
    font-weight: 700 !important;
    font-size: 1rem !important;
    padding: 0.75rem 2rem !important;
    box-shadow: 0 4px 14px rgba(99,102,241,0.35) !important;
    letter-spacing: 0.01em !important;
}
.stButton > button[kind="primary"]:hover,
[data-testid="stDownloadButton"] > button[kind="primary"]:hover {
    box-shadow: 0 6px 24px rgba(99,102,241,0.45) !important;
    transform: translateY(-2px) !important;
    color: #fff !important;
}

[data-testid="stRadio"] label {
    color: var(--text2) !important;
    font-size: 0.95rem !important;
    font-weight: 500 !important;
}

[data-testid="stStatus"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r2) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.85rem !important;
    box-shadow: var(--shadow-sm) !important;
}

[data-testid="stAlert"] {
    background: var(--s1) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--r2) !important;
    color: var(--text2) !important;
    font-size: 0.92rem !important;
    padding: 1rem 1.2rem !important;
}

.success-banner {
    background: linear-gradient(135deg, var(--green-bg) 0%, #d1fae5 100%);
    border: 1px solid var(--green-bdr);
    border-left: 5px solid var(--green);
    border-radius: var(--r3);
    padding: 1.5rem 2rem;
    margin: 1rem 0 1.5rem;
    box-shadow: var(--shadow-sm);
    display: flex;
    align-items: center;
    gap: 1.2rem;
}
.success-icon {
    width: 44px; height: 44px;
    background: linear-gradient(135deg, var(--green), var(--green2));
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    color: white; font-size: 1.3rem; flex-shrink: 0;
    box-shadow: 0 3px 8px rgba(5,150,105,0.3);
}
.success-title {
    font-size: 1.15rem;
    font-weight: 700;
    color: var(--green2);
    margin-bottom: 0.15rem;
}
.success-sub {
    font-size: 0.88rem;
    color: var(--text2);
}

.auth-wrap {
    max-width: 400px;
    margin: 10vh auto 0;
    text-align: center;
}
.auth-icon {
    width: 72px; height: 72px;
    background: linear-gradient(135deg, var(--accent), var(--accent3));
    border-radius: 20px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 2rem; color: white;
    margin-bottom: 1.2rem;
    box-shadow: 0 6px 20px rgba(99,102,241,0.35);
}
.auth-title {
    font-size: 1.8rem; font-weight: 800;
    color: var(--text);
    margin-bottom: 0.4rem;
    letter-spacing: -0.02em;
}
.auth-sub {
    font-size: 0.92rem;
    color: var(--text3);
    margin-bottom: 2.5rem;
}

.cluster-info {
    background: linear-gradient(135deg, var(--accent-bg) 0%, var(--accent-bg2) 100%);
    border: 1px solid var(--accent-bdr);
    border-radius: var(--r2);
    padding: 1.2rem 1.5rem;
    margin: 0.8rem 0;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: var(--text2);
    line-height: 2;
}
.cluster-info b {
    color: var(--accent2);
    font-size: 0.82rem;
}

[data-testid="stProgressBar"] > div > div {
    background: linear-gradient(90deg, var(--accent), var(--accent2), var(--accent3)) !important;
    border-radius: 6px !important;
    height: 6px !important;
}

[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--r2) !important;
    box-shadow: var(--shadow-sm) !important;
    overflow: hidden !important;
}

hr { border-color: var(--s3) !important }

::-webkit-scrollbar { width: 8px; height: 8px }
::-webkit-scrollbar-track { background: var(--s2); border-radius: 4px }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 4px }
::-webkit-scrollbar-thumb:hover { background: var(--accent) }

.footer {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: var(--text4);
    text-align: center;
    padding: 2rem 0 1rem;
    letter-spacing: 0.08em;
    border-top: 1px solid var(--s3);
    margin-top: 3rem;
}

@media (max-width: 768px) {
    .metrics-grid { grid-template-columns: repeat(2, 1fr) }
    .upload-zone { grid-template-columns: 1fr }
    .app-header { flex-direction: column; text-align: center; gap: 1rem; padding: 1.5rem }
    .app-header-badge { margin-top: 0.5rem }
}
</style>
""", unsafe_allow_html=True)


# ======================================
# Caché Global de Embeddings
# ======================================
class EmbeddingCache:
    def __init__(self):
        self._cache: Dict[str, List[float]] = {}
        self._hits = 0
        self._misses = 0
    def _key(self, text): return hashlib.md5(text[:2000].encode('utf-8', errors='ignore')).hexdigest()
    def get(self, text):
        k = self._key(text)
        if k in self._cache: self._hits += 1; return self._cache[k]
        self._misses += 1; return None
    def put(self, text, emb): self._cache[self._key(text)] = emb
    def get_many(self, textos):
        results = [None]*len(textos); missing = []
        for i, t in enumerate(textos):
            c = self.get(t)
            if c is not None: results[i] = c
            else: missing.append(i)
        return results, missing
    def stats(self):
        total = self._hits + self._misses
        rate = (self._hits/total*100) if total>0 else 0
        return f"Cache: {self._hits} hits, {self._misses} misses ({rate:.0f}%)"
    def clear(self): self._cache.clear(); self._hits = 0; self._misses = 0

if '_emb_cache' not in st.session_state:
    st.session_state['_emb_cache'] = EmbeddingCache()
def get_embedding_cache(): return st.session_state['_emb_cache']


# ======================================
# Utilidades
# ======================================
def check_password():
    if st.session_state.get("password_correct", False): return True
    st.markdown("""
    <div class="auth-wrap">
        <div class="auth-icon">◈</div>
        <div class="auth-title">Sistema de Análisis</div>
        <div class="auth-sub">Ingresa tus credenciales para continuar</div>
    </div>""", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("pw"):
            pw = st.text_input("Contraseña", type="password", placeholder="Ingresa tu contraseña")
            if st.form_submit_button("Ingresar", use_container_width=True, type="primary"):
                if pw == st.secrets.get("APP_PASSWORD", "INVALID"):
                    st.session_state["password_correct"] = True; st.rerun()
                else: st.error("Contraseña incorrecta")
    return False

def call_with_retries(fn, *a, **kw):
    d = 1
    for att in range(3):
        try: return fn(*a, **kw)
        except Exception as e:
            if att == 2: raise e
            time.sleep(d); d *= 2

async def acall_with_retries(fn, *a, **kw):
    d = 1
    for att in range(3):
        try: return await fn(*a, **kw)
        except Exception as e:
            if att == 2: raise e
            await asyncio.sleep(d); d *= 2

def norm_key(text):
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def capitalizar_etiqueta(tema):
    if not tema or not tema.strip(): return "Sin tema"
    tema = tema.strip().lower()
    tema = corregir_tildes(tema)
    return tema[0].upper() + tema[1:]


def _frase_esta_completa(texto):
    """Verifica que una frase temática no termina en palabra incompleta."""
    if not texto or not texto.strip():
        return False
    palabras = texto.strip().split()
    if not palabras:
        return False
    ultima = palabras[-1].lower().rstrip(".,;:!?")
    # Remover tildes para comparar
    ultima_norm = unidecode(ultima)
    return ultima_norm not in _TRAILING_INCOMPLETE and len(ultima) > 1


def _recortar_frase_completa(texto, max_palabras=7):
    """Recorta a max_palabras pero asegura que la frase termine en palabra sustantiva."""
    if not texto:
        return "Sin tema"
    palabras = texto.strip().split()
    # Primero recortar al máximo
    if len(palabras) > max_palabras:
        palabras = palabras[:max_palabras]
    # Ahora quitar palabras finales que dejan la frase incompleta
    while palabras and unidecode(palabras[-1].lower().rstrip(".,;:!?")) in _TRAILING_INCOMPLETE:
        palabras.pop()
    # Si quedó vacío o solo 1 palabra muy corta, devolver lo que había
    if not palabras:
        return texto.strip().split()[0] if texto.strip() else "Sin tema"
    return " ".join(palabras)


def limpiar_tema(tema):
    if not tema: return "Sin tema"
    tema = tema.strip().strip('"\'')
    for px in ["subtema:","tema:","categoría:","categoria:","category:"]:
        if tema.lower().startswith(px): tema = tema[len(px):].strip()
    # Recortar de forma segura (no dejar frases incompletas)
    tema = _recortar_frase_completa(tema, max_palabras=7)
    return capitalizar_etiqueta(tema) if tema else "Sin tema"


def limpiar_tema_geografico(tema, marca, aliases):
    if not tema: return "Sin tema"
    tl = unidecode(tema.lower())
    for n in [marca]+[a for a in aliases if a]:
        tl = re.sub(rf'\b{re.escape(unidecode(n.strip().lower()))}\b','',tl)
    for c in CIUDADES_COLOMBIA: tl = re.sub(rf'\b{re.escape(c)}\b','',tl)
    for g in GENTILICIOS_COLOMBIA: tl = re.sub(rf'\b{re.escape(g)}\b','',tl)
    for f in ["en colombia","de colombia","del pais","en el pais","nacional","colombiano","colombiana","colombianos","colombianas","territorio nacional"]:
        tl = re.sub(rf'\b{re.escape(f)}\b','',tl)
    p = [x.strip() for x in tl.split() if x.strip()]
    return limpiar_tema(" ".join(p)) if p else "Sin tema"

def string_norm_label(s):
    if not s: return ""
    s = unidecode(s.lower()); s = re.sub(r"[^a-z0-9\s]"," ",s)
    return " ".join(t for t in s.split() if t not in STOPWORDS_ES)

def extract_link(cell):
    if hasattr(cell,"hyperlink") and cell.hyperlink: return {"value":"Link","url":cell.hyperlink.target}
    if isinstance(cell.value,str) and "=HYPERLINK" in cell.value:
        m = re.search(r'=HYPERLINK\("([^"]+)"',cell.value)
        if m: return {"value":"Link","url":m.group(1)}
    return {"value":cell.value,"url":None}

def normalize_title_for_comparison(title):
    if not isinstance(title,str): return ""
    tmp = re.split(r"\s*[:|-]\s*",title,1)
    return re.sub(r"\W+"," ",tmp[0]).lower().strip()

def clean_title_for_output(title): return re.sub(r"\s*\|\s*[\w\s]+$","",str(title)).strip()

def corregir_texto(text):
    if not isinstance(text,str): return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)"," ",text).strip()
    m = re.search(r"[A-ZÁÉÍÓÚÑ]",text)
    if m: text = text[m.start():]
    if text and not text.endswith("..."): text = text.rstrip(".")+"..."
    return text

def normalizar_tipo_medio(tipo_raw):
    if not isinstance(tipo_raw,str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    return {"fm":"Radio","am":"Radio","radio":"Radio","aire":"Televisión","cable":"Televisión","tv":"Televisión","television":"Televisión","televisión":"Televisión","senal abierta":"Televisión","señal abierta":"Televisión","diario":"Prensa","prensa":"Prensa","revista":"Revista","revistas":"Revista","online":"Internet","internet":"Internet","digital":"Internet","web":"Internet"}.get(t,str(tipo_raw).strip().title() or "Otro")

def texto_para_embedding(titulo,resumen,max_len=1800):
    t = str(titulo or "").strip(); r = str(resumen or "").strip()
    return f"{t}. {t}. {t}. {r}"[:max_len]

# ======================================
# Validación de etiquetas completas
# ======================================
def _validar_etiqueta_completa(etiqueta, titulos_grp=None, resumenes_grp=None, marca="", aliases=None, fallback_fn=None):
    """
    Valida que una etiqueta sea una frase completa.
    Si no lo es, intenta regenerarla o usa fallback.
    """
    if not etiqueta or etiqueta.strip().lower() in ("sin tema", "varios", "n/a"):
        if fallback_fn:
            return fallback_fn(titulos_grp or [])
        return "Cobertura informativa general"

    # Verificar completitud
    if _frase_esta_completa(etiqueta):
        return etiqueta

    # La frase está incompleta - intentar arreglar recortando
    recortada = _recortar_frase_completa(etiqueta, max_palabras=7)
    if _frase_esta_completa(recortada) and len(recortada.split()) >= 2:
        return capitalizar_etiqueta(recortada)

    # Si recortar no funcionó, regenerar con LLM
    if titulos_grp and len(titulos_grp) > 0:
        try:
            prompt = (
                f"La frase '{etiqueta}' está incompleta como categoría temática. "
                f"Genera una frase temática COMPLETA en español de 3-5 palabras basada en estos títulos:\n\n"
                + "\n".join(f"  · {t[:120]}" for t in titulos_grp[:4]) + "\n\n"
                "REGLAS:\n"
                "1. La frase debe terminar en un sustantivo o adjetivo, NUNCA en artículo/preposición\n"
                "2. Debe ser coherente y auto-contenida\n"
                "3. NO uses nombres de empresas, marcas ni ciudades\n"
                "4. Usa tildes correctas\n\n"
                "INCORRECTO: 'Impacto de la', 'Análisis del nuevo', 'Regulación de'\n"
                "CORRECTO: 'Impacto ambiental', 'Análisis financiero trimestral', 'Regulación bancaria'\n\n"
                'JSON: {"subtema":"..."}'
            )
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=80,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            u = resp.get('usage', {}) if isinstance(resp, dict) else getattr(resp, 'usage', {})
            if u:
                st.session_state['tokens_input'] += (u.get('prompt_tokens') if isinstance(u, dict) else getattr(u, 'prompt_tokens', 0)) or 0
                st.session_state['tokens_output'] += (u.get('completion_tokens') if isinstance(u, dict) else getattr(u, 'completion_tokens', 0)) or 0
            raw = json.loads(resp.choices[0].message.content).get("subtema", "")
            if raw:
                cleaned = limpiar_tema_geografico(limpiar_tema(raw), marca, aliases or [])
                if _frase_esta_completa(cleaned) and len(cleaned.split()) >= 2:
                    return capitalizar_etiqueta(cleaned)
        except:
            pass

    # Último recurso: fallback
    if fallback_fn:
        return fallback_fn(titulos_grp or [])
    return capitalizar_etiqueta(recortada) if recortada and len(recortada.split()) >= 2 else "Cobertura informativa general"


# ======================================
# Deduplicación de etiquetas
# ======================================
def dedup_labels(etiquetas, umbral=UMBRAL_DEDUP_LABEL):
    unique = list(dict.fromkeys(etiquetas))
    if len(unique)<=1: return etiquetas
    normed = [string_norm_label(u) for u in unique]; n = len(unique)
    parent = list(range(n))
    def find(x):
        while parent[x]!=x: parent[x]=parent[parent[x]]; x=parent[x]
        return x
    def union(a,b):
        ra,rb=find(a),find(b)
        if ra!=rb: parent[rb]=ra
    for i in range(n):
        if not normed[i]: continue
        for j in range(i+1,n):
            if not normed[j] or find(i)==find(j): continue
            if SequenceMatcher(None,normed[i],normed[j]).ratio()>=umbral: union(i,j)
    le = get_embeddings_batch(unique)
    vp = [(i,le[i]) for i in range(n) if le[i] is not None]
    if len(vp)>=2:
        vi,vv = zip(*vp); sm = cosine_similarity(np.array(vv))
        for pi in range(len(vi)):
            for pj in range(pi+1,len(vi)):
                if sm[pi][pj]>=umbral+0.05:
                    if find(vi[pi])!=find(vi[pj]): union(vi[pi],vi[pj])
    freq = Counter(etiquetas)
    grupos = defaultdict(list)
    for i in range(n): grupos[find(i)].append(i)
    canon = {}
    for root, members in grupos.items():
        cands = [unique[m] for m in members]
        # Filtrar candidatos que sean frases completas
        valid_complete = [c for c in cands
                         if c.lower() not in ("sin tema", "varios")
                         and _frase_esta_completa(c)]
        valid_any = [c for c in cands if c.lower() not in ("sin tema", "varios")]

        if valid_complete:
            # Preferir la más frecuente entre las completas
            canon[root] = max(valid_complete, key=lambda c: (freq[c], len(c)))
        elif valid_any:
            # Si ninguna está completa, recortar la mejor candidata
            best = max(valid_any, key=lambda c: (freq[c], len(c)))
            recortada = _recortar_frase_completa(best)
            canon[root] = recortada if _frase_esta_completa(recortada) else best
        else:
            canon[root] = cands[0]
    lm = {unique[i]: canon[find(i)] for i in range(n)}
    return [capitalizar_etiqueta(lm.get(e, e)) for e in etiquetas]

# ======================================
# Embeddings con caché
# ======================================
def get_embeddings_batch(textos, batch_size=100):
    if not textos: return []
    cache = get_embedding_cache(); resultados, missing = cache.get_many(textos)
    if not missing: return resultados
    mt = [textos[i][:2000] if textos[i] else "" for i in missing]
    for i in range(0,len(mt),batch_size):
        batch = mt[i:i+batch_size]; bidx = missing[i:i+batch_size]
        try:
            resp = call_with_retries(openai.Embedding.create,input=batch,model=OPENAI_MODEL_EMBEDDING)
            u = resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u: st.session_state['tokens_embedding']+=(u.get('total_tokens') if isinstance(u,dict) else getattr(u,'total_tokens',0)) or 0
            for j,d in enumerate(resp["data"]):
                oi=bidx[j]; emb=d["embedding"]; resultados[oi]=emb; cache.put(textos[oi],emb)
        except:
            for j,t in enumerate(batch):
                oi=bidx[j]
                try:
                    r=openai.Embedding.create(input=[t],model=OPENAI_MODEL_EMBEDDING)
                    emb=r["data"][0]["embedding"]; resultados[oi]=emb; cache.put(textos[oi],emb)
                except: pass
    return resultados

# ======================================
# DSU
# ======================================
class DSU:
    def __init__(self,n): self.p=list(range(n)); self.rank=[0]*n
    def find(self,i):
        path=[]
        while self.p[i]!=i: path.append(i); i=self.p[i]
        for node in path: self.p[node]=i
        return i
    def union(self,i,j):
        ri,rj=self.find(i),self.find(j)
        if ri==rj: return
        if self.rank[ri]<self.rank[rj]: ri,rj=rj,ri
        self.p[rj]=ri
        if self.rank[ri]==self.rank[rj]: self.rank[ri]+=1
    def grupos(self,n):
        c=defaultdict(list)
        for i in range(n): c[self.find(i)].append(i)
        return dict(c)

# ======================================
# Agrupación
# ======================================
def agrupar_textos_similares(textos,umbral):
    if not textos: return {}
    embs=get_embeddings_batch(textos)
    valid=[(i,e) for i,e in enumerate(embs) if e is not None]
    if len(valid)<2: return {}
    idxs,M=zip(*valid)
    labels=AgglomerativeClustering(n_clusters=None,distance_threshold=1-umbral,metric="cosine",linkage="average").fit(np.array(M)).labels_
    g=defaultdict(list)
    for k,lbl in enumerate(labels): g[lbl].append(idxs[k])
    return dict(enumerate(g.values()))

def agrupar_por_titulo_similar(titulos):
    gid,grupos,used=0,{},set()
    norm=[normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(norm)):
        if i in used or not norm[i]: continue
        grp=[i]; used.add(i)
        for j in range(i+1,len(norm)):
            if j in used or not norm[j]: continue
            if SequenceMatcher(None,norm[i],norm[j]).ratio()>=SIMILARITY_THRESHOLD_TITULOS: grp.append(j); used.add(j)
        if len(grp)>=2: grupos[gid]=grp; gid+=1
    return grupos

def seleccionar_representante(indices,textos):
    embs=get_embeddings_batch([textos[i] for i in indices])
    validos=[(indices[k],e) for k,e in enumerate(embs) if e is not None]
    if not validos: return indices[0],textos[indices[0]]
    idxs,M=zip(*validos)
    centro=np.mean(M,axis=0,keepdims=True)
    best=int(np.argmax(cosine_similarity(np.array(M),centro)))
    return idxs[best],textos[idxs[best]]

# ======================================
# Segmentación
# ======================================
_SENT_SPLIT=re.compile(r'(?<=[.!?;])\s+|(?<=\n)')
def _split_sentences(text):
    parts=_SENT_SPLIT.split(text)
    sents=[p.strip() for p in parts if len(p.strip())>15]
    return sents if sents else [text[:600]]

# ======================================
# CLASIFICADOR DE TONO
# ======================================
class ClasificadorTono:
    def __init__(self,marca,aliases):
        self.marca=marca.strip()
        self.aliases=[a.strip() for a in (aliases or []) if a.strip()]
        self._all_names=[self.marca]+self.aliases
        patterns=[re.escape(unidecode(n.lower())) for n in self._all_names]
        self.brand_re=re.compile(r"\b("+"|".join(patterns)+r")\b",re.IGNORECASE) if patterns else re.compile(r"(a^b)")

    def _extraer_oraciones_marca(self,texto):
        oraciones=_split_sentences(texto); resultado=[]
        for i,sent in enumerate(oraciones):
            if self.brand_re.search(unidecode(sent.lower())):
                ctx=(oraciones[i-1]+" "+sent) if i>0 else sent
                resultado.append(ctx.strip())
        return list(dict.fromkeys(resultado))[:5] if resultado else [texto[:600]]

    def _es_sujeto(self,oracion):
        on=unidecode(oracion.lower()); m=self.brand_re.search(on)
        return m and m.start()<len(on)*0.6

    def _sentimiento_oracion(self,oracion):
        on=unidecode(oracion.lower()); bf=self.brand_re.search(on)
        if not bf: return 0,0
        neg_near=bool(re.search(r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente|tampoco|ni)\b',on[max(0,bf.start()-40):bf.end()+40],re.IGNORECASE))
        if CRISIS_KEYWORDS.search(on) and RESPONSE_VERBS.search(on):
            if self._es_sujeto(oracion): return 3,0
        ph=sum(1 for p in POS_PATTERNS if p.search(on))
        nh=sum(1 for p in NEG_PATTERNS if p.search(on))
        w=1.0 if self._es_sujeto(oracion) else 0.3
        if neg_near: return int(nh*w),int(ph*w)
        return int(ph*w),int(nh*w)

    def _reglas(self,oraciones):
        tp,tn=0,0
        for s in oraciones:
            p,n=self._sentimiento_oracion(s); tp+=p; tn+=n
        if tp>=4 and tp>tn*2.5: return "Positivo"
        if tn>=4 and tn>tp*2.5: return "Negativo"
        return None

    async def _llm(self,oraciones,texto):
        fragmentos="\n".join(f"  → {s[:250]}" for s in oraciones[:4])
        prompt=(
            f"Eres un analista de reputación. Evalúa el sentimiento EXCLUSIVAMENTE hacia "
            f"'{self.marca}' (alias: {', '.join(self.aliases) if self.aliases else 'sin aliases'}).\n\n"
            f"REGLAS CRÍTICAS:\n"
            f"1. Evalúa SOLO cómo queda '{self.marca}', NO otras empresas\n"
            f"2. Si lo negativo es para un COMPETIDOR pero '{self.marca}' queda neutra/positiva → Neutro o Positivo\n"
            f"3. Si '{self.marca}' NO es protagonista → Neutro\n\n"
            f"CRITERIOS:\n• Positivo: logros, premios, crecimiento, innovación, alianzas, RSE\n"
            f"• Negativo: sanciones, fraudes, quejas, pérdidas, escándalos\n"
            f"• Neutro: mención informativa sin carga clara\n\n"
            f"ORACIONES CON '{self.marca}':\n{fragmentos}\n\n"
            f"CONTEXTO:\n{texto[:300]}...\n\n"
            f'Responde SOLO JSON: {{"tono":"Positivo|Negativo|Neutro"}}')
        try:
            resp=await acall_with_retries(openai.ChatCompletion.acreate,model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role":"user","content":prompt}],max_tokens=50,temperature=0.0,response_format={"type":"json_object"})
            u=resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                st.session_state['tokens_input']+=(u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                st.session_state['tokens_output']+=(u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
            tono=str(json.loads(resp.choices[0].message.content).get("tono","Neutro")).strip().title()
            return {"tono":tono if tono in ("Positivo","Negativo","Neutro") else "Neutro"}
        except: return {"tono":"Neutro"}

    async def _clasificar(self,texto,sem):
        async with sem:
            om=self._extraer_oraciones_marca(texto)
            r=self._reglas(om)
            if r: return {"tono":r}
            return await self._llm(om,texto)

    async def procesar_lote_async(self,textos,pbar,resumenes,titulos):
        n=len(textos); txts=textos.tolist()
        pbar.progress(0.05,"Agrupando para tono...")
        txts_emb=[texto_para_embedding(str(titulos.iloc[i]),str(resumenes.iloc[i])) for i in range(n)]
        dsu=DSU(n)
        for g in [agrupar_textos_similares(txts_emb,SIMILARITY_THRESHOLD_TONO),agrupar_por_titulo_similar(titulos.tolist())]:
            for _,idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0],j)
        grupos=dsu.grupos(n)
        reps={cid:seleccionar_representante(idxs,txts)[1] for cid,idxs in grupos.items()}
        sem=asyncio.Semaphore(CONCURRENT_REQUESTS); cids=list(reps.keys())
        tasks=[self._clasificar(reps[c],sem) for c in cids]
        rl=[]
        for i,f in enumerate(asyncio.as_completed(tasks)):
            rl.append(await f); pbar.progress(0.1+0.85*(i+1)/len(tasks),f"Tono {i+1}/{len(tasks)}")
        rpg={cids[i]:r for i,r in enumerate(rl)}
        final=[None]*n
        for cid,idxs in grupos.items():
            r=rpg.get(cid,{"tono":"Neutro"})
            for i in idxs: final[i]=r
        pbar.progress(1.0,"Tono completado"); return final

def analizar_tono_con_pkl(textos,pkl_file):
    try:
        pipeline=joblib.load(pkl_file)
        TM={1:"Positivo","1":"Positivo",0:"Neutro","0":"Neutro",-1:"Negativo","-1":"Negativo"}
        return [{"tono":TM.get(p,str(p).title())} for p in pipeline.predict(textos)]
    except Exception as e: st.error(f"Error pkl: {e}"); return None

# ======================================
# CLASIFICADOR DE SUBTEMAS
# ======================================
class ClasificadorSubtema:
    def __init__(self,marca,aliases):
        self.marca=marca; self.aliases=aliases or []; self._cache={}

    def _paso1(self,titulos,resumenes,dsu):
        def nt(t,n): return ' '.join(re.sub(r'[^a-z0-9\s]','',unidecode(str(t).lower())).split()[:n])
        bt,br=defaultdict(list),defaultdict(list)
        for i,(ti,re_) in enumerate(zip(titulos,resumenes)):
            a,b=nt(ti,40),nt(re_,15)
            if a: bt[hashlib.md5(a.encode()).hexdigest()].append(i)
            if b: br[hashlib.md5(b.encode()).hexdigest()].append(i)
        for bk in (bt,br):
            for idxs in bk.values():
                for j in idxs[1:]: dsu.union(idxs[0],j)

    def _paso2(self,titulos,dsu):
        norm=[normalize_title_for_comparison(t) for t in titulos]; n=len(norm)
        for i in range(n):
            if not norm[i]: continue
            for j in range(i+1,n):
                if not norm[j] or dsu.find(i)==dsu.find(j): continue
                if SequenceMatcher(None,norm[i],norm[j]).ratio()>=SIMILARITY_THRESHOLD_TITULOS: dsu.union(i,j)

    def _paso3(self,et,ae,dsu,pbar,ps):
        n=len(et)
        if n<2: return
        B=500
        if n<=B:
            pbar.progress(ps,"Clustering semántico...")
            ok=[(k,e) for k,e in enumerate(ae) if e is not None]
            if len(ok)<2: return
            io_,M=zip(*ok); sim=cosine_similarity(np.array(M))
            labels=AgglomerativeClustering(n_clusters=None,distance_threshold=1-UMBRAL_SUBTEMA,metric='precomputed',linkage='average').fit(1-sim).labels_
            g=defaultdict(list)
            for k,lbl in enumerate(labels): g[lbl].append(io_[k])
            for cl in g.values():
                if len(cl)>=2:
                    for j in cl[1:]: dsu.union(cl[0],j)
            pbar.progress(ps+0.18,"Clustering completado"); return
        tb=max(1,(n+B-1)//B)
        for bn,bs in enumerate(range(0,n,B)):
            bi=list(range(bs,min(bs+B,n)))
            ok=[(idx,ae[idx]) for idx in bi if ae[idx] is not None]
            if len(ok)<2: continue
            io_,M=zip(*ok); sim=cosine_similarity(np.array(M))
            labels=AgglomerativeClustering(n_clusters=None,distance_threshold=1-UMBRAL_SUBTEMA,metric='precomputed',linkage='average').fit(1-sim).labels_
            g=defaultdict(list)
            for k,lbl in enumerate(labels): g[lbl].append(io_[k])
            for cl in g.values():
                if len(cl)>=2:
                    for j in cl[1:]: dsu.union(cl[0],j)
            pbar.progress(ps+0.15*(bn+1)/tb,f"Clustering {bn+1}/{tb}...")
        pbar.progress(ps+0.16,"Unificando...")
        self._fusion(et,ae,dsu,pbar,ps+0.16)

    def _fusion(self,textos,ae,dsu,pbar,ps):
        n=len(textos)
        for it in range(MAX_ITER_FUSION):
            grupos=dsu.grupos(n)
            if len(grupos)<2: break
            centroids,vg=[],[]
            for gid,idxs in grupos.items():
                vecs=[ae[i] for i in idxs[:50] if ae[i] is not None]
                if vecs: centroids.append(np.mean(vecs,axis=0)); vg.append(gid)
            if len(vg)<2: break
            sim=cosine_similarity(np.array(centroids))
            pairs=sorted([(sim[i][j],i,j) for i in range(len(vg)) for j in range(i+1,len(vg)) if sim[i][j]>=UMBRAL_FUSION_INTERGRUPO],reverse=True)
            fus=0
            for _,i,j in pairs:
                ri,rj=grupos[vg[i]][0],grupos[vg[j]][0]
                if dsu.find(ri)!=dsu.find(rj): dsu.union(ri,rj); fus+=1
            pbar.progress(min(ps+0.04*(it+1),0.52),f"Fusión {it+1}: {fus}")
            if fus==0: break

    def _generar_etiqueta(self,textos_grp,titulos_grp,resumenes_grp):
        tn=sorted(set(normalize_title_for_comparison(t) for t in titulos_grp if t))
        ck=hashlib.md5("|".join(tn[:12]).encode()).hexdigest()
        if ck in self._cache: return self._cache[ck]
        palabras=[]
        for t in titulos_grp[:8]:
            for w in string_norm_label(t).split():
                if len(w)>3: palabras.append(w)
        kw=" · ".join(w for w,_ in Counter(palabras).most_common(8))
        tm=list(dict.fromkeys(t[:120] for t in titulos_grp if t))[:6]
        rm=[str(r)[:200] for r in resumenes_grp[:3] if r and len(str(r))>20]
        ctx=("\n\nRESÚMENES:\n"+"\n".join(f"  · {r}" for r in rm)) if rm else ""
        prompt=(
            "Genera un SUBTEMA periodístico en español (3-6 palabras) que describa "
            "el asunto central de estas noticias.\n\n"
            f"TÍTULOS:\n"+"\n".join(f"  · {t}" for t in tm)+ctx+"\n\n"
            f"PALABRAS CLAVE: {kw}\n\n"
            "REGLAS CRÍTICAS:\n"
            "1. NO uses nombres de empresas, marcas, personas ni ciudades\n"
            "2. Debe ser una FRASE COHERENTE en español con preposiciones y artículos donde corresponda\n"
            "3. NO concatenes palabras sueltas (INCORRECTO: 'Seguimiento derechos laborales fusiones')\n"
            "4. Usa tildes correctas (ej: 'Regulación', NO 'Regulacion')\n"
            "5. Todo en minúsculas excepto la primera letra\n"
            "6. Describe el ASUNTO ESPECÍFICO, no el actor\n"
            "7. NO uses genéricas como 'Gestión', 'Noticias', 'Eventos'\n"
            "8. La frase DEBE terminar en un SUSTANTIVO o ADJETIVO, NUNCA en artículo, preposición o determinante\n"
            "   INCORRECTO: 'Impacto de la', 'Análisis del', 'Regulación de'\n"
            "   CORRECTO: 'Impacto ambiental', 'Análisis financiero', 'Regulación bancaria'\n\n"
            "CORRECTOS: 'Resultados financieros trimestrales', 'Expansión de sucursales'\n"
            "INCORRECTOS: 'Seguimiento derechos laborales fusiones', 'Impacto de una', 'Análisis del nuevo'\n\n"
            'JSON: {"subtema":"..."}')
        try:
            resp=call_with_retries(openai.ChatCompletion.create,model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role":"user","content":prompt}],max_tokens=80,temperature=0.0,response_format={"type":"json_object"})
            u=resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                st.session_state['tokens_input']+=(u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                st.session_state['tokens_output']+=(u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
            raw=json.loads(resp.choices[0].message.content).get("subtema","Varios")
            et=limpiar_tema_geografico(limpiar_tema(raw),self.marca,self.aliases)
            genericas={"gestión","gestion","actividades","acciones","noticias","información","informacion","eventos","varios","sin tema","actividad corporativa","noticias corporativas","gestión empresarial","cobertura informativa","gestión integral"}
            if string_norm_label(et) in {string_norm_label(g) for g in genericas} or len(et.split())<2:
                et=self._refinar(tm,kw,rm)
            # Validar completitud
            et = _validar_etiqueta_completa(
                et,
                titulos_grp=titulos_grp,
                resumenes_grp=resumenes_grp,
                marca=self.marca,
                aliases=self.aliases,
                fallback_fn=self._fallback
            )
        except: et=self._fallback(titulos_grp)
        et=capitalizar_etiqueta(et); self._cache[ck]=et; return et

    def _refinar(self,titulos,kw,resumenes=None):
        ctx=f"\nContexto: {' | '.join(r[:100] for r in resumenes[:2])}" if resumenes else ""
        prompt=(
            "Estos títulos comparten un tema. Genera una frase temática COHERENTE en español de 3-5 palabras.\n\n"
            f"Títulos: {' | '.join(titulos[:4])}\nKeywords: {kw}{ctx}\n\n"
            "REGLAS:\n"
            "- Debe ser frase natural con preposiciones donde corresponda\n"
            "- Tildes correctas. Primera letra mayúscula\n"
            "- DEBE terminar en SUSTANTIVO o ADJETIVO, NUNCA en preposición/artículo\n"
            "  INCORRECTO: 'Impacto de la', 'Regulación del'\n"
            "  CORRECTO: 'Impacto regulatorio', 'Regulación sectorial'\n\n"
            'JSON: {"subtema":"..."}')
        try:
            resp=call_with_retries(openai.ChatCompletion.create,model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role":"user","content":prompt}],max_tokens=80,temperature=0.1,response_format={"type":"json_object"})
            u=resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                st.session_state['tokens_input']+=(u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                st.session_state['tokens_output']+=(u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
            raw = json.loads(resp.choices[0].message.content).get("subtema","Varios")
            et = limpiar_tema_geografico(limpiar_tema(raw),self.marca,self.aliases)
            # Validar completitud del resultado refinado
            if not _frase_esta_completa(et):
                et = _recortar_frase_completa(et)
                if not _frase_esta_completa(et):
                    return self._fallback(titulos)
            return et
        except: return self._fallback([])

    def _fallback(self,titulos):
        if not titulos: return "Cobertura informativa general"
        palabras=[]
        for t in titulos[:5]:
            for w in string_norm_label(t).split():
                if len(w)>4: palabras.append(w)
        if palabras:
            top=[w for w,_ in Counter(palabras).most_common(3)]
            if len(top)>=2:
                # Construir frase y verificar completitud
                frase = f"{top[0]} de {top[1]}"
                if _frase_esta_completa(frase):
                    return capitalizar_etiqueta(frase)
                # Si "de X" no funciona, concatenar como adjetivo
                return capitalizar_etiqueta(f"{top[0]} {top[1]}")
            return capitalizar_etiqueta(top[0])
        return "Cobertura informativa general"

    def procesar_lote(self,col,pbar,res_puros,tit_puros):
        textos=col.tolist(); titulos=tit_puros.tolist(); resumenes=res_puros.tolist(); n=len(textos)
        et=[texto_para_embedding(titulos[i],resumenes[i]) for i in range(n)]
        pbar.progress(0.05,"Fase 1 · Agrupando idénticas...")
        dsu=DSU(n); self._paso1(titulos,resumenes,dsu)
        pbar.progress(0.12,"Fase 2 · Similitud títulos...")
        self._paso2(titulos,dsu)
        pbar.progress(0.18,"Calculando embeddings...")
        ae=get_embeddings_batch(et)
        pbar.progress(0.20,"Fase 3 · Clustering...")
        self._paso3(et,ae,dsu,pbar,0.20)
        gf=dsu.grupos(n); ng=len(gf)
        pbar.progress(0.55,f"Fase 4 · Etiquetando {ng} grupos...")
        mapa={}; sg=sorted(gf.items(),key=lambda x:-len(x[1]))
        for k,(lid,idxs) in enumerate(sg):
            if k%10==0: pbar.progress(0.55+0.30*(k/max(ng,1)),f"Etiquetando {k+1}/{ng}...")
            e=self._generar_etiqueta([textos[i] for i in idxs],[titulos[i] for i in idxs],[resumenes[i] for i in idxs])
            for i in idxs: mapa[i]=e
        subtemas=[mapa.get(i,"Varios") for i in range(n)]
        pbar.progress(0.88,"Fase 5 · Deduplicando...")
        subtemas=dedup_labels(subtemas,UMBRAL_DEDUP_LABEL)
        pbar.progress(0.93,"Fase 6 · Consistencia...")
        subtemas=self._consistencia(subtemas,ae,pbar)
        # Fase 7: Validación final de completitud
        pbar.progress(0.96,"Fase 7 · Validando completitud...")
        subtemas = self._validar_completitud_final(subtemas, textos, titulos, resumenes)
        subtemas=[capitalizar_etiqueta(s) for s in subtemas]
        nf=len(set(subtemas)); pbar.progress(1.0,f"{nf} subtemas")
        st.info(f"Subtemas: **{nf}** · Grupos: **{ng}**"); return subtemas

    def _validar_completitud_final(self, subtemas, textos, titulos, resumenes):
        """Pase final: detecta y repara etiquetas que terminan en preposición/artículo."""
        # Agrupar por subtema para tener contexto
        por_subtema = defaultdict(list)
        for i, s in enumerate(subtemas):
            por_subtema[s].append(i)

        resultado = list(subtemas)
        for sub, idxs in por_subtema.items():
            if _frase_esta_completa(sub):
                continue
            # Intentar recortar
            recortada = _recortar_frase_completa(sub)
            if _frase_esta_completa(recortada) and len(recortada.split()) >= 2:
                for i in idxs:
                    resultado[i] = capitalizar_etiqueta(recortada)
                continue
            # Regenerar con contexto
            tit_grp = [titulos[i] for i in idxs[:6]]
            res_grp = [resumenes[i] for i in idxs[:3]]
            nueva = _validar_etiqueta_completa(
                sub,
                titulos_grp=tit_grp,
                resumenes_grp=res_grp,
                marca=self.marca,
                aliases=self.aliases,
                fallback_fn=self._fallback
            )
            for i in idxs:
                resultado[i] = capitalizar_etiqueta(nueva)
        return resultado

    def _consistencia(self,subtemas,ae,pbar):
        ps=defaultdict(list)
        for i,s in enumerate(subtemas): ps[s].append(i)
        r=list(subtemas); centroids={}
        for sub,idxs in ps.items():
            vecs=[ae[i] for i in idxs if ae[i] is not None]
            if vecs: centroids[sub]=np.mean(vecs,axis=0)
        for sub in [s for s in centroids if len(ps[s])>=3]:
            idxs=ps[sub]
            if sub.lower() in ("sin tema","varios") or len(idxs)<3: continue
            vi=[(i,ae[i]) for i in idxs if ae[i] is not None]
            if len(vi)<3: continue
            v_i,v_v=zip(*vi); M=np.array(v_v)
            sims=cosine_similarity(M,centroids[sub].reshape(1,-1)).flatten()
            thr=max(0.60,np.mean(sims)-2*np.std(sims))
            for k,(oi,sv) in enumerate(zip(v_i,sims)):
                if sv<thr:
                    bs,bsim=sub,sv; emb=ae[oi]
                    for os,oc in centroids.items():
                        if os==sub: continue
                        s=cosine_similarity(np.array(emb).reshape(1,-1),oc.reshape(1,-1))[0][0]
                        if s>bsim and s>0.75: bsim=s; bs=os
                    if bs!=sub: r[oi]=bs
        return r

# ======================================
# TEMAS
# ======================================
def consolidar_temas(subtemas,textos,pbar):
    pbar.progress(0.05,"Centroides...")
    df=pd.DataFrame({'subtema':subtemas,'texto':textos}); us=list(df['subtema'].unique())
    if len(us)<=1: pbar.progress(1.0,"Un tema"); return [capitalizar_etiqueta(s) for s in subtemas]
    ae=get_embeddings_batch(textos); centroids={}
    for sub in us:
        idxs=df.index[df['subtema']==sub].tolist()[:40]
        vecs=[ae[i] for i in idxs if ae[i] is not None]
        if vecs: centroids[sub]=np.mean(vecs,axis=0)
    ler=get_embeddings_batch(us); le={s:e for s,e in zip(us,ler) if e is not None}
    vs=[s for s in us if s in centroids]
    if len(vs)<2: pbar.progress(1.0,"Sin agrupación"); return [capitalizar_etiqueta(s) for s in subtemas]
    pbar.progress(0.40,"Clustering temas...")
    Mc=np.array([centroids[s] for s in vs]); sc=cosine_similarity(Mc)
    sim=0.6*sc+0.4*cosine_similarity(np.array([le[s] for s in vs])) if all(s in le for s in vs) else sc
    cl=AgglomerativeClustering(n_clusters=None,distance_threshold=1-UMBRAL_TEMA,metric='precomputed',linkage='average').fit(1-sim)
    if len(set(cl.labels_))>NUM_TEMAS_MAX:
        cl=AgglomerativeClustering(n_clusters=NUM_TEMAS_MAX,metric='precomputed',linkage='average').fit(1-sim)
    cs=defaultdict(list)
    for i,lbl in enumerate(cl.labels_): cs[lbl].append(vs[i])
    uc=[s for s in us if s not in vs]; mt={}; tc=len(cs)
    for k,(cid,ls) in enumerate(cs.items()):
        pbar.progress(0.50+0.40*(k/max(tc,1)),f"Tema {k+1}/{tc}...")
        if len(ls)==1:
            nombre=ls[0]; p=nombre.split()
            if len(p)>3:
                nombre = _recortar_frase_completa(" ".join(p), max_palabras=4)
        else:
            prompt=(
                "Genera UNA categoría temática en español (2-4 palabras), frase coherente con preposiciones.\n\n"
                f"SUBTEMAS:\n"+"\n".join(f"  · {s}" for s in ls[:10])+"\n\n"
                "REGLAS:\n"
                "- Tildes correctas. Primera letra mayúscula, resto minúsculas\n"
                "- Sin marcas ni ciudades\n"
                "- DEBE terminar en SUSTANTIVO o ADJETIVO\n"
                "- NUNCA terminar en: de, del, la, el, los, las, un, una, al, en, con, por, para, su, sus\n"
                "  INCORRECTO: 'Impacto de la', 'Regulación del'\n"
                "  CORRECTO: 'Impacto regulatorio', 'Regulación sectorial'\n\n"
                "Responde SOLO el nombre del tema")
            try:
                resp=call_with_retries(openai.ChatCompletion.create,model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role":"user","content":prompt}],max_tokens=60,temperature=0.0)
                u=resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
                if u:
                    st.session_state['tokens_input']+=(u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                    st.session_state['tokens_output']+=(u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
                nombre=limpiar_tema(resp.choices[0].message.content.strip().replace('"','').replace('.',''))
            except: nombre=ls[0]
        # Validar completitud del nombre del tema
        if not _frase_esta_completa(nombre):
            nombre = _recortar_frase_completa(nombre, max_palabras=4)
            if not _frase_esta_completa(nombre):
                # Usar el subtema más frecuente del grupo como fallback
                freq = Counter(subtemas)
                nombre = max(ls, key=lambda s: freq.get(s, 0))
                nombre = _recortar_frase_completa(nombre, max_palabras=4)
        for sub in ls: mt[sub]=nombre
    for sub in uc: mt[sub]=sub
    tf=[mt.get(sub,sub) for sub in subtemas]
    pbar.progress(0.92,"Deduplicando temas...")
    tf=dedup_labels(tf,UMBRAL_DEDUP_LABEL)
    # Validación final de completitud en temas
    pbar.progress(0.95,"Validando completitud de temas...")
    tf_validados = []
    for t in tf:
        if _frase_esta_completa(t):
            tf_validados.append(capitalizar_etiqueta(t))
        else:
            recortado = _recortar_frase_completa(t)
            tf_validados.append(capitalizar_etiqueta(recortado) if _frase_esta_completa(recortado) else capitalizar_etiqueta(t))
    tf = tf_validados
    st.info(f"Temas: **{len(set(tf))}** (máx: {NUM_TEMAS_MAX})")
    pbar.progress(1.0,"Temas listos"); return tf

def analizar_temas_con_pkl(textos,pkl_file):
    try:
        pipeline=joblib.load(pkl_file)
        return [capitalizar_etiqueta(str(p)) for p in pipeline.predict(textos)]
    except Exception as e: st.error(f"Error pkl: {e}"); return None

# ======================================
# Duplicados y Excel
# ======================================
def detectar_duplicados_avanzado(rows,km):
    processed=deepcopy(rows); seen_url,seen_bcast={},{}; tb=defaultdict(list)
    for i,row in enumerate(processed):
        if row.get("is_duplicate"): continue
        tipo=normalizar_tipo_medio(str(row.get(km.get("tipodemedio",""))))
        mencion=norm_key(row.get(km.get("menciones",""))); medio=norm_key(row.get(km.get("medio","")))
        if tipo=="Internet":
            li=row.get(km.get("link_nota",{})) or {}; url=li.get("url") if isinstance(li,dict) else None
            if url and mencion:
                k=(url,mencion)
                if k in seen_url: row["is_duplicate"]=True; row["idduplicada"]=processed[seen_url[k]].get(km.get("idnoticia",""),""); continue
                seen_url[k]=i
            if medio and mencion: tb[(medio,mencion)].append(i)
        elif tipo in ("Radio","Televisión"):
            hora=str(row.get(km.get("hora",""),"")).strip()
            if mencion and medio and hora:
                k=(mencion,medio,hora)
                if k in seen_bcast: row["is_duplicate"]=True; row["idduplicada"]=processed[seen_bcast[k]].get(km.get("idnoticia",""),"")
                else: seen_bcast[k]=i
    for idxs in tb.values():
        if len(idxs)<2: continue
        for i in range(len(idxs)):
            for j in range(i+1,len(idxs)):
                a,b=idxs[i],idxs[j]
                if processed[a].get("is_duplicate") or processed[b].get("is_duplicate"): continue
                ta=normalize_title_for_comparison(processed[a].get(km.get("titulo","")))
                tb_=normalize_title_for_comparison(processed[b].get(km.get("titulo","")))
                if ta and tb_ and SequenceMatcher(None,ta,tb_).ratio()>=SIMILARITY_THRESHOLD_TITULOS:
                    if len(ta)<len(tb_): processed[a]["is_duplicate"]=True; processed[a]["idduplicada"]=processed[b].get(km.get("idnoticia",""),"")
                    else: processed[b]["is_duplicate"]=True; processed[b]["idduplicada"]=processed[a].get(km.get("idnoticia",""),"")
    return processed

def run_dossier_logic(sheet):
    headers=[c.value for c in sheet[1] if c.value]; nk=[norm_key(h) for h in headers]
    km={n:n for n in nk}
    km.update({"titulo":norm_key("Titulo"),"resumen":norm_key("Resumen - Aclaracion"),"menciones":norm_key("Menciones - Empresa"),"medio":norm_key("Medio"),"tonoiai":norm_key("Tono IA"),"tema":norm_key("Tema"),"subtema":norm_key("Subtema"),"idnoticia":norm_key("ID Noticia"),"idduplicada":norm_key("ID duplicada"),"tipodemedio":norm_key("Tipo de Medio"),"hora":norm_key("Hora"),"link_nota":norm_key("Link Nota"),"link_streaming":norm_key("Link (Streaming - Imagen)"),"region":norm_key("Region")})
    rows,split_rows=[],[]
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({nk[i]:c for i,c in enumerate(row) if i<len(nk)})
    for rc in rows:
        base={k:(extract_link(v) if k in (km["link_nota"],km["link_streaming"]) else v.value) for k,v in rc.items()}
        if km.get("tipodemedio") in base: base[km["tipodemedio"]]=normalizar_tipo_medio(base.get(km["tipodemedio"]))
        ml=[m.strip() for m in str(base.get(km["menciones"],"")).split(";") if m.strip()]
        for m in ml or [None]:
            nr=deepcopy(base)
            if m: nr[km["menciones"]]=m
            split_rows.append(nr)
    for idx,row in enumerate(split_rows): row.update({"original_index":idx,"is_duplicate":False})
    processed=detectar_duplicados_avanzado(split_rows,km)

    for row in processed:
        if row["is_duplicate"]: 
        row.update({km["tonoiai"]:"Duplicada",km["tema"]:"-",km["subtema"]:"-"})
    return processed,km

def fix_links_by_media_type(row,km):
    tkey=km.get("tipodemedio"); ln=km.get("link_nota"); ls=km.get("link_streaming")
    if not(tkey and ln and ls): return
    tipo=row.get(tkey,""); rl=row.get(ln) or {"value":"","url":None}; rs=row.get(ls) or {"value":"","url":None}
    hurl=lambda x:isinstance(x,dict) and bool(x.get("url"))
    if tipo in ("Radio","Televisión"): row[ls]={"value":"","url":None}
    elif tipo=="Internet": row[ln],row[ls]=rs,rl
    elif tipo in ("Prensa","Revista"):
        if not hurl(rl) and hurl(rs): row[ln]=rs
        row[ls]={"value":"","url":None}

def generate_output_excel(rows,km):
    wb=Workbook(); ws=wb.active; ws.title="Resultado"
    ORDER=["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Region","Seccion - Programa","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Audiencia","Tier","Tono","Tono IA","Tema","Subtema","Link Nota","Resumen - Aclaracion","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    NUM={"ID Noticia","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia"}
    ws.append(ORDER)
    ls=NamedStyle(name="HL",font=Font(color="0000FF",underline="single"))
    if "HL" not in wb.style_names: wb.add_named_style(ls)
    for row in rows:
        tk=km.get("titulo")
        if tk and tk in row: row[tk]=clean_title_for_output(row.get(tk))
        rk=km.get("resumen")
        if rk and rk in row: row[rk]=corregir_texto(row.get(rk))
        out,links=[],{}
        for ci,h in enumerate(ORDER,1):
            dk=km.get(norm_key(h),norm_key(h)); val=row.get(dk); cv=None
            if h in NUM:
                try: cv=float(val) if val is not None and str(val).strip()!="" else None
                except: cv=str(val) if val is not None else None
            elif isinstance(val,dict) and "url" in val:
                cv=val.get("value","Link")
                if val.get("url"): links[ci]=val["url"]
            elif val is not None: cv=str(val)
            out.append(cv)
        ws.append(out)
        for ci,url in links.items():
            cell=ws.cell(row=ws.max_row,column=ci); cell.hyperlink=url; cell.style="HL"
    buf=io.BytesIO(); wb.save(buf); return buf.getvalue()

# ======================================
# Proceso principal
# ======================================
async def run_full_process_async(df_file,reg_file,int_file,bn,ba,tpkl,epkl,mode):
    st.session_state.update({'tokens_input':0,'tokens_output':0,'tokens_embedding':0})
    get_embedding_cache().clear(); t0=time.time()
    if "API" in mode:
        try: openai.api_key=st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
        except: st.error("OPENAI_API_KEY no encontrado."); st.stop()
    with st.status("Paso 1 · Limpieza y duplicados",expanded=True) as s:
        rows,km=run_dossier_logic(load_workbook(df_file,data_only=True).active)
        s.update(label="✓ Paso 1 completado",state="complete")
    with st.status("Paso 2 · Mapeos",expanded=True) as s:
        dfr=pd.read_excel(reg_file)
        rmap={str(k).lower().strip():v for k,v in pd.Series(dfr.iloc[:,1].values,index=dfr.iloc[:,0]).to_dict().items()}
        dfi=pd.read_excel(int_file)
        imap={str(k).lower().strip():v for k,v in pd.Series(dfi.iloc[:,1].values,index=dfi.iloc[:,0]).to_dict().items()}
        for row in rows:
            mk=str(row.get(km.get("medio",""),"")).lower().strip()
            row[km.get("region")]=rmap.get(mk,"N/A")
            if mk in imap: row[km.get("medio")]=imap[mk]; row[km.get("tipodemedio")]="Internet"
            fix_links_by_media_type(row,km)
        s.update(label="✓ Paso 2 completado",state="complete")
    gc.collect(); ta=[r for r in rows if not r.get("is_duplicate")]
    if ta:
        df=pd.DataFrame(ta)
        df["_txt"]=df.apply(lambda r:texto_para_embedding(str(r.get(km["titulo"],"")),str(r.get(km["resumen"],""))),axis=1)
        with st.status("Calculando embeddings...",expanded=True) as s:
            _=get_embeddings_batch(df["_txt"].tolist())
            s.update(label=f"✓ Embeddings · {get_embedding_cache().stats()}",state="complete")
        with st.status("Paso 3 · Análisis de tono",expanded=True) as s:
            pb=st.progress(0)
            if "PKL" in mode and tpkl:
                res=analizar_tono_con_pkl(df["_txt"].tolist(),tpkl)
                if res is None: st.stop()
            elif "API" in mode:
                res=await ClasificadorTono(bn,ba).procesar_lote_async(df["_txt"],pb,df[km["resumen"]],df[km["titulo"]])
            else: res=[{"tono":"N/A"}]*len(ta)
            df[km["tonoiai"]]=[r["tono"] for r in res]
            s.update(label="✓ Paso 3 · Tono completado",state="complete")
        with st.status("Paso 4 · Clasificación temática",expanded=True) as s:
            pb=st.progress(0)
            if "Solo Modelos PKL" in mode: subtemas=["N/A"]*len(ta); temas=["N/A"]*len(ta)
            else:
                subtemas=ClasificadorSubtema(bn,ba).procesar_lote(df["_txt"],pb,df[km["resumen"]],df[km["titulo"]])
                temas=consolidar_temas(subtemas,df["_txt"].tolist(),pb)
            df[km["subtema"]]=subtemas
            if "PKL" in mode and epkl:
                tp=analizar_temas_con_pkl(df["_txt"].tolist(),epkl)
                if tp: df[km["tema"]]=tp
            else: df[km["tema"]]=temas
            s.update(label="✓ Paso 4 · Clasificación completada",state="complete")
        rm2=df.set_index("original_index").to_dict("index")
        for row in rows:
            if not row.get("is_duplicate"): row.update(rm2.get(row["original_index"],{}))
    gc.collect()
    ci=(st.session_state['tokens_input']/1e6)*PRICE_INPUT_1M
    co=(st.session_state['tokens_output']/1e6)*PRICE_OUTPUT_1M
    ce=(st.session_state['tokens_embedding']/1e6)*PRICE_EMBEDDING_1M
    with st.status("Paso 5 · Generando informe",expanded=True) as s:
        st.session_state["output_data"]=generate_output_excel(rows,km)
        st.session_state["output_filename"]=f"Informe_IA_{bn.replace(' ','_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"]=True
        st.session_state.update({"brand_name":bn,"brand_aliases":ba,"total_rows":len(rows),"unique_rows":len(ta),"duplicates":len(rows)-len(ta),"process_duration":f"{time.time()-t0:.0f}s","process_cost":f"${ci+co+ce:.4f} USD","cache_stats":get_embedding_cache().stats()})
        s.update(label=f"✓ Completado · {get_embedding_cache().stats()}",state="complete")

# ======================================
# Análisis Rápido
# ======================================
async def run_quick_async(df,tc,sc,bn,al):
    st.session_state.update({'tokens_input':0,'tokens_output':0,'tokens_embedding':0})
    get_embedding_cache().clear()
    df['_txt']=df.apply(lambda r:texto_para_embedding(str(r.get(tc,"")),str(r.get(sc,""))),axis=1)
    with st.status("Embeddings...",expanded=True) as s:
        _=get_embeddings_batch(df['_txt'].tolist())
        s.update(label=f"✓ {get_embedding_cache().stats()}",state="complete")
    with st.status("Paso 1/2 · Tono",expanded=True) as s:
        pb=st.progress(0)
        res=await ClasificadorTono(bn,al).procesar_lote_async(df["_txt"],pb,df[sc].fillna(''),df[tc].fillna(''))
        df['Tono IA']=[r["tono"] for r in res]
        s.update(label="✓ Tono completado",state="complete")
    with st.status("Paso 2/2 · Clasificación",expanded=True) as s:
        pb=st.progress(0)
        subtemas=ClasificadorSubtema(bn,al).procesar_lote(df["_txt"],pb,df[sc].fillna(''),df[tc].fillna(''))
        df['Subtema']=subtemas
        temas=consolidar_temas(subtemas,df["_txt"].tolist(),pb)
        df['Tema']=temas
        s.update(label="✓ Clasificación completada",state="complete")
    df.drop(columns=['_txt'],inplace=True)
    ci=(st.session_state['tokens_input']/1e6)*PRICE_INPUT_1M
    co=(st.session_state['tokens_output']/1e6)*PRICE_OUTPUT_1M
    ce=(st.session_state['tokens_embedding']/1e6)*PRICE_EMBEDDING_1M
    st.session_state['quick_cost']=f"${ci+co+ce:.4f} USD"; return df

def gen_quick_excel(df):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine='openpyxl') as w: df.to_excel(w,index=False,sheet_name='Analisis')
    return buf.getvalue()

def render_quick_tab():
    st.markdown('<div class="sec-label">Análisis rápido</div>',unsafe_allow_html=True)
    if 'quick_result' in st.session_state:
        st.markdown('<div class="success-banner"><div class="success-icon">✓</div><div class="success-content"><div class="success-title">Análisis completado</div><div class="success-sub">Resultados listos para descargar</div></div></div>',unsafe_allow_html=True)
        st.metric("Costo estimado",st.session_state.get('quick_cost',"$0.00"))
        st.dataframe(st.session_state.quick_result.head(10),use_container_width=True)
        st.download_button("Descargar resultados",data=gen_quick_excel(st.session_state.quick_result),file_name="Analisis_Rapido_IA.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True,type="primary")
        if st.button("Nuevo análisis"):
            for k in ('quick_result','quick_df','quick_name','quick_cost'):
                if k in st.session_state: del st.session_state[k]
            st.rerun()
        return
    if 'quick_df' not in st.session_state:
        st.markdown("Sube un archivo Excel con columnas de título y resumen para analizar.")
        f=st.file_uploader("Archivo Excel",type=["xlsx"],label_visibility="collapsed",key="qu")
        if f:
            try: st.session_state.quick_df=pd.read_excel(f); st.session_state.quick_name=f.name; st.rerun()
            except Exception as e: st.error(f"Error: {e}")
    else:
        st.success(f"Archivo **{st.session_state.quick_name}** cargado correctamente")
        with st.form("qf"):
            cols=st.session_state.quick_df.columns.tolist()
            c1,c2=st.columns(2)
            tc=c1.selectbox("Columna de título",cols,0)
            sc=c2.selectbox("Columna de resumen",cols,1 if len(cols)>1 else 0)
            st.write("---")
            bn=st.text_input("Marca principal",placeholder="Ej: Bancolombia")
            bat=st.text_area("Alias (separados por ;)",placeholder="Ej: Grupo Bancolombia;Ban",height=80)
            if st.form_submit_button("Iniciar análisis",use_container_width=True,type="primary"):
                if not bn: st.error("Indica la marca.")
                else:
                    try: openai.api_key=st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
                    except: st.error("OPENAI_API_KEY no encontrada."); st.stop()
                    al=[a.strip() for a in bat.split(";") if a.strip()]
                    with st.spinner("Procesando..."): st.session_state.quick_result=asyncio.run(run_quick_async(st.session_state.quick_df.copy(),tc,sc,bn,al))
                    st.rerun()
        if st.button("Cargar otro archivo"):
            for k in ('quick_df','quick_name','quick_result','quick_cost'):
                if k in st.session_state: del st.session_state[k]
            st.rerun()

# ======================================
# Main
# ======================================
def main():
    load_custom_css()
    if not check_password(): return
    st.markdown("""
    <div class="app-header">
        <div class="app-header-icon">◈</div>
        <div class="app-header-text">
            <div class="app-header-title">Sistema de Análisis de Noticias</div>
            <div class="app-header-version">v15.1 · fix frases incompletas · validación de completitud</div>
        </div>
        <div class="app-header-badge">IA Powered</div>
    </div>""",unsafe_allow_html=True)

    tab1,tab2=st.tabs(["Análisis Completo","Análisis Rápido"])

    with tab1:
        if not st.session_state.get("processing_complete",False):
            with st.form("main_form"):
                st.markdown('<div class="sec-label">Archivos de entrada</div>',unsafe_allow_html=True)
                st.markdown("""
                <div class="upload-zone">
                    <div class="upload-zone-card">
                        <div class="upload-zone-icon uz-dossier">📋</div>
                        <div class="upload-zone-title">Dossier</div>
                        <div class="upload-zone-desc">Archivo principal con todas las noticias a analizar</div>
                        <div class="upload-zone-hint">Arrastra o selecciona · .xlsx</div>
                    </div>
                    <div class="upload-zone-card">
                        <div class="upload-zone-icon uz-region">🗺️</div>
                        <div class="upload-zone-title">Región</div>
                        <div class="upload-zone-desc">Mapeo de medios de comunicación a regiones geográficas</div>
                        <div class="upload-zone-hint">Arrastra o selecciona · .xlsx</div>
                    </div>
                    <div class="upload-zone-card">
                        <div class="upload-zone-icon uz-internet">🌐</div>
                        <div class="upload-zone-title">Internet</div>
                        <div class="upload-zone-desc">Mapeo de nombres de medios digitales y portales web</div>
                        <div class="upload-zone-hint">Arrastra o selecciona · .xlsx</div>
                    </div>
                </div>""",unsafe_allow_html=True)
                c1,c2,c3=st.columns(3)
                f1=c1.file_uploader("Dossier",type=["xlsx"],label_visibility="collapsed",key="f1")
                f2=c2.file_uploader("Región",type=["xlsx"],label_visibility="collapsed",key="f2")
                f3=c3.file_uploader("Internet",type=["xlsx"],label_visibility="collapsed",key="f3")

                st.markdown('<div class="sec-label">Marca a analizar</div>',unsafe_allow_html=True)
                bn=st.text_input("Nombre principal de la marca",placeholder="Ej: Bancolombia, Tigo, Claro",key="bn")
                bat=st.text_area("Alias de la marca (separados por ;)",placeholder="Ej: Grupo Bancolombia;Ban;Bancolombia S.A.",height=80,key="ba")

                st.markdown('<div class="sec-label">Modo de análisis</div>',unsafe_allow_html=True)
                mode=st.radio("Selecciona el modo",["Híbrido (PKL + API)","Solo Modelos PKL","API de OpenAI"],index=0,key="mode")
                tpkl,epkl=None,None
                if "PKL" in mode:
                    p1,p2=st.columns(2)
                    tpkl=p1.file_uploader("Pipeline de sentimiento (.pkl)",type=["pkl"])
                    epkl=p2.file_uploader("Pipeline de temas (.pkl)",type=["pkl"])

                st.markdown(f"""
                <div class="cluster-info">
                  <b>Parámetros de clustering</b><br>
                  Subtema = {UMBRAL_SUBTEMA} · Tema = {UMBRAL_TEMA} · Máx. temas = {NUM_TEMAS_MAX} ·
                  Fusión = {UMBRAL_FUSION_INTERGRUPO} · Dedup = {UMBRAL_DEDUP_LABEL}
                </div>""",unsafe_allow_html=True)

                if st.form_submit_button("Iniciar análisis completo",use_container_width=True,type="primary"):
                    if not all([f1,f2,f3,bn.strip()]): st.error("Completa todos los campos y sube los tres archivos.")
                    else:
                        al=[a.strip() for a in bat.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(f1,f2,f3,bn,al,tpkl,epkl,mode)); st.rerun()
        else:
            total=st.session_state.total_rows; uniq=st.session_state.unique_rows
            dups=st.session_state.duplicates; dur=st.session_state.process_duration
            cost=st.session_state.get("process_cost","$0.00")
            st.markdown('<div class="success-banner"><div class="success-icon">✓</div><div class="success-content"><div class="success-title">Análisis completado exitosamente</div><div class="success-sub">El informe está listo para descargar</div></div></div>',unsafe_allow_html=True)
            st.markdown(f"""
            <div class="metrics-grid">
              <div class="metric-card m-total"><div class="metric-val" style="color:var(--text)">{total}</div><div class="metric-lbl">Total filas</div></div>
              <div class="metric-card m-unique"><div class="metric-val" style="color:var(--green)">{uniq}</div><div class="metric-lbl">Únicas</div></div>
              <div class="metric-card m-dup"><div class="metric-val" style="color:var(--amber)">{dups}</div><div class="metric-lbl">Duplicados</div></div>
              <div class="metric-card m-time"><div class="metric-val" style="color:var(--blue)">{dur}</div><div class="metric-lbl">Tiempo</div></div>
              <div class="metric-card m-cost"><div class="metric-val" style="color:var(--accent)">{cost}</div><div class="metric-lbl">Costo</div></div>
            </div>""",unsafe_allow_html=True)
            if 'cache_stats' in st.session_state: st.caption(f"📊 {st.session_state['cache_stats']}")
            st.download_button("Descargar informe completo",data=st.session_state.output_data,file_name=st.session_state.output_filename,mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True,type="primary")
            if st.button("Realizar nuevo análisis",use_container_width=True):
                pwd=st.session_state.get("password_correct"); st.session_state.clear(); st.session_state.password_correct=pwd; st.rerun()

    with tab2: render_quick_tab()
    st.markdown('<div class="footer">v15.1.0 · Sistema de Análisis de Noticias con IA · Realizado por Johnathan Cortés ©</div>',unsafe_allow_html=True)

if __name__=="__main__": main()
