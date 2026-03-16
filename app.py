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

# ── Diccionario de corrección de tildes ──────────────────────────────────────
_TILDE_MAP = {
    "regulacion": "regulación", "regulaciones": "regulaciones",
    "innovacion": "innovación", "innovaciones": "innovaciones",
    "tecnologia": "tecnología", "tecnologias": "tecnologías",
    "tecnologica": "tecnológica", "tecnologico": "tecnológico",
    "educacion": "educación", "educativo": "educativo",
    "gestion": "gestión", "administracion": "administración",
    "informacion": "información", "comunicacion": "comunicación",
    "comunicaciones": "comunicaciones",
    "operacion": "operación", "operaciones": "operaciones",
    "inversion": "inversión", "inversiones": "inversiones",
    "expansion": "expansión", "adquisicion": "adquisición",
    "adquisiciones": "adquisiciones", "fusion": "fusión",
    "fusiones": "fusiones", "transicion": "transición",
    "transformacion": "transformación", "digitalizacion": "digitalización",
    "automatizacion": "automatización", "modernizacion": "modernización",
    "optimizacion": "optimización", "implementacion": "implementación",
    "evaluacion": "evaluación", "planificacion": "planificación",
    "organizacion": "organización", "atencion": "atención",
    "produccion": "producción", "construccion": "construcción",
    "distribucion": "distribución", "exportacion": "exportación",
    "importacion": "importación", "comercializacion": "comercialización",
    "negociacion": "negociación", "negociaciones": "negociaciones",
    "participacion": "participación", "colaboracion": "colaboración",
    "asociacion": "asociación", "integracion": "integración",
    "relacion": "relación", "relaciones": "relaciones",
    "situacion": "situación", "condicion": "condición",
    "condiciones": "condiciones", "solucion": "solución",
    "soluciones": "soluciones", "prevencion": "prevención",
    "proteccion": "protección", "fiscalizacion": "fiscalización",
    "sancion": "sanción", "sanciones": "sanciones",
    "investigacion": "investigación", "investigaciones": "investigaciones",
    "accion": "acción", "acciones": "acciones",
    "direccion": "dirección", "decision": "decisión",
    "decisiones": "decisiones", "eleccion": "elección",
    "elecciones": "elecciones", "votacion": "votación",
    "aprobacion": "aprobación", "legislacion": "legislación",
    "reclamacion": "reclamación", "reclamaciones": "reclamaciones",
    "obligacion": "obligación", "obligaciones": "obligaciones",
    "inflacion": "inflación", "deflacion": "deflación",
    "tributacion": "tributación", "tributaria": "tributaria",
    "financiera": "financiera", "financiero": "financiero",
    "financieros": "financieros", "economica": "económica",
    "economico": "económico", "economicos": "económicos",
    "economia": "economía", "credito": "crédito",
    "creditos": "créditos", "prestamo": "préstamo",
    "prestamos": "préstamos", "interes": "interés",
    "deposito": "depósito", "depositos": "depósitos",
    "comision": "comisión", "comisiones": "comisiones",
    "politica": "política", "politicas": "políticas",
    "politico": "político", "politicos": "políticos",
    "publica": "pública", "publico": "público",
    "publicos": "públicos", "republica": "república",
    "democratica": "democrática", "democratico": "democrático",
    "estrategia": "estrategia", "estrategica": "estratégica",
    "estrategico": "estratégico", "estrategicos": "estratégicos",
    "logistica": "logística", "logistico": "logístico",
    "analisis": "análisis", "diagnostico": "diagnóstico",
    "pronostico": "pronóstico", "indice": "índice",
    "indices": "índices", "estadistica": "estadística",
    "vehiculo": "vehículo", "vehiculos": "vehículos",
    "electrico": "eléctrico", "electrica": "eléctrica",
    "electricos": "eléctricos", "energia": "energía",
    "energetica": "energética", "energetico": "energético",
    "petroleo": "petróleo", "mineria": "minería",
    "agricola": "agrícola", "biologica": "biológica",
    "biologico": "biológico", "ecologica": "ecológica",
    "ecologico": "ecológico", "sostenibilidad": "sostenibilidad",
    "medioambiental": "medioambiental",
    "inclusion": "inclusión", "exclusion": "exclusión",
    "pension": "pensión", "pensiones": "pensiones",
    "jubilacion": "jubilación", "compensacion": "compensación",
    "remuneracion": "remuneración", "contratacion": "contratación",
    "capacitacion": "capacitación", "formacion": "formación",
    "certificacion": "certificación", "habilitacion": "habilitación",
    "autorizacion": "autorización", "concesion": "concesión",
    "licitacion": "licitación", "licitaciones": "licitaciones",
    "contratacion": "contratación", "migracion": "migración",
    "poblacion": "población", "seguridad": "seguridad",
    "emergencia": "emergencia", "prevencion": "prevención",
    "atencion": "atención", "recaudacion": "recaudación",
    "asignacion": "asignación", "destinacion": "destinación",
    "corporacion": "corporación", "fundacion": "fundación",
    "institucion": "institución", "instituciones": "instituciones",
    "region": "región", "regional": "regional",
    "unico": "único", "unica": "única",
    "ultimo": "último", "ultima": "última",
    "proximo": "próximo", "proxima": "próxima",
    "basico": "básico", "basica": "básica",
    "clasico": "clásico", "clasica": "clásica",
    "historico": "histórico", "historica": "histórica",
    "medico": "médico", "medica": "médica",
    "medicos": "médicos", "farmaceutica": "farmacéutica",
    "farmaceutico": "farmacéutico", "clinica": "clínica",
    "clinico": "clínico", "cirugia": "cirugía",
    "pediatrica": "pediátrica", "geriatrica": "geriátrica",
    "terapeutica": "terapéutica",
    "numero": "número", "numeros": "números",
    "telefono": "teléfono", "telefonos": "teléfonos",
    "telefonia": "telefonía", "movil": "móvil",
    "moviles": "móviles", "satelite": "satélite",
    "codigo": "código", "codigos": "códigos",
    "algoritmo": "algoritmo", "informatica": "informática",
    "electronica": "electrónica", "electronico": "electrónico",
    "robotica": "robótica", "cibernetica": "cibernética",
    "ciberseguridad": "ciberseguridad",
    "trafico": "tráfico", "transito": "tránsito",
    "aeroportuaria": "aeroportuaria", "aereo": "aéreo",
    "maritimo": "marítimo", "maritima": "marítima",
    "ferroviario": "ferroviario",
    "turismo": "turismo", "turistica": "turística",
    "turistico": "turístico", "hoteleria": "hotelería",
    "gastronomia": "gastronomía", "gastronomica": "gastronómica",
    "academica": "académica", "academico": "académico",
    "pedagogica": "pedagógica", "pedagogico": "pedagógico",
    "cientifica": "científica", "cientifico": "científico",
    "investigacion": "investigación",
    "juridica": "jurídica", "juridico": "jurídico",
    "constitucion": "constitución", "constitucional": "constitucional",
    "resolucion": "resolución", "disposicion": "disposición",
    "notificacion": "notificación",
    "comunicacion": "comunicación",
    "programacion": "programación",
    "actualizacion": "actualización",
    "verificacion": "verificación",
    "validacion": "validación",
    "liquidacion": "liquidación",
    "facturacion": "facturación",
    "recaudacion": "recaudación",
    "tributacion": "tributación",
    "evasion": "evasión",
    "corrupcion": "corrupción",
    "deforestacion": "deforestación",
    "contaminacion": "contaminación",
    "conservacion": "conservación",
    "restauracion": "restauración",
    "rehabilitacion": "rehabilitación",
    "renovacion": "renovación",
    "ampliacion": "ampliación",
    "demolicion": "demolición",
    "inauguracion": "inauguración",
    "celebracion": "celebración",
    "competicion": "competición",
    "clasificacion": "clasificación",
    "eliminacion": "eliminación",
    "negacion": "negación",
    "motivacion": "motivación",
    "inspiracion": "inspiración",
    "frustracion": "frustración",
    "satisfaccion": "satisfacción",
    "reputacion": "reputación",
}

def corregir_tildes(texto: str) -> str:
    """Corrige palabras sin tildes usando diccionario de español."""
    if not texto:
        return texto
    palabras = texto.split()
    resultado = []
    for p in palabras:
        lower = p.lower()
        if lower in _TILDE_MAP:
            corregida = _TILDE_MAP[lower]
            if p[0].isupper():
                corregida = corregida[0].upper() + corregida[1:]
            resultado.append(corregida)
        else:
            resultado.append(p)
    return " ".join(resultado)


# ======================================
# CSS
# ======================================
def load_custom_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@300;400;500&display=swap');

:root {
    --bg:#f8f8f6;--s1:#ffffff;--s2:#f2f1ee;--s3:#eae9e5;
    --border:#e2e0d9;--border2:#ccc9c0;
    --text:#1a1a1a;--text2:#555;--text3:#999;
    --accent:#6366f1;--accent2:#4f46e5;--accent-bg:#eef2ff;--accent-bdr:#c7d2fe;
    --green:#16a34a;--green-bg:#f0fdf4;--green-bdr:#bbf7d0;
    --red:#dc2626;--amber:#d97706;--blue:#2563eb;
    --r:8px;--r2:12px;--r3:16px;
    --shadow-xs:0 1px 2px rgba(0,0,0,.04);
    --shadow-sm:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
    --shadow-md:0 4px 6px -1px rgba(0,0,0,.07),0 2px 4px -2px rgba(0,0,0,.05);
    --shadow-lg:0 10px 15px -3px rgba(0,0,0,.08),0 4px 6px -4px rgba(0,0,0,.04);
}
html,body,[data-testid="stApp"]{background:var(--bg)!important;color:var(--text)!important;
  font-family:'Inter',-apple-system,sans-serif;-webkit-font-smoothing:antialiased}
#MainMenu,footer,header{visibility:hidden}.stDeployButton{display:none}

/* ── Header ── */
.app-header{background:var(--s1);border:1px solid var(--border);border-radius:var(--r3);
  padding:1.5rem 2rem;margin-bottom:1.5rem;display:flex;align-items:center;gap:1.2rem;box-shadow:var(--shadow-sm)}
.app-header-icon{width:48px;height:48px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:var(--r2);display:flex;align-items:center;justify-content:center;font-size:1.4rem;color:#fff;
  flex-shrink:0;box-shadow:0 2px 8px rgba(99,102,241,.25)}
.app-header-text{flex:1}
.app-header-title{font-size:1.35rem;font-weight:700;color:var(--text);letter-spacing:-.02em}
.app-header-version{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--text3);
  letter-spacing:.08em;margin-top:.15rem}
.app-header-badge{background:var(--accent-bg);border:1px solid var(--accent-bdr);color:var(--accent2);
  font-family:'JetBrains Mono',monospace;font-size:.58rem;font-weight:600;padding:.25rem .6rem;
  border-radius:20px;letter-spacing:.06em;text-transform:uppercase}

/* ── Tabs ── */
[data-testid="stTabs"] [data-testid="stTabsList"]{background:var(--s1)!important;
  border:1px solid var(--border)!important;border-radius:var(--r2)!important;padding:4px!important;gap:4px!important}
[data-testid="stTabs"] button[data-baseweb="tab"]{font-family:'Inter'!important;font-size:.82rem!important;
  font-weight:500!important;color:var(--text2)!important;border-radius:var(--r)!important;
  padding:.5rem 1.2rem!important;border:none!important;background:transparent!important;transition:all .2s!important}
[data-testid="stTabs"] button[data-baseweb="tab"]:hover{background:var(--s2)!important;color:var(--text)!important}
[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"]{background:var(--accent-bg)!important;
  color:var(--accent2)!important;border:1px solid var(--accent-bdr)!important;font-weight:600!important}

/* ── Metrics ── */
.metrics-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:.75rem;margin:1.2rem 0}
.metric-card{background:var(--s1);border:1px solid var(--border);border-radius:var(--r2);
  padding:1.1rem .6rem;text-align:center;transition:all .2s;box-shadow:var(--shadow-xs);
  position:relative;overflow:hidden}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;
  border-radius:var(--r2) var(--r2) 0 0}
.metric-card.m-total::before{background:var(--text2)}
.metric-card.m-unique::before{background:var(--green)}
.metric-card.m-dup::before{background:var(--amber)}
.metric-card.m-time::before{background:var(--blue)}
.metric-card.m-cost::before{background:var(--accent)}
.metric-card:hover{border-color:var(--border2);transform:translateY(-1px);box-shadow:var(--shadow-md)}
.metric-val{font-size:1.65rem;font-weight:700;line-height:1;margin-bottom:.4rem}
.metric-lbl{font-family:'JetBrains Mono',monospace;font-size:.6rem;color:var(--text3);
  text-transform:uppercase;letter-spacing:.1em}

/* ── Forms ── */
[data-testid="stForm"]{background:var(--s1)!important;border:1px solid var(--border)!important;
  border-radius:var(--r3)!important;padding:2rem!important;box-shadow:var(--shadow-sm)!important}

/* ── Section Labels ── */
.sec-label{font-size:.7rem;font-weight:600;color:var(--text3);letter-spacing:.1em;
  text-transform:uppercase;padding-bottom:.5rem;border-bottom:1px solid var(--border);
  margin:1.5rem 0 .8rem;display:flex;align-items:center;gap:.5rem}
.sec-label::before{content:'';display:inline-block;width:3px;height:12px;background:var(--accent);border-radius:2px}

/* ── Upload Cards ── */
.upload-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin:.5rem 0 1rem}
.upload-card{background:var(--s1);border:1px solid var(--border);border-radius:var(--r2);
  padding:1.2rem;transition:all .25s ease;box-shadow:var(--shadow-xs);position:relative;overflow:hidden}
.upload-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--accent),var(--accent2));opacity:0;transition:opacity .25s}
.upload-card:hover{border-color:var(--accent-bdr);box-shadow:var(--shadow-md);transform:translateY(-2px)}
.upload-card:hover::before{opacity:1}
.upload-card-header{display:flex;align-items:center;gap:.6rem;margin-bottom:.8rem}
.upload-card-icon{width:32px;height:32px;border-radius:var(--r);display:flex;align-items:center;
  justify-content:center;font-size:.9rem;flex-shrink:0}
.upload-card-icon.ic-dossier{background:#eef2ff;color:#6366f1}
.upload-card-icon.ic-region{background:#f0fdf4;color:#16a34a}
.upload-card-icon.ic-internet{background:#eff6ff;color:#2563eb}
.upload-card-title{font-size:.8rem;font-weight:600;color:var(--text)}
.upload-card-desc{font-size:.68rem;color:var(--text3);line-height:1.3}

/* ── Inputs ── */
[data-testid="stTextInput"] input,[data-testid="stTextArea"] textarea{
  background:var(--s1)!important;border:1px solid var(--border)!important;color:var(--text)!important;
  border-radius:var(--r)!important;font-family:'Inter'!important;font-size:.88rem!important;
  transition:border-color .2s,box-shadow .2s!important}
[data-testid="stTextInput"] input:focus,[data-testid="stTextArea"] textarea:focus{
  border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(99,102,241,.12)!important;outline:none!important}
[data-testid="stTextInput"] input::placeholder,[data-testid="stTextArea"] textarea::placeholder{
  color:var(--text3)!important}
label[data-testid="stWidgetLabel"] p{color:var(--text2)!important;font-size:.82rem!important;font-weight:500!important}

/* ── File Uploader ── */
[data-testid="stFileUploader"]{background:var(--s2)!important;border:2px dashed var(--border)!important;
  border-radius:var(--r)!important;transition:all .2s!important;padding:.6rem!important}
[data-testid="stFileUploader"]:hover{border-color:var(--accent)!important;background:var(--accent-bg)!important}
[data-testid="stFileUploader"] section{padding:.3rem!important}
[data-testid="stFileUploader"] section > div:first-child{font-size:.72rem!important}

/* ── Buttons ── */
.stButton>button,[data-testid="stDownloadButton"]>button{background:var(--s1)!important;
  border:1px solid var(--border)!important;color:var(--text)!important;border-radius:var(--r)!important;
  font-family:'Inter'!important;font-weight:500!important;font-size:.85rem!important;
  transition:all .2s!important;padding:.5rem 1.2rem!important;box-shadow:var(--shadow-xs)!important}
.stButton>button:hover,[data-testid="stDownloadButton"]>button:hover{border-color:var(--accent)!important;
  color:var(--accent2)!important;background:var(--accent-bg)!important;box-shadow:var(--shadow-sm)!important}
.stButton>button[kind="primary"],[data-testid="stDownloadButton"]>button[kind="primary"]{
  background:linear-gradient(135deg,var(--accent),var(--accent2))!important;border:none!important;
  color:#fff!important;font-weight:600!important;box-shadow:0 2px 8px rgba(99,102,241,.3)!important}
.stButton>button[kind="primary"]:hover,[data-testid="stDownloadButton"]>button[kind="primary"]:hover{
  box-shadow:0 4px 16px rgba(99,102,241,.4)!important;transform:translateY(-1px)!important;color:#fff!important}

/* ── Radio ── */
[data-testid="stRadio"] label{color:var(--text2)!important;font-size:.84rem!important}

/* ── Status ── */
[data-testid="stStatus"]{background:var(--s1)!important;border:1px solid var(--border)!important;
  border-radius:var(--r2)!important;font-family:'JetBrains Mono',monospace!important;
  font-size:.78rem!important;box-shadow:var(--shadow-xs)!important}

/* ── Success ── */
.success-banner{background:var(--green-bg);border:1px solid var(--green-bdr);border-left:4px solid var(--green);
  border-radius:var(--r2);padding:1.2rem 1.5rem;margin:.6rem 0 1.2rem;box-shadow:var(--shadow-xs);
  display:flex;align-items:center;gap:1rem}
.success-icon{width:36px;height:36px;background:var(--green);border-radius:50%;display:flex;
  align-items:center;justify-content:center;color:#fff;font-size:1.1rem;flex-shrink:0}
.success-title{font-size:1rem;font-weight:600;color:var(--green);margin-bottom:.1rem}
.success-sub{font-size:.78rem;color:var(--text2)}

/* ── Auth ── */
.auth-wrap{max-width:380px;margin:8vh auto 0;text-align:center}
.auth-icon{width:64px;height:64px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:16px;display:inline-flex;align-items:center;justify-content:center;font-size:1.8rem;
  color:#fff;margin-bottom:1rem;box-shadow:0 4px 12px rgba(99,102,241,.3)}
.auth-title{font-size:1.5rem;font-weight:700;color:var(--text);margin-bottom:.3rem}
.auth-sub{font-size:.8rem;color:var(--text3);margin-bottom:2rem}

/* ── Cluster Info ── */
.cluster-info{background:var(--accent-bg);border:1px solid var(--accent-bdr);border-radius:var(--r2);
  padding:1rem 1.2rem;margin:.6rem 0;font-family:'JetBrains Mono',monospace;font-size:.7rem;
  color:var(--text2);line-height:1.8}

/* ── Progress ── */
[data-testid="stProgressBar"]>div>div{background:linear-gradient(90deg,var(--accent),var(--accent2))!important;
  border-radius:4px!important}

/* ── Misc ── */
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:var(--r)!important}
[data-testid="stAlert"]{background:var(--s1)!important;border:1px solid var(--border)!important;
  border-radius:var(--r)!important;font-size:.84rem!important}
hr{border-color:var(--border)!important}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--s2);border-radius:3px}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:var(--accent)}
.footer{font-family:'JetBrains Mono',monospace;font-size:.58rem;color:var(--text3);text-align:center;
  padding:1.5rem 0 .8rem;letter-spacing:.08em;border-top:1px solid var(--border);margin-top:2.5rem}
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

    def _key(self, text: str) -> str:
        return hashlib.md5(text[:2000].encode('utf-8', errors='ignore')).hexdigest()

    def get(self, text: str) -> Optional[List[float]]:
        k = self._key(text)
        if k in self._cache:
            self._hits += 1
            return self._cache[k]
        self._misses += 1
        return None

    def put(self, text: str, emb: List[float]):
        self._cache[self._key(text)] = emb

    def get_many(self, textos: List[str]) -> Tuple[List[Optional[List[float]]], List[int]]:
        results = [None] * len(textos)
        missing = []
        for i, t in enumerate(textos):
            cached = self.get(t)
            if cached is not None:
                results[i] = cached
            else:
                missing.append(i)
        return results, missing

    def stats(self) -> str:
        total = self._hits + self._misses
        rate = (self._hits / total * 100) if total > 0 else 0
        return f"Cache: {self._hits} hits, {self._misses} misses ({rate:.0f}%)"

    def clear(self):
        self._cache.clear()
        self._hits = 0
        self._misses = 0


if '_emb_cache' not in st.session_state:
    st.session_state['_emb_cache'] = EmbeddingCache()

def get_embedding_cache() -> EmbeddingCache:
    return st.session_state['_emb_cache']


# ======================================
# Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True
    st.markdown("""
    <div class="auth-wrap">
        <div class="auth-icon">◈</div>
        <div class="auth-title">Sistema de Análisis</div>
        <div class="auth-sub">Ingresa tus credenciales para continuar</div>
    </div>""", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("password_form"):
            pw = st.text_input("Contraseña", type="password", placeholder="···")
            if st.form_submit_button("Ingresar", use_container_width=True, type="primary"):
                if pw == st.secrets.get("APP_PASSWORD", "INVALID"):
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta")
    return False


def call_with_retries(fn, *args, **kwargs):
    delay = 1
    for attempt in range(3):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == 2: raise e
            time.sleep(delay); delay *= 2


async def acall_with_retries(fn, *args, **kwargs):
    delay = 1
    for attempt in range(3):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            if attempt == 2: raise e
            await asyncio.sleep(delay); delay *= 2


def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))


def capitalizar_etiqueta(tema: str) -> str:
    if not tema or not tema.strip():
        return "Sin tema"
    tema = tema.strip().lower()
    tema = corregir_tildes(tema)
    return tema[0].upper() + tema[1:]


def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"\'')
    for prefix in ["subtema:", "tema:", "categoría:", "categoria:", "category:"]:
        if tema.lower().startswith(prefix):
            tema = tema[len(prefix):].strip()
    invalid_end = {"en","de","del","la","el","y","o","con","sin","por","para",
                   "sobre","los","las","un","una","al","su","sus","que","se"}
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_end:
        palabras.pop()
    tema = " ".join(palabras[:7])
    return capitalizar_etiqueta(tema) if tema else "Sin tema"


def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema:
        return "Sin tema"
    tl = unidecode(tema.lower())
    for name in [marca] + [a for a in aliases if a]:
        tl = re.sub(rf'\b{re.escape(unidecode(name.strip().lower()))}\b', '', tl)
    for ciudad in CIUDADES_COLOMBIA:
        tl = re.sub(rf'\b{re.escape(ciudad)}\b', '', tl)
    for gent in GENTILICIOS_COLOMBIA:
        tl = re.sub(rf'\b{re.escape(gent)}\b', '', tl)
    for frase in ["en colombia","de colombia","del pais","en el pais",
                   "nacional","colombiano","colombiana","colombianos",
                   "colombianas","territorio nacional"]:
        tl = re.sub(rf'\b{re.escape(frase)}\b', '', tl)
    palabras = [p.strip() for p in tl.split() if p.strip()]
    if not palabras:
        return "Sin tema"
    return limpiar_tema(" ".join(palabras))


def string_norm_label(s: str) -> str:
    if not s: return ""
    s = unidecode(s.lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(t for t in s.split() if t not in STOPWORDS_ES)


def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        m = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if m: return {"value": "Link", "url": m.group(1)}
    return {"value": cell.value, "url": None}


def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str): return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    return re.sub(r"\W+", " ", tmp[0]).lower().strip()


def clean_title_for_output(title: Any) -> str:
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()


def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str): return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    m = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if m: text = text[m.start():]
    if text and not text.endswith("..."):
        text = text.rstrip(".") + "..."
    return text


def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    return {"fm":"Radio","am":"Radio","radio":"Radio",
            "aire":"Televisión","cable":"Televisión","tv":"Televisión",
            "television":"Televisión","televisión":"Televisión",
            "senal abierta":"Televisión","señal abierta":"Televisión",
            "diario":"Prensa","prensa":"Prensa",
            "revista":"Revista","revistas":"Revista",
            "online":"Internet","internet":"Internet",
            "digital":"Internet","web":"Internet",
    }.get(t, str(tipo_raw).strip().title() or "Otro")


def texto_para_embedding(titulo: str, resumen: str, max_len: int = 1800) -> str:
    t = str(titulo or "").strip()
    r = str(resumen or "").strip()
    return f"{t}. {t}. {t}. {r}"[:max_len]


# ======================================
# Deduplicación de etiquetas
# ======================================
def dedup_labels(etiquetas: List[str], umbral: float = UMBRAL_DEDUP_LABEL) -> List[str]:
    unique = list(dict.fromkeys(etiquetas))
    if len(unique) <= 1: return etiquetas
    normed = [string_norm_label(u) for u in unique]
    n = len(unique)
    parent = list(range(n))
    def find(x):
        while parent[x] != x: parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[rb] = ra
    for i in range(n):
        if not normed[i]: continue
        for j in range(i+1, n):
            if not normed[j] or find(i)==find(j): continue
            if SequenceMatcher(None, normed[i], normed[j]).ratio() >= umbral:
                union(i, j)
    label_embs = get_embeddings_batch(unique)
    vp = [(i, label_embs[i]) for i in range(n) if label_embs[i] is not None]
    if len(vp) >= 2:
        vi, vv = zip(*vp)
        sm = cosine_similarity(np.array(vv))
        for pi in range(len(vi)):
            for pj in range(pi+1, len(vi)):
                if sm[pi][pj] >= umbral + 0.05:
                    if find(vi[pi]) != find(vi[pj]): union(vi[pi], vi[pj])
    freq = Counter(etiquetas)
    grupos: Dict[int,List[int]] = defaultdict(list)
    for i in range(n): grupos[find(i)].append(i)
    canon = {}
    for root, members in grupos.items():
        cands = [unique[m] for m in members]
        valid = [c for c in cands if c.lower() not in ("sin tema","varios")]
        canon[root] = max(valid, key=lambda c: (freq[c], len(c))) if valid else cands[0]
    lm = {unique[i]: canon[find(i)] for i in range(n)}
    return [capitalizar_etiqueta(lm.get(e, e)) for e in etiquetas]


# ======================================
# Embeddings con caché
# ======================================
def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos: return []
    cache = get_embedding_cache()
    resultados, missing_idxs = cache.get_many(textos)
    if not missing_idxs: return resultados
    missing_textos = [textos[i][:2000] if textos[i] else "" for i in missing_idxs]
    for i in range(0, len(missing_textos), batch_size):
        batch = missing_textos[i:i+batch_size]
        bidx = missing_idxs[i:i+batch_size]
        try:
            resp = call_with_retries(openai.Embedding.create, input=batch, model=OPENAI_MODEL_EMBEDDING)
            u = resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                t = u.get('total_tokens') if isinstance(u,dict) else getattr(u,'total_tokens',0)
                st.session_state['tokens_embedding'] += (t or 0)
            for j, d in enumerate(resp["data"]):
                oi = bidx[j]; emb = d["embedding"]
                resultados[oi] = emb; cache.put(textos[oi], emb)
        except:
            for j, t in enumerate(batch):
                oi = bidx[j]
                try:
                    r = openai.Embedding.create(input=[t], model=OPENAI_MODEL_EMBEDDING)
                    emb = r["data"][0]["embedding"]; resultados[oi] = emb; cache.put(textos[oi], emb)
                except: pass
    return resultados


# ======================================
# DSU
# ======================================
class DSU:
    def __init__(self, n):
        self.p = list(range(n)); self.rank = [0]*n
    def find(self, i):
        path = []
        while self.p[i] != i: path.append(i); i = self.p[i]
        for node in path: self.p[node] = i
        return i
    def union(self, i, j):
        ri, rj = self.find(i), self.find(j)
        if ri == rj: return
        if self.rank[ri] < self.rank[rj]: ri, rj = rj, ri
        self.p[rj] = ri
        if self.rank[ri] == self.rank[rj]: self.rank[ri] += 1
    def grupos(self, n):
        comp: Dict[int,List[int]] = defaultdict(list)
        for i in range(n): comp[self.find(i)].append(i)
        return dict(comp)


# ======================================
# Agrupación
# ======================================
def agrupar_textos_similares(textos, umbral):
    if not textos: return {}
    embs = get_embeddings_batch(textos)
    valid = [(i,e) for i,e in enumerate(embs) if e is not None]
    if len(valid) < 2: return {}
    idxs, M = zip(*valid)
    labels = AgglomerativeClustering(n_clusters=None, distance_threshold=1-umbral,
        metric="cosine", linkage="average").fit(np.array(M)).labels_
    g: Dict[int,List[int]] = defaultdict(list)
    for k, lbl in enumerate(labels): g[lbl].append(idxs[k])
    return dict(enumerate(g.values()))

def agrupar_por_titulo_similar(titulos):
    gid, grupos, used = 0, {}, set()
    norm = [normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(norm)):
        if i in used or not norm[i]: continue
        grp = [i]; used.add(i)
        for j in range(i+1, len(norm)):
            if j in used or not norm[j]: continue
            if SequenceMatcher(None, norm[i], norm[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                grp.append(j); used.add(j)
        if len(grp) >= 2: grupos[gid] = grp; gid += 1
    return grupos

def seleccionar_representante(indices, textos):
    embs = get_embeddings_batch([textos[i] for i in indices])
    validos = [(indices[k],e) for k,e in enumerate(embs) if e is not None]
    if not validos: return indices[0], textos[indices[0]]
    idxs, M = zip(*validos)
    centro = np.mean(M, axis=0, keepdims=True)
    best = int(np.argmax(cosine_similarity(np.array(M), centro)))
    return idxs[best], textos[idxs[best]]


# ======================================
# Segmentación a nivel de oración
# ======================================
_SENT_SPLIT = re.compile(r'(?<=[.!?;])\s+|(?<=\n)')

def _split_sentences(text: str) -> List[str]:
    parts = _SENT_SPLIT.split(text)
    sents = [p.strip() for p in parts if len(p.strip()) > 15]
    return sents if sents else [text[:600]]


# ======================================
# CLASIFICADOR DE TONO
# ======================================
class ClasificadorTono:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca.strip()
        self.aliases = [a.strip() for a in (aliases or []) if a.strip()]
        self._all_names = [self.marca] + self.aliases
        patterns = [re.escape(unidecode(n.lower())) for n in self._all_names]
        self.brand_re = re.compile(
            r"\b(" + "|".join(patterns) + r")\b", re.IGNORECASE
        ) if patterns else re.compile(r"(a^b)")

    def _extraer_oraciones_marca(self, texto: str) -> List[str]:
        oraciones = _split_sentences(texto)
        resultado = []
        for i, sent in enumerate(oraciones):
            if self.brand_re.search(unidecode(sent.lower())):
                ctx = (oraciones[i-1] + " " + sent) if i > 0 else sent
                resultado.append(ctx.strip())
        if not resultado:
            return [texto[:600]]
        return list(dict.fromkeys(resultado))[:5]

    def _es_sujeto_de_oracion(self, oracion: str) -> bool:
        on = unidecode(oracion.lower())
        match = self.brand_re.search(on)
        if not match: return False
        return match.start() < len(on) * 0.6

    def _analizar_sentimiento_oracion(self, oracion: str) -> Tuple[int, int]:
        on = unidecode(oracion.lower())
        brand_found = self.brand_re.search(on)
        if not brand_found: return 0, 0
        neg_near = bool(re.search(
            r'\b(no|sin|nunca|jamás|niega|rechaza|desmiente|tampoco|ni)\b',
            on[max(0,brand_found.start()-40):brand_found.end()+40], re.IGNORECASE))
        if CRISIS_KEYWORDS.search(on) and RESPONSE_VERBS.search(on):
            if self._es_sujeto_de_oracion(oracion): return 3, 0
        ph = sum(1 for p in POS_PATTERNS if p.search(on))
        nh = sum(1 for p in NEG_PATTERNS if p.search(on))
        weight = 1.0 if self._es_sujeto_de_oracion(oracion) else 0.3
        if neg_near: return int(nh*weight), int(ph*weight)
        return int(ph*weight), int(nh*weight)

    def _reglas(self, oraciones_marca: List[str]) -> Optional[str]:
        tp, tn = 0, 0
        for s in oraciones_marca:
            p, n = self._analizar_sentimiento_oracion(s)
            tp += p; tn += n
        if tp >= 4 and tp > tn * 2.5: return "Positivo"
        if tn >= 4 and tn > tp * 2.5: return "Negativo"
        return None

    async def _llm(self, oraciones_marca: List[str], texto_completo: str) -> Dict[str, str]:
        fragmentos = "\n".join(f"  → {s[:250]}" for s in oraciones_marca[:4])
        ctx = texto_completo[:300]
        prompt = (
            f"Eres un analista de reputación. Evalúa el sentimiento "
            f"EXCLUSIVAMENTE hacia '{self.marca}' "
            f"(alias: {', '.join(self.aliases) if self.aliases else 'sin aliases'}).\n\n"
            f"REGLAS CRÍTICAS:\n"
            f"1. Evalúa SOLO cómo queda '{self.marca}', NO otras empresas\n"
            f"2. Si lo negativo es para un COMPETIDOR pero '{self.marca}' queda "
            f"neutra/positiva → Neutro o Positivo\n"
            f"3. Si '{self.marca}' NO es protagonista → Neutro\n"
            f"4. Si '{self.marca}' solo es mencionada sin carga → Neutro\n\n"
            f"CRITERIOS:\n"
            f"• Positivo: logros, premios, crecimiento, innovación, alianzas, RSE\n"
            f"• Negativo: sanciones, fraudes, quejas, pérdidas, escándalos, fallas\n"
            f"• Neutro: mención informativa sin carga clara\n\n"
            f"ORACIONES CON '{self.marca}':\n{fragmentos}\n\n"
            f"CONTEXTO:\n{ctx}...\n\n"
            f'Responde SOLO JSON: {{"tono":"Positivo|Negativo|Neutro"}}'
        )
        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role":"user","content":prompt}],
                max_tokens=50, temperature=0.0,
                response_format={"type":"json_object"})
            u = resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                st.session_state['tokens_input'] += (u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                st.session_state['tokens_output'] += (u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
            tono = str(json.loads(resp.choices[0].message.content).get("tono","Neutro")).strip().title()
            return {"tono": tono if tono in ("Positivo","Negativo","Neutro") else "Neutro"}
        except:
            return {"tono": "Neutro"}

    async def _clasificar_async(self, texto, sem):
        async with sem:
            om = self._extraer_oraciones_marca(texto)
            r = self._reglas(om)
            if r: return {"tono": r}
            return await self._llm(om, texto)

    async def procesar_lote_async(self, textos, pbar, resumenes, titulos):
        n = len(textos); txts = textos.tolist()
        pbar.progress(0.05, "Agrupando para análisis de tono...")
        txts_emb = [texto_para_embedding(str(titulos.iloc[i]), str(resumenes.iloc[i])) for i in range(n)]
        dsu = DSU(n)
        for g in [agrupar_textos_similares(txts_emb, SIMILARITY_THRESHOLD_TONO),
                  agrupar_por_titulo_similar(titulos.tolist())]:
            for _, idxs in g.items():
                for j in idxs[1:]: dsu.union(idxs[0], j)
        grupos = dsu.grupos(n)
        reps = {cid: seleccionar_representante(idxs, txts)[1] for cid, idxs in grupos.items()}
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        cids = list(reps.keys())
        tasks = [self._clasificar_async(reps[c], sem) for c in cids]
        rl = []
        for i, f in enumerate(asyncio.as_completed(tasks)):
            rl.append(await f)
            pbar.progress(0.1 + 0.85*(i+1)/len(tasks), f"Tono {i+1}/{len(tasks)}")
        rpg = {cids[i]: r for i, r in enumerate(rl)}
        final = [None]*n
        for cid, idxs in grupos.items():
            r = rpg.get(cid, {"tono":"Neutro"})
            for i in idxs: final[i] = r
        pbar.progress(1.0, "Tono completado")
        return final


def analizar_tono_con_pkl(textos, pkl_file):
    try:
        pipeline = joblib.load(pkl_file)
        TM = {1:"Positivo","1":"Positivo",0:"Neutro","0":"Neutro",-1:"Negativo","-1":"Negativo"}
        return [{"tono": TM.get(p, str(p).title())} for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error pkl sentimiento: {e}"); return None


# ======================================
# CLASIFICADOR DE SUBTEMAS
# ======================================
class ClasificadorSubtema:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []
        self._cache: Dict[str, str] = {}

    def _paso1_hash_exacto(self, titulos, resumenes, dsu):
        def nt(t, n):
            return ' '.join(re.sub(r'[^a-z0-9\s]','',unidecode(str(t).lower())).split()[:n])
        bkt_t: Dict[str,List[int]] = defaultdict(list)
        bkt_r: Dict[str,List[int]] = defaultdict(list)
        for i, (ti, re_) in enumerate(zip(titulos, resumenes)):
            a, b = nt(ti, 40), nt(re_, 15)
            if a: bkt_t[hashlib.md5(a.encode()).hexdigest()].append(i)
            if b: bkt_r[hashlib.md5(b.encode()).hexdigest()].append(i)
        for bk in (bkt_t, bkt_r):
            for idxs in bk.values():
                for j in idxs[1:]: dsu.union(idxs[0], j)

    def _paso2_titulos_similares(self, titulos, dsu):
        norm = [normalize_title_for_comparison(t) for t in titulos]
        n = len(norm)
        for i in range(n):
            if not norm[i]: continue
            for j in range(i+1, n):
                if not norm[j] or dsu.find(i)==dsu.find(j): continue
                if SequenceMatcher(None, norm[i], norm[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    dsu.union(i, j)

    def _paso3_semantico(self, emb_textos, all_embs, dsu, pbar, p_start):
        n = len(emb_textos)
        if n < 2: return
        BATCH = 500
        if n <= BATCH:
            pbar.progress(p_start, "Clustering semántico...")
            ok = [(k,e) for k,e in enumerate(all_embs) if e is not None]
            if len(ok) < 2: return
            io_, M = zip(*ok)
            sim = cosine_similarity(np.array(M))
            labels = AgglomerativeClustering(n_clusters=None, distance_threshold=1-UMBRAL_SUBTEMA,
                metric='precomputed', linkage='average').fit(1-sim).labels_
            g: Dict[int,List[int]] = defaultdict(list)
            for k, lbl in enumerate(labels): g[lbl].append(io_[k])
            for cl in g.values():
                if len(cl)>=2:
                    for j in cl[1:]: dsu.union(cl[0], j)
            pbar.progress(p_start+0.18, "Clustering completado")
            return
        tb = max(1,(n+BATCH-1)//BATCH)
        for bn, bs in enumerate(range(0,n,BATCH)):
            bi = list(range(bs, min(bs+BATCH, n)))
            ok = [(idx, all_embs[idx]) for idx in bi if all_embs[idx] is not None]
            if len(ok)<2: continue
            io_, M = zip(*ok)
            sim = cosine_similarity(np.array(M))
            labels = AgglomerativeClustering(n_clusters=None, distance_threshold=1-UMBRAL_SUBTEMA,
                metric='precomputed', linkage='average').fit(1-sim).labels_
            g: Dict[int,List[int]] = defaultdict(list)
            for k, lbl in enumerate(labels): g[lbl].append(io_[k])
            for cl in g.values():
                if len(cl)>=2:
                    for j in cl[1:]: dsu.union(cl[0], j)
            pbar.progress(p_start+0.15*(bn+1)/tb, f"Clustering lote {bn+1}/{tb}...")
        pbar.progress(p_start+0.16, "Unificando lotes...")
        self._fusion_iterativa(emb_textos, all_embs, dsu, pbar, p_start+0.16)

    def _fusion_iterativa(self, textos, all_embs, dsu, pbar, p_start):
        n = len(textos)
        for it in range(MAX_ITER_FUSION):
            grupos = dsu.grupos(n)
            if len(grupos)<2: break
            centroids, vg = [], []
            for gid, idxs in grupos.items():
                vecs = [all_embs[i] for i in idxs[:50] if all_embs[i] is not None]
                if vecs: centroids.append(np.mean(vecs, axis=0)); vg.append(gid)
            if len(vg)<2: break
            sim = cosine_similarity(np.array(centroids))
            pairs = sorted([(sim[i][j],i,j) for i in range(len(vg))
                for j in range(i+1,len(vg)) if sim[i][j]>=UMBRAL_FUSION_INTERGRUPO], reverse=True)
            fus = 0
            for _, i, j in pairs:
                ri, rj = grupos[vg[i]][0], grupos[vg[j]][0]
                if dsu.find(ri) != dsu.find(rj): dsu.union(ri, rj); fus += 1
            pbar.progress(min(p_start+0.04*(it+1), 0.52), f"Fusión iter {it+1}: {fus} fusiones")
            if fus == 0: break

    def _generar_etiqueta(self, textos_grp, titulos_grp, resumenes_grp):
        titulos_norm = sorted(set(normalize_title_for_comparison(t) for t in titulos_grp if t))
        ck = hashlib.md5("|".join(titulos_norm[:12]).encode()).hexdigest()
        if ck in self._cache: return self._cache[ck]

        palabras = []
        for t in titulos_grp[:8]:
            for w in string_norm_label(t).split():
                if len(w)>3: palabras.append(w)
        keywords = " · ".join(w for w,_ in Counter(palabras).most_common(8))
        tit_m = list(dict.fromkeys(t[:120] for t in titulos_grp if t))[:6]
        res_m = [str(r)[:200] for r in resumenes_grp[:3] if r and len(str(r))>20]
        ctx = ""
        if res_m:
            ctx = "\n\nRESÚMENES:\n" + "\n".join(f"  · {r}" for r in res_m)

        prompt = (
            "Genera un SUBTEMA periodístico en español (3-6 palabras) que describa "
            "el asunto central de estas noticias.\n\n"
            f"TÍTULOS:\n" + "\n".join(f"  · {t}" for t in tit_m) + ctx + "\n\n"
            f"PALABRAS CLAVE: {keywords}\n\n"
            "REGLAS:\n"
            "1. NO uses nombres de empresas, marcas, personas ni ciudades\n"
            "2. Debe ser una FRASE COHERENTE en español con preposiciones y artículos "
            "donde corresponda (ej: 'Regulación del sector financiero', NO 'Regulación sector financiero')\n"
            "3. NO concatenes palabras sueltas (INCORRECTO: 'Seguimiento derechos laborales fusiones')\n"
            "4. Usa tildes correctas en español (ej: 'Regulación', NO 'Regulacion')\n"
            "5. Todo en minúsculas excepto la primera letra\n"
            "6. Debe describir el ASUNTO ESPECÍFICO, no el actor\n"
            "7. NO uses palabras genéricas solas como 'Gestión', 'Noticias', 'Eventos'\n\n"
            "EJEMPLOS CORRECTOS:\n"
            "  · Resultados financieros del trimestre\n"
            "  · Programa de becas universitarias\n"
            "  · Expansión de la red de sucursales\n"
            "  · Regulación del mercado de valores\n"
            "  · Alianza para inclusión financiera\n"
            "  · Transformación digital de servicios\n\n"
            "EJEMPLOS INCORRECTOS:\n"
            "  · Seguimiento derechos laborales fusiones\n"
            "  · Regulacion financiera sector\n"
            "  · Gestión Empresarial\n"
            "  · RESULTADOS FINANCIEROS\n\n"
            'Responde SOLO JSON: {"subtema":"..."}'
        )
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role":"user","content":prompt}],
                max_tokens=50, temperature=0.0,
                response_format={"type":"json_object"})
            u = resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                st.session_state['tokens_input'] += (u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                st.session_state['tokens_output'] += (u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
            raw = json.loads(resp.choices[0].message.content).get("subtema", "Varios")
            etiqueta = limpiar_tema_geografico(limpiar_tema(raw), self.marca, self.aliases)
            genericas = {"gestión","gestion","actividades","acciones","noticias",
                "información","informacion","eventos","varios","sin tema",
                "actividad corporativa","noticias corporativas",
                "gestión empresarial","gestion empresarial",
                "cobertura informativa","gestión integral"}
            if (string_norm_label(etiqueta) in {string_norm_label(g) for g in genericas}
                    or len(etiqueta.split()) < 2):
                etiqueta = self._refinar_etiqueta(tit_m, keywords, res_m)
        except:
            etiqueta = self._etiqueta_fallback(titulos_grp)
        etiqueta = capitalizar_etiqueta(etiqueta)
        self._cache[ck] = etiqueta
        return etiqueta

    def _refinar_etiqueta(self, titulos, keywords, resumenes=None):
        ctx = ""
        if resumenes:
            ctx = f"\nContexto: {' | '.join(r[:100] for r in resumenes[:2])}"
        prompt = (
            "Estos títulos comparten un tema. Genera una frase temática "
            "COHERENTE en español de 3-5 palabras con preposiciones donde corresponda.\n\n"
            f"Títulos: {' | '.join(titulos[:4])}\n"
            f"Keywords: {keywords}{ctx}\n\n"
            "IMPORTANTE: Debe ser una frase natural en español, NO palabras sueltas.\n"
            "Usa tildes correctas. Todo en minúsculas excepto la primera letra.\n"
            'JSON: {"subtema":"..."}'
        )
        try:
            resp = call_with_retries(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role":"user","content":prompt}],
                max_tokens=50, temperature=0.1,
                response_format={"type":"json_object"})
            u = resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
            if u:
                st.session_state['tokens_input'] += (u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                st.session_state['tokens_output'] += (u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
            raw = json.loads(resp.choices[0].message.content).get("subtema","Varios")
            return limpiar_tema_geografico(limpiar_tema(raw), self.marca, self.aliases)
        except:
            return self._etiqueta_fallback([])

    def _etiqueta_fallback(self, titulos):
        if not titulos: return "Cobertura informativa general"
        palabras = []
        for t in titulos[:5]:
            for w in string_norm_label(t).split():
                if len(w)>4: palabras.append(w)
        if palabras:
            top = [w for w,_ in Counter(palabras).most_common(3)]
            # Intentar formar frase coherente con conectores
            if len(top) >= 2:
                frase = f"{top[0]} de {top[1]}" if len(top) == 2 else f"{top[0]} de {top[1]} y {top[2]}"
                return capitalizar_etiqueta(frase)
            return capitalizar_etiqueta(top[0])
        return "Cobertura informativa general"

    def procesar_lote(self, col_resumen, pbar, resumenes_puros, titulos_puros):
        textos = col_resumen.tolist()
        titulos = titulos_puros.tolist()
        resumenes = resumenes_puros.tolist()
        n = len(textos)
        emb_textos = [texto_para_embedding(titulos[i], resumenes[i]) for i in range(n)]
        pbar.progress(0.05, "Fase 1 · Agrupando noticias idénticas...")
        dsu = DSU(n)
        self._paso1_hash_exacto(titulos, resumenes, dsu)
        pbar.progress(0.12, "Fase 2 · Similitud de títulos...")
        self._paso2_titulos_similares(titulos, dsu)
        pbar.progress(0.18, "Calculando embeddings...")
        all_embs = get_embeddings_batch(emb_textos)
        pbar.progress(0.20, "Fase 3 · Clustering semántico...")
        self._paso3_semantico(emb_textos, all_embs, dsu, pbar, 0.20)
        gf = dsu.grupos(n); ng = len(gf)
        pbar.progress(0.55, f"Fase 4 · Etiquetando {ng} grupos...")
        mapa: Dict[int,str] = {}
        sg = sorted(gf.items(), key=lambda x: -len(x[1]))
        for k, (lid, idxs) in enumerate(sg):
            if k%10==0:
                pbar.progress(0.55+0.30*(k/max(ng,1)), f"Etiquetando {k+1}/{ng}...")
            et = self._generar_etiqueta(
                [textos[i] for i in idxs],
                [titulos[i] for i in idxs],
                [resumenes[i] for i in idxs])
            for i in idxs: mapa[i] = et
        subtemas = [mapa.get(i,"Varios") for i in range(n)]
        pbar.progress(0.88, "Fase 5 · Deduplicando etiquetas...")
        subtemas = dedup_labels(subtemas, UMBRAL_DEDUP_LABEL)
        pbar.progress(0.93, "Fase 6 · Verificando consistencia...")
        subtemas = self._verificar_consistencia(subtemas, all_embs, pbar)
        subtemas = [capitalizar_etiqueta(s) for s in subtemas]
        nf = len(set(subtemas))
        pbar.progress(1.0, f"Completado: {nf} subtemas")
        st.info(f"Subtemas únicos: **{nf}** · Grupos: **{ng}**")
        return subtemas

    def _verificar_consistencia(self, subtemas, all_embs, pbar):
        por_sub: Dict[str,List[int]] = defaultdict(list)
        for i, s in enumerate(subtemas): por_sub[s].append(i)
        resultado = list(subtemas)
        centroids = {}
        for sub, idxs in por_sub.items():
            vecs = [all_embs[i] for i in idxs if all_embs[i] is not None]
            if vecs: centroids[sub] = np.mean(vecs, axis=0)
        for sub in [s for s in centroids if len(por_sub[s])>=3]:
            idxs = por_sub[sub]
            if sub.lower() in ("sin tema","varios") or len(idxs)<3: continue
            vi = [(i, all_embs[i]) for i in idxs if all_embs[i] is not None]
            if len(vi)<3: continue
            v_idxs, v_vecs = zip(*vi)
            M = np.array(v_vecs)
            sims = cosine_similarity(M, centroids[sub].reshape(1,-1)).flatten()
            thr = max(0.60, np.mean(sims)-2*np.std(sims))
            for k, (oi, sv) in enumerate(zip(v_idxs, sims)):
                if sv < thr:
                    best_sub, best_sim = sub, sv
                    emb = all_embs[oi]
                    for os, oc in centroids.items():
                        if os == sub: continue
                        s = cosine_similarity(np.array(emb).reshape(1,-1), oc.reshape(1,-1))[0][0]
                        if s > best_sim and s > 0.75: best_sim = s; best_sub = os
                    if best_sub != sub: resultado[oi] = best_sub
        return resultado


# ======================================
# CONSOLIDACIÓN DE TEMAS
# ======================================
def consolidar_temas(subtemas, textos, pbar):
    pbar.progress(0.05, "Calculando centroides...")
    df = pd.DataFrame({'subtema': subtemas, 'texto': textos})
    us = list(df['subtema'].unique())
    if len(us) <= 1:
        pbar.progress(1.0, "Un solo tema")
        return [capitalizar_etiqueta(s) for s in subtemas]
    ae = get_embeddings_batch(textos)
    centroids = {}
    for sub in us:
        idxs = df.index[df['subtema']==sub].tolist()[:40]
        vecs = [ae[i] for i in idxs if ae[i] is not None]
        if vecs: centroids[sub] = np.mean(vecs, axis=0)
    ler = get_embeddings_batch(us)
    le = {s:e for s,e in zip(us,ler) if e is not None}
    vs = [s for s in us if s in centroids]
    if len(vs)<2:
        pbar.progress(1.0, "Sin agrupación posible")
        return [capitalizar_etiqueta(s) for s in subtemas]
    pbar.progress(0.40, "Clustering de subtemas en temas...")
    Mc = np.array([centroids[s] for s in vs])
    sc = cosine_similarity(Mc)
    if all(s in le for s in vs):
        Ml = np.array([le[s] for s in vs])
        sim = 0.6*sc + 0.4*cosine_similarity(Ml)
    else:
        sim = sc
    cl = AgglomerativeClustering(n_clusters=None, distance_threshold=1-UMBRAL_TEMA,
        metric='precomputed', linkage='average').fit(1-sim)
    if len(set(cl.labels_))>NUM_TEMAS_MAX:
        cl = AgglomerativeClustering(n_clusters=NUM_TEMAS_MAX,
            metric='precomputed', linkage='average').fit(1-sim)
    cs: Dict[int,List[str]] = defaultdict(list)
    for i, lbl in enumerate(cl.labels_): cs[lbl].append(vs[i])
    uc = [s for s in us if s not in vs]
    mt: Dict[str,str] = {}
    tc = len(cs)
    for k, (cid, ls) in enumerate(cs.items()):
        pbar.progress(0.50+0.40*(k/max(tc,1)), f"Tema {k+1}/{tc}...")
        if len(ls)==1:
            nombre = ls[0]
            p = nombre.split()
            if len(p)>3: nombre = " ".join(p[:3])
        else:
            prompt = (
                "Genera UNA categoría temática general en español (2-4 palabras) "
                "que agrupe estos subtemas. Debe ser una FRASE COHERENTE con "
                "preposiciones y artículos donde corresponda.\n\n"
                f"SUBTEMAS:\n" + "\n".join(f"  · {s}" for s in ls[:10]) + "\n\n"
                "REGLAS:\n"
                "- Sin nombres de empresas ni ciudades\n"
                "- FRASE NATURAL en español, NO palabras sueltas\n"
                "- Usa tildes correctas (ej: 'Regulación', NO 'Regulacion')\n"
                "- Todo en minúsculas excepto la primera letra\n"
                "- NO tan vaga como 'Noticias' o 'Información'\n"
                "- Responde SOLO el nombre del tema"
            )
            try:
                resp = call_with_retries(
                    openai.ChatCompletion.create,
                    model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role":"user","content":prompt}],
                    max_tokens=20, temperature=0.0)
                u = resp.get('usage',{}) if isinstance(resp,dict) else getattr(resp,'usage',{})
                if u:
                    st.session_state['tokens_input'] += (u.get('prompt_tokens') if isinstance(u,dict) else getattr(u,'prompt_tokens',0)) or 0
                    st.session_state['tokens_output'] += (u.get('completion_tokens') if isinstance(u,dict) else getattr(u,'completion_tokens',0)) or 0
                nombre = limpiar_tema(resp.choices[0].message.content.strip().replace('"','').replace('.',''))
            except:
                nombre = ls[0]
        for sub in ls: mt[sub] = nombre
    for sub in uc: mt[sub] = sub
    tf = [mt.get(sub, sub) for sub in subtemas]
    pbar.progress(0.92, "Deduplicando temas...")
    tf = dedup_labels(tf, UMBRAL_DEDUP_LABEL)
    tf = [capitalizar_etiqueta(t) for t in tf]
    nt = len(set(tf))
    st.info(f"Temas consolidados: **{nt}** (máx: {NUM_TEMAS_MAX})")
    pbar.progress(1.0, "Temas finalizados")
    return tf


def analizar_temas_con_pkl(textos, pkl_file):
    try:
        pipeline = joblib.load(pkl_file)
        return [capitalizar_etiqueta(str(p)) for p in pipeline.predict(textos)]
    except Exception as e:
        st.error(f"Error pkl temas: {e}"); return None


# ======================================
# Duplicados y Excel
# ======================================
def detectar_duplicados_avanzado(rows, key_map):
    processed = deepcopy(rows)
    seen_url, seen_bcast = {}, {}
    title_buckets: Dict[tuple,List[int]] = defaultdict(list)
    for i, row in enumerate(processed):
        if row.get("is_duplicate"): continue
        tipo = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio",""))))
        mencion = norm_key(row.get(key_map.get("menciones","")))
        medio = norm_key(row.get(key_map.get("medio","")))
        if tipo == "Internet":
            li = row.get(key_map.get("link_nota",{})) or {}
            url = li.get("url") if isinstance(li,dict) else None
            if url and mencion:
                k = (url, mencion)
                if k in seen_url:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed[seen_url[k]].get(key_map.get("idnoticia",""),"")
                    continue
                seen_url[k] = i
            if medio and mencion: title_buckets[(medio,mencion)].append(i)
        elif tipo in ("Radio","Televisión"):
            hora = str(row.get(key_map.get("hora",""),"")).strip()
            if mencion and medio and hora:
                k = (mencion,medio,hora)
                if k in seen_bcast:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed[seen_bcast[k]].get(key_map.get("idnoticia",""),"")
                else: seen_bcast[k] = i
    for idxs in title_buckets.values():
        if len(idxs)<2: continue
        for i in range(len(idxs)):
            for j in range(i+1, len(idxs)):
                a, b = idxs[i], idxs[j]
                if processed[a].get("is_duplicate") or processed[b].get("is_duplicate"): continue
                ta = normalize_title_for_comparison(processed[a].get(key_map.get("titulo","")))
                tb = normalize_title_for_comparison(processed[b].get(key_map.get("titulo","")))
                if ta and tb and SequenceMatcher(None,ta,tb).ratio()>=SIMILARITY_THRESHOLD_TITULOS:
                    if len(ta)<len(tb):
                        processed[a]["is_duplicate"]=True
                        processed[a]["idduplicada"]=processed[b].get(key_map.get("idnoticia",""),"")
                    else:
                        processed[b]["is_duplicate"]=True
                        processed[b]["idduplicada"]=processed[a].get(key_map.get("idnoticia",""),"")
    return processed


def run_dossier_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    nk = [norm_key(h) for h in headers]
    km = {n:n for n in nk}
    km.update({
        "titulo":norm_key("Titulo"),"resumen":norm_key("Resumen - Aclaracion"),
        "menciones":norm_key("Menciones - Empresa"),"medio":norm_key("Medio"),
        "tonoiai":norm_key("Tono IA"),"tema":norm_key("Tema"),"subtema":norm_key("Subtema"),
        "idnoticia":norm_key("ID Noticia"),"idduplicada":norm_key("ID duplicada"),
        "tipodemedio":norm_key("Tipo de Medio"),"hora":norm_key("Hora"),
        "link_nota":norm_key("Link Nota"),"link_streaming":norm_key("Link (Streaming - Imagen)"),
        "region":norm_key("Region"),
    })
    rows, split_rows = [], []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row): continue
        rows.append({nk[i]:c for i,c in enumerate(row) if i<len(nk)})
    for rc in rows:
        base = {k:(extract_link(v) if k in (km["link_nota"],km["link_streaming"]) else v.value)
                for k,v in rc.items()}
        if km.get("tipodemedio") in base:
            base[km["tipodemedio"]] = normalizar_tipo_medio(base.get(km["tipodemedio"]))
        ml = [m.strip() for m in str(base.get(km["menciones"],"")).split(";") if m.strip()]
        for m in ml or [None]:
            nr = deepcopy(base)
            if m: nr[km["menciones"]] = m
            split_rows.append(nr)
    for idx, row in enumerate(split_rows):
        row.update({"original_index":idx,"is_duplicate":False})
    processed = detectar_duplicados_avanzado(split_rows, km)
    for row in processed:
        if row["is_duplicate"]:
            row.update({km["tonoiai"]:"Duplicada",km["tema"]:"Duplicada",km["subtema"]:"Duplicada"})
    return processed, km


def fix_links_by_media_type(row, km):
    tkey = km.get("tipodemedio"); ln = km.get("link_nota"); ls = km.get("link_streaming")
    if not(tkey and ln and ls): return
    tipo = row.get(tkey,"")
    rl = row.get(ln) or {"value":"","url":None}
    rs = row.get(ls) or {"value":"","url":None}
    hurl = lambda x: isinstance(x,dict) and bool(x.get("url"))
    if tipo in ("Radio","Televisión"): row[ls]={"value":"","url":None}
    elif tipo=="Internet": row[ln],row[ls]=rs,rl
    elif tipo in ("Prensa","Revista"):
        if not hurl(rl) and hurl(rs): row[ln]=rs
        row[ls]={"value":"","url":None}


def generate_output_excel(rows, km):
    wb = Workbook(); ws = wb.active; ws.title = "Resultado"
    ORDER = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Region",
        "Seccion - Programa","Titulo","Autor - Conductor","Nro. Pagina",
        "Dimension","Duracion - Nro. Caracteres","CPE","Audiencia","Tier",
        "Tono","Tono IA","Tema","Subtema","Link Nota",
        "Resumen - Aclaracion","Link (Streaming - Imagen)",
        "Menciones - Empresa","ID duplicada"]
    NUM = {"ID Noticia","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia"}
    ws.append(ORDER)
    ls = NamedStyle(name="HL", font=Font(color="0000FF", underline="single"))
    if "HL" not in wb.style_names: wb.add_named_style(ls)
    for row in rows:
        tk = km.get("titulo")
        if tk and tk in row: row[tk] = clean_title_for_output(row.get(tk))
        rk = km.get("resumen")
        if rk and rk in row: row[rk] = corregir_texto(row.get(rk))
        out, links = [], {}
        for ci, h in enumerate(ORDER, 1):
            dk = km.get(norm_key(h), norm_key(h)); val = row.get(dk); cv = None
            if h in NUM:
                try: cv = float(val) if val is not None and str(val).strip()!="" else None
                except: cv = str(val) if val is not None else None
            elif isinstance(val,dict) and "url" in val:
                cv = val.get("value","Link")
                if val.get("url"): links[ci] = val["url"]
            elif val is not None: cv = str(val)
            out.append(cv)
        ws.append(out)
        for ci, url in links.items():
            cell = ws.cell(row=ws.max_row, column=ci)
            cell.hyperlink = url; cell.style = "HL"
    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()


# ======================================
# Proceso principal
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file,
                                  brand_name, brand_aliases, tono_pkl, tema_pkl, mode):
    st.session_state.update({'tokens_input':0,'tokens_output':0,'tokens_embedding':0})
    get_embedding_cache().clear(); t0 = time.time()
    if "API" in mode:
        try: openai.api_key = st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
        except: st.error("OPENAI_API_KEY no encontrado."); st.stop()

    with st.status("Paso 1 · Limpieza y duplicados", expanded=True) as s:
        rows, km = run_dossier_logic(load_workbook(dossier_file, data_only=True).active)
        s.update(label="Paso 1 completado", state="complete")

    with st.status("Paso 2 · Mapeos", expanded=True) as s:
        dfr = pd.read_excel(region_file)
        rmap = {str(k).lower().strip():v for k,v in
            pd.Series(dfr.iloc[:,1].values, index=dfr.iloc[:,0]).to_dict().items()}
        dfi = pd.read_excel(internet_file)
        imap = {str(k).lower().strip():v for k,v in
            pd.Series(dfi.iloc[:,1].values, index=dfi.iloc[:,0]).to_dict().items()}
        for row in rows:
            mk = str(row.get(km.get("medio",""),"")).lower().strip()
            row[km.get("region")] = rmap.get(mk,"N/A")
            if mk in imap:
                row[km.get("medio")] = imap[mk]
                row[km.get("tipodemedio")] = "Internet"
            fix_links_by_media_type(row, km)
        s.update(label="Paso 2 completado", state="complete")

    gc.collect()
    to_analyze = [r for r in rows if not r.get("is_duplicate")]
    if to_analyze:
        df = pd.DataFrame(to_analyze)
        df["_txt"] = df.apply(lambda r: texto_para_embedding(
            str(r.get(km["titulo"],"")), str(r.get(km["resumen"],""))), axis=1)

        with st.status("Pre-calculando embeddings...", expanded=True) as s:
            _ = get_embeddings_batch(df["_txt"].tolist())
            s.update(label=f"Embeddings · {get_embedding_cache().stats()}", state="complete")

        with st.status("Paso 3 · Tono", expanded=True) as s:
            pb = st.progress(0)
            if "PKL" in mode and tono_pkl:
                res = analizar_tono_con_pkl(df["_txt"].tolist(), tono_pkl)
                if res is None: st.stop()
            elif "API" in mode:
                res = await ClasificadorTono(brand_name, brand_aliases).procesar_lote_async(
                    df["_txt"], pb, df[km["resumen"]], df[km["titulo"]])
            else: res = [{"tono":"N/A"}]*len(to_analyze)
            df[km["tonoiai"]] = [r["tono"] for r in res]
            s.update(label="Paso 3 completado · Tono", state="complete")

        with st.status("Paso 4 · Tema y Subtema", expanded=True) as s:
            pb = st.progress(0)
            if "Solo Modelos PKL" in mode:
                subtemas = ["N/A"]*len(to_analyze); temas = ["N/A"]*len(to_analyze)
            else:
                subtemas = ClasificadorSubtema(brand_name, brand_aliases).procesar_lote(
                    df["_txt"], pb, df[km["resumen"]], df[km["titulo"]])
                temas = consolidar_temas(subtemas, df["_txt"].tolist(), pb)
            df[km["subtema"]] = subtemas
            if "PKL" in mode and tema_pkl:
                tp = analizar_temas_con_pkl(df["_txt"].tolist(), tema_pkl)
                if tp: df[km["tema"]] = tp
            else: df[km["tema"]] = temas
            s.update(label="Paso 4 completado · Clasificación", state="complete")

        rm2 = df.set_index("original_index").to_dict("index")
        for row in rows:
            if not row.get("is_duplicate"): row.update(rm2.get(row["original_index"],{}))

    gc.collect()
    ci = (st.session_state['tokens_input']/1e6)*PRICE_INPUT_1M
    co = (st.session_state['tokens_output']/1e6)*PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding']/1e6)*PRICE_EMBEDDING_1M

    with st.status("Paso 5 · Generando informe", expanded=True) as s:
        st.session_state["output_data"] = generate_output_excel(rows, km)
        st.session_state["output_filename"] = (
            f"Informe_IA_{brand_name.replace(' ','_')}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")
        st.session_state["processing_complete"] = True
        st.session_state.update({
            "brand_name":brand_name,"brand_aliases":brand_aliases,
            "total_rows":len(rows),"unique_rows":len(to_analyze),
            "duplicates":len(rows)-len(to_analyze),
            "process_duration":f"{time.time()-t0:.0f}s",
            "process_cost":f"${ci+co+ce:.4f} USD",
            "cache_stats":get_embedding_cache().stats(),
        })
        s.update(label=f"Completado · {get_embedding_cache().stats()}", state="complete")


# ======================================
# Análisis Rápido
# ======================================
async def run_quick_analysis_async(df, tc, sc, bn, aliases):
    st.session_state.update({'tokens_input':0,'tokens_output':0,'tokens_embedding':0})
    get_embedding_cache().clear()
    df['_txt'] = df.apply(lambda r: texto_para_embedding(
        str(r.get(tc,"")), str(r.get(sc,""))), axis=1)

    with st.status("Embeddings...", expanded=True) as s:
        _ = get_embeddings_batch(df['_txt'].tolist())
        s.update(label=f"Embeddings · {get_embedding_cache().stats()}", state="complete")
    with st.status("Paso 1/2 · Tono...", expanded=True) as s:
        pb = st.progress(0)
        res = await ClasificadorTono(bn, aliases).procesar_lote_async(
            df["_txt"], pb, df[sc].fillna(''), df[tc].fillna(''))
        df['Tono IA'] = [r["tono"] for r in res]
        s.update(label="Tono completado", state="complete")
    with st.status("Paso 2/2 · Clasificación...", expanded=True) as s:
        pb = st.progress(0)
        subtemas = ClasificadorSubtema(bn, aliases).procesar_lote(
            df["_txt"], pb, df[sc].fillna(''), df[tc].fillna(''))
        df['Subtema'] = subtemas
        temas = consolidar_temas(subtemas, df["_txt"].tolist(), pb)
        df['Tema'] = temas
        s.update(label="Clasificación completada", state="complete")
    df.drop(columns=['_txt'], inplace=True)
    ci = (st.session_state['tokens_input']/1e6)*PRICE_INPUT_1M
    co = (st.session_state['tokens_output']/1e6)*PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding']/1e6)*PRICE_EMBEDDING_1M
    st.session_state['quick_cost'] = f"${ci+co+ce:.4f} USD"
    return df


def gen_quick_excel(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Analisis')
    return buf.getvalue()


def render_quick_tab():
    st.markdown('<div class="sec-label">Análisis rápido</div>', unsafe_allow_html=True)
    if 'quick_result' in st.session_state:
        st.markdown(
            '<div class="success-banner"><div class="success-icon">✓</div>'
            '<div class="success-content"><div class="success-title">Análisis completado</div>'
            '<div class="success-sub">Resultados listos</div></div></div>', unsafe_allow_html=True)
        st.metric("Costo", st.session_state.get('quick_cost',"$0.00"))
        st.dataframe(st.session_state.quick_result.head(10), use_container_width=True)
        st.download_button("Descargar", data=gen_quick_excel(st.session_state.quick_result),
            file_name="Analisis_Rapido_IA.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary")
        if st.button("Nuevo análisis"):
            for k in ('quick_result','quick_df','quick_name','quick_cost'):
                if k in st.session_state: del st.session_state[k]
            st.rerun()
        return
    if 'quick_df' not in st.session_state:
        st.markdown("Sube un archivo Excel con columnas de título y resumen.")
        f = st.file_uploader("Archivo Excel", type=["xlsx"], label_visibility="collapsed", key="qu")
        if f:
            try: st.session_state.quick_df = pd.read_excel(f); st.session_state.quick_name = f.name; st.rerun()
            except Exception as e: st.error(f"Error: {e}"); st.stop()
    else:
        st.success(f"**{st.session_state.quick_name}** cargado")
        with st.form("qf"):
            cols = st.session_state.quick_df.columns.tolist()
            c1,c2 = st.columns(2)
            tc = c1.selectbox("Columna Título", cols, 0)
            sc = c2.selectbox("Columna Resumen", cols, 1 if len(cols)>1 else 0)
            st.write("---")
            bn = st.text_input("Marca principal", placeholder="Ej: Bancolombia")
            bat = st.text_area("Alias (sep. ;)", placeholder="Ej: Grupo Bancolombia;Ban", height=70)
            if st.form_submit_button("Analizar", use_container_width=True, type="primary"):
                if not bn: st.error("Indica la marca.")
                else:
                    try: openai.api_key = st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
                    except: st.error("OPENAI_API_KEY no encontrada."); st.stop()
                    aliases = [a.strip() for a in bat.split(";") if a.strip()]
                    with st.spinner("Analizando..."):
                        st.session_state.quick_result = asyncio.run(
                            run_quick_analysis_async(st.session_state.quick_df.copy(), tc, sc, bn, aliases))
                    st.rerun()
        if st.button("Otro archivo"):
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
            <div class="app-header-version">v14.0 · tildes · frases coherentes · brand-aware tone</div>
        </div>
        <div class="app-header-badge">IA Powered</div>
    </div>""", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Análisis Completo", "Análisis Rápido"])

    with tab1:
        if not st.session_state.get("processing_complete", False):
            with st.form("main_form"):
                st.markdown('<div class="sec-label">Archivos de entrada</div>', unsafe_allow_html=True)

                st.markdown("""
                <div class="upload-grid">
                    <div class="upload-card">
                        <div class="upload-card-header">
                            <div class="upload-card-icon ic-dossier">📋</div>
                            <div>
                                <div class="upload-card-title">Dossier</div>
                                <div class="upload-card-desc">Archivo principal con las noticias</div>
                            </div>
                        </div>
                    </div>
                    <div class="upload-card">
                        <div class="upload-card-header">
                            <div class="upload-card-icon ic-region">🗺️</div>
                            <div>
                                <div class="upload-card-title">Región</div>
                                <div class="upload-card-desc">Mapeo de medios a regiones</div>
                            </div>
                        </div>
                    </div>
                    <div class="upload-card">
                        <div class="upload-card-header">
                            <div class="upload-card-icon ic-internet">🌐</div>
                            <div>
                                <div class="upload-card-title">Internet</div>
                                <div class="upload-card-desc">Mapeo de medios digitales</div>
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                c1, c2, c3 = st.columns(3)
                df_file = c1.file_uploader("Dossier", type=["xlsx"], label_visibility="collapsed", key="f1")
                reg_file = c2.file_uploader("Región", type=["xlsx"], label_visibility="collapsed", key="f2")
                int_file = c3.file_uploader("Internet", type=["xlsx"], label_visibility="collapsed", key="f3")

                st.markdown('<div class="sec-label">Marca a analizar</div>', unsafe_allow_html=True)
                bn = st.text_input("Nombre principal", placeholder="Ej: Bancolombia", key="bn")
                bat = st.text_area("Alias (sep. ;)", placeholder="Ej: Grupo Bancolombia;Ban", height=70, key="ba")

                st.markdown('<div class="sec-label">Modo de análisis</div>', unsafe_allow_html=True)
                mode = st.radio("", ["Híbrido (PKL + API)","Solo Modelos PKL","API de OpenAI"],
                    index=0, key="mode")
                tpkl, epkl = None, None
                if "PKL" in mode:
                    p1,p2 = st.columns(2)
                    tpkl = p1.file_uploader("pipeline_sentimiento.pkl", type=["pkl"])
                    epkl = p2.file_uploader("pipeline_tema.pkl", type=["pkl"])

                st.markdown(f"""
                <div class="cluster-info">
                  <b>Parámetros</b><br>
                  SUBTEMA={UMBRAL_SUBTEMA} · TEMA={UMBRAL_TEMA} · MAX_TEMAS={NUM_TEMAS_MAX} ·
                  FUSIÓN={UMBRAL_FUSION_INTERGRUPO} · DEDUP={UMBRAL_DEDUP_LABEL}
                </div>""", unsafe_allow_html=True)

                if st.form_submit_button("Iniciar análisis", use_container_width=True, type="primary"):
                    if not all([df_file, reg_file, int_file, bn.strip()]):
                        st.error("Completa todos los campos y archivos.")
                    else:
                        aliases = [a.strip() for a in bat.split(";") if a.strip()]
                        asyncio.run(run_full_process_async(
                            df_file, reg_file, int_file, bn, aliases, tpkl, epkl, mode))
                        st.rerun()
        else:
            total = st.session_state.total_rows
            uniq = st.session_state.unique_rows
            dups = st.session_state.duplicates
            dur = st.session_state.process_duration
            cost = st.session_state.get("process_cost","$0.00")

            st.markdown(
                '<div class="success-banner"><div class="success-icon">✓</div>'
                '<div class="success-content"><div class="success-title">Análisis completado</div>'
                '<div class="success-sub">El informe está listo para descargar</div>'
                '</div></div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div class="metrics-grid">
              <div class="metric-card m-total"><div class="metric-val" style="color:var(--text)">{total}</div><div class="metric-lbl">Total</div></div>
              <div class="metric-card m-unique"><div class="metric-val" style="color:var(--green)">{uniq}</div><div class="metric-lbl">Únicas</div></div>
              <div class="metric-card m-dup"><div class="metric-val" style="color:var(--amber)">{dups}</div><div class="metric-lbl">Duplicados</div></div>
              <div class="metric-card m-time"><div class="metric-val" style="color:var(--blue)">{dur}</div><div class="metric-lbl">Tiempo</div></div>
              <div class="metric-card m-cost"><div class="metric-val" style="color:var(--accent)">{cost}</div><div class="metric-lbl">Costo</div></div>
            </div>""", unsafe_allow_html=True)

            if 'cache_stats' in st.session_state:
                st.caption(f"📊 {st.session_state['cache_stats']}")

            st.download_button("Descargar informe",
                data=st.session_state.output_data,
                file_name=st.session_state.output_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, type="primary")
            if st.button("Nuevo análisis", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state.password_correct = pwd
                st.rerun()

    with tab2:
        render_quick_tab()

    st.markdown('<div class="footer">v14.0.0 · Realizado por Johnathan Cortés ©</div>',
                unsafe_allow_html=True)


if __name__ == "__main__":
    main()
