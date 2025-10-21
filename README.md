* 📰 Sistema de Análisis de Noticias con IA (v5.2.1) *
Este proyecto es una aplicación web construida con Streamlit que automatiza el análisis de dossieres de noticias. Utiliza un enfoque híbrido de reglas heurísticas avanzadas y modelos de lenguaje grande (LLMs) a través de la API de OpenAI para realizar tareas complejas de procesamiento de lenguaje natural (NLP) sobre textos en español.
La aplicación está diseñada para ser intuitiva y robusta, ofreciendo dos flujos de trabajo principales: un Análisis Completo para dossieres estructurados y un Análisis Rápido para archivos Excel genéricos.

[![Abrir en Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://api-hibrid-tono-tema.streamlit.app/)

<img width="1802" height="852" alt="Captura app" src="https://github.com/user-attachments/assets/36fb15a8-ee42-4048-bd7e-ddb8c978e50e" />

(Nota: Reemplaza el enlace anterior con una captura de pantalla real de tu aplicación para un README más visual).
* ✨ Características Principales *
* 1. Análisis Completo de Dossieres *
Este es el flujo de trabajo principal, diseñado para procesar un conjunto de archivos estructurados y generar un informe unificado y enriquecido.
Limpieza y Deduplicación Inteligente:
Divide automáticamente filas con múltiples menciones en noticias individuales.
Identifica y marca noticias duplicadas basándose en la URL (para medios online), la combinación de medio y hora (para radio/TV), y la similitud de títulos (usando SequenceMatcher).
Normalización y Mapeo de Datos:
Estandariza los "Tipos de Medio" (ej: 'fm', 'diario' se convierten en 'Radio', 'Prensa').
Enriquece los datos mapeando medios a sus respectivas regiones y normalizando los nombres de medios de internet usando archivos Excel de referencia.
Análisis de Tono Híbrido (Reglas + IA):
Motor de Reglas Prioritarias: Un sistema robusto que clasifica automáticamente la mayoría de los casos sin consultar a la IA:
⭐ Positivo (Atribución de Experto): Clasifica como Positivo si un vocero o experto de la marca es citado. La regla detecta patrones comunes como "[Cargo] de [Marca]" (ej: "Gerente de Producto en Siemens") o "[Verbo de cita]... [Marca]" (ej: "señala el director de Siemens"). Esta regla soluciona los errores de clasificación de citas neutras.
Positivo: Si la marca responde activamente a una crisis (ej: "activa plan de contingencia ante inundación").
Positivo/Negativo: Basado en la co-ocurrencia de la marca con un léxico curado de palabras clave de acción positiva (lanzamiento, alianza, crecimiento) o negativa (demanda, caída, problema).
Refuerzo con IA (OpenAI): Solo las noticias ambiguas que no son capturadas por las reglas se envían al modelo gpt-4.1-nano-2025-04-14 para una clasificación de tono contextual, asegurando eficiencia y precisión.
Generación Dinámica de Temas y Subtemas (IA):
Agrupa noticias semánticamente similares usando embeddings vectoriales (text-embedding-3-small).
Subtemas: El modelo gpt-4.1-nano-2025-04-14 genera un subtema específico y conciso (2-6 palabras) para cada grupo de noticias.
Temas Principales: Consolida los subtemas en un número definido de temas de alto nivel. Para ello, agrupa los subtemas mediante clustering de embeddings y utiliza nuevamente la IA para nombrar las categorías resultantes de forma coherente y útil para informes.
Generación de Informe Excel:
Produce un archivo .xlsx final con todas las columnas originales y las nuevas columnas de análisis (Tono IAI, Tema, Subtema).
La columna Justificacion Tono ha sido eliminada para un informe más limpio.
Limpia títulos, corrige texto de resúmenes y preserva los hipervínculos.
* 2. Análisis Rápido (IA) *
Una herramienta flexible para analizar rápidamente cualquier archivo Excel que contenga texto.
Interfaz Guiada en 2 Pasos: El usuario primero sube un archivo. Una vez cargado, la aplicación muestra las columnas del archivo y le pide que seleccione cuáles corresponden al Título y al Resumen.
Análisis Focalizado: Aplica los mismos potentes módulos de Análisis de Tono Híbrido y Generación Dinámica de Temas/Subtemas del flujo completo.
Resultado Inmediato: Añade las columnas Tono IAI, Tema y Subtema al archivo original y permite la descarga inmediata del resultado.
* 3. Autenticación y Personalización *
Acceso Seguro: Protegido por una contraseña simple configurada a través de los Secrets de Streamlit.
Soporte para Modelos Personalizados (.pkl): Permite a usuarios avanzados subir sus propios modelos scikit-learn para Tono y Tema, reemplazando el análisis de IA por defecto en el flujo de "Análisis Completo".
* 🛠️ Tecnologías Utilizadas *
Framework Web: Streamlit
Análisis de Datos: Pandas, NumPy
Procesamiento de Excel: Openpyxl
Inteligencia Artificial (NLP):
API de OpenAI para generación de embeddings, clasificación de tono y generación de temas.
Modelos Utilizados: gpt-4.1-nano-2025-04-14 (para clasificación y generación de texto corto) y text-embedding-3-small (para embeddings semánticos).
Machine Learning: Scikit-learn para clustering y para cargar modelos .pkl personalizados.
Utilidades: unidecode para normalización de texto, asyncio para peticiones concurrentes a la API.
* 🚀 Cómo Desplegar en Streamlit Cloud *
Fork/Clona este Repositorio: Asegúrate de tener el código en tu propia cuenta de GitHub.
Crea el archivo requirements.txt: Este archivo es crucial. Debe estar en la raíz de tu repositorio y contener todas las dependencias.
code
Text
streamlit
pandas
openpyxl
openai==0.28.0
scikit-learn
unidecode
joblib
numpy
Configura los Secrets en Streamlit Cloud:
Ve a tu aplicación en Streamlit Cloud.
Haz clic en "Manage app" -> "Settings" -> "Secrets".
Añade dos secrets:
OPENAI_API_KEY: Tu clave secreta de la API de OpenAI.
code
Toml
OPENAI_API_KEY="sk-..."
APP_PASSWORD: La contraseña que desees para proteger el acceso a la aplicación.
code
Toml
APP_PASSWORD="tu_contraseña_secreta"
Despliega:
En tu dashboard de Streamlit Cloud, haz clic en "New app".
Selecciona tu repositorio y la rama principal.
Asegúrate de que el archivo principal es app.py (o como lo hayas llamado).
¡Haz clic en "Deploy!"! Streamlit instalará las dependencias de requirements.txt y lanzará la aplicación.
* 📁 Estructura de Archivos de Entrada (Para Análisis Completo) *
Dossier Principal (.xlsx): El archivo principal con las noticias. Debe tener columnas como "Titulo", "Resumen - Aclaracion", "Medio", "Tipo de Medio", etc.
Mapeo de Región (.xlsx): Un archivo con dos columnas: la primera con el nombre del medio (como aparece en el dossier) y la segunda con la región correspondiente.
Mapeo Internet (.xlsx): Un archivo con dos columnas: la primera con el nombre del medio de internet y la segunda con el nombre normalizado.
Realizado por Johnathan Cortés.
