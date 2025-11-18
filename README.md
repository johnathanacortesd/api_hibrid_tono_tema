---

# 📰 Sistema de Análisis de Noticias con IA

Una aplicación web construida por Johnathan Cortés con **Streamlit** para el procesamiento, limpieza y clasificación avanzada de dossieres de noticias utilizando **Inteligencia Artificial (OpenAI)** y modelos de **Machine Learning** personalizados.

<img width="1817" height="785" alt="image" src="https://github.com/user-attachments/assets/f6095f3c-0bfe-4441-8061-5b2171ed693d" />

[![Abrir en Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://api-hibrid-tono-tema.streamlit.app/)

## 📜 Descripción General

Este proyecto proporciona una solución integral para analistas de medios y comunicadores que necesitan procesar grandes volúmenes de noticias (dossieres). La aplicación automatiza tareas tediosas y complejas como la detección de duplicados, la normalización de datos y, lo más importante, el **análisis de sentimiento (Tono)** y la **clasificación temática (Tema y Subtema)** de cada artículo.

Ofrece una interfaz amigable con dos flujos de trabajo principales:
1.  **Análisis Completo:** Un proceso robusto diseñado para el formato estándar de dossieres, que incluye limpieza, mapeo de datos, deduplicación y análisis profundo con múltiples modos de ejecución.
2.  **Análisis Rápido:** Una herramienta flexible para analizar rápidamente cualquier archivo Excel que contenga títulos y resúmenes, utilizando la potencia de la IA sin necesidad de una estructura de dossier fija.

## ✨ Características Principales

### 🧠 Análisis Inteligente
-   **Análisis de Tono Contextual (Sentimiento):** Clasifica cada noticia como *Positiva*, *Negativa* o *Neutra* en relación directa con la marca analizada. Utiliza un modelo híbrido avanzado que primero aplica reglas contextuales y luego, si es necesario, emplea el poder de la API de OpenAI (`gpt-4.1-nano`) para una clasificación precisa.
-   **Clasificación Temática Dinámica y Consolidada:**
    -   **Subtemas Específicos:** La IA genera subtemas detallados (3-5 palabras) para grupos de noticias similares, filtrando automáticamente el ruido (nombres de marca, ciudades, gentilicios) para mayor claridad.
    -   **Consolidación Inteligente de Subtemas:** Utiliza embeddings para identificar y unificar subtemas semánticamente idénticos (ej. "Apertura de nueva tienda" y "Inauguración de sucursal"), garantizando la consistencia del informe.
    -   **Temas Principales Sintetizados:** Consolida automáticamente los subtemas en un número manejable de temas principales, utilizando clustering y la capacidad de síntesis de la IA para nombrarlos de forma coherente y ejecutiva.
-   **Detección Avanzada de Duplicados:** Identifica noticias duplicadas con alta precisión mediante una combinación de tres métodos:
    -   🔗 **Coincidencia de URL** (para medios online).
    -   ✍️ **Similitud de Títulos Normalizados** (para noticias de agencia replicadas en diferentes medios).
    -   ⏰ **Mención + Medio + Hora** (para Radio y TV).
-   **Agrupación Eficiente:** Utiliza embeddings vectoriales (`text-embedding-3-small`) y clustering aglomerativo para agrupar noticias semánticamente similares, optimizando las llamadas a la API y mejorando la consistencia del análisis.

### ⚙️ Flexibilidad y Personalización
-   **Múltiples Modos de Análisis (en Análisis Completo):**
    1.  **🤖 API de OpenAI:** Utiliza la IA para todas las tareas de clasificación (Tono, Tema, Subtema). Es la opción más potente y no requiere modelos pre-entrenados.
    2.  **🧩 Híbrido (PKL + API) (Recomendado):** Permite usar tus propios modelos `.pkl` para Tono y/o Tema. Si no se proporciona un modelo, la IA se encarga de esa tarea. La generación de Subtemas siempre utiliza la API para máxima especificidad.
    3.  **📦 Solo Modelos PKL:** Ejecuta el análisis de Tono y Tema exclusivamente con tus modelos locales. Ideal para operar sin conexión o sin costos de API (el análisis de Subtema se omite en este modo).
-   **Mapeo y Normalización de Datos:** Limpia y estandariza datos clave como "Tipo de Medio" y enriquece las noticias con información de "Región" a partir de archivos de mapeo Excel.
-   **Manejo de Alias y Voceros:** El análisis se centra en la marca principal y en una lista configurable de alias, filiales o voceros importantes.

### 💻 Interfaz de Usuario
-   **Interfaz Web Intuitiva:** Construida con Streamlit para una experiencia de usuario sencilla y directa.
-   **Seguridad:** 🔑 Acceso protegido por contraseña.
-   **Dos Pestañas, Dos Usos:**
    -   **Análisis Completo:** Guiado paso a paso para procesar dossieres formales, con opciones de personalización avanzadas.
    -   **Análisis Rápido (IA):** Herramienta ágil para análisis exploratorios sobre cualquier archivo Excel.
-   **Post-procesamiento Interactivo:** Incluye una función para **refinar y consolidar los subtemas** del resultado final con un solo clic, aplicando la lógica de consolidación inteligente para mejorar aún más la calidad del informe.
-   **Reporte Final:** Genera un archivo Excel (`.xlsx`) limpio y formateado con todas las clasificaciones, listo para ser utilizado en informes y dashboards.

## 🛠️ Stack Tecnológico

-   **Backend:** Python 3.9+
-   **Interfaz:** Streamlit
-   **Análisis de Datos:** Pandas, NumPy
-   **IA y NLP:** OpenAI API, Unidecode
-   **Machine Learning:** Scikit-learn, Joblib
-   **Manejo de Excel:** Openpyxl

## 🚀 Instalación y Puesta en Marcha

Sigue estos pasos para ejecutar la aplicación en tu entorno local.

### 1. Prerrequisitos
-   Python 3.9 o superior.
-   Git.

### 2. Clonar el Repositorio
```bash
git clone https://github.com/tu-usuario/tu-repositorio.git
cd tu-repositorio
```

### 3. Crear un Entorno Virtual (Recomendado)
```bash
# Para macOS/Linux
python3 -m venv venv
source venv/bin/activate

# Para Windows
python -m venv venv
.\venv\Scripts\activate
```

### 4. Instalar Dependencias
El código requiere las siguientes librerías. Puedes instalarlas con pip:
```bash
pip install streamlit pandas openpyxl openai unidecode scikit-learn joblib numpy
```

### 5. Configurar las Credenciales
La aplicación utiliza un archivo `secrets.toml` para gestionar las credenciales de forma segura.

1.  Crea una carpeta `.streamlit` en la raíz de tu proyecto.
2.  Dentro de esa carpeta, crea un archivo llamado `secrets.toml`.
3.  Añade el siguiente contenido al archivo, reemplazando los valores:

```toml
# .streamlit/secrets.toml

# Clave secreta de la API de OpenAI
OPENAI_API_KEY = "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# Contraseña para acceder a la aplicación Streamlit
APP_PASSWORD = "tu_contraseña_super_secreta"
```

### 6. Ejecutar la Aplicación
Una vez configurado, inicia la aplicación con el siguiente comando:
```bash
streamlit run app.py
```
*Nota: Asegúrate de que tu archivo principal de Python se llame `app.py` o ajusta el comando.*

## 📋 Cómo Usar

### Análisis Completo

1.  **📂 Carga los archivos obligatorios:**
    -   `Dossier Principal (.xlsx)`: El archivo con las noticias a analizar.
    -   `Mapeo de Región (.xlsx)`: Un Excel con dos columnas (Medio | Región).
    -   `Mapeo Internet (.xlsx)`: Un Excel con dos columnas (URL de medio | Nombre oficial del medio).
2.  **🏢 Configura la marca:**
    -   **Marca Principal:** El nombre de la empresa, producto o entidad a analizar.
    -   **Alias y voceros:** Nombres alternativos, filiales o personas clave (separados por `;`).
3.  **⚙️ Elige el Modo de Análisis:**
    -   **API de OpenAI:** La opción más potente. Usa la IA para todas las tareas.
    -   **Híbrido (PKL + API):** Si quieres usar tus modelos `.pkl` para Tono/Tema. Aparecerán los campos para subirlos. Si no subes un modelo, la IA se encargará de esa parte.
    -   **Solo Modelos PKL:** Si quieres un análisis offline sin Subtemas. Deberás subir ambos archivos `.pkl`.
4.  **🚀 Inicia el análisis** y espera a que el proceso de 5 pasos se complete.
5.  **📥 Descarga el informe** o usa el botón **"Refinar y Consolidar Subtemas"** para mejorar aún más el resultado antes de descargar.

### Análisis Rápido (IA)

1.  Sube **cualquier archivo Excel** que contenga noticias.
2.  **✏️ Selecciona las columnas** que corresponden al **Título** y al **Resumen/Contenido**.
3.  **🏢 Configura la marca** y sus alias.
4.  **🚀 Inicia el análisis.** El sistema usará la API de OpenAI para generar Tono, Tema y Subtema.
5.  **📥 Descarga los resultados** en un nuevo archivo Excel.

### Formato de Modelos Personalizados (`.pkl`)

Si eliges usar los modos "Híbrido" o "Solo Modelos PKL", tus modelos deben cumplir con los siguientes requisitos:

-   **`pipeline_sentimiento.pkl`**: Debe ser un objeto compatible con Scikit-learn (como un `Pipeline`) que implemente un método `.predict()`. La salida de este método debe ser `1` para *Positivo*, `0` para *Neutro* y `-1` para *Negativo*.
-   **`pipeline_tema.pkl`**: Debe ser un objeto similar que implemente `.predict()`. La salida debe ser una cadena de texto (`string`) con el nombre del tema clasificado.

## 🏋️‍♂️ Entrenador de Modelos Personalizados (.pkl)

[![Abrir en Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1oe9ThUGIkGA5_QQzycErE2R530MchcFC#scrollTo=QPhTBAzd_xas)

Para aprovechar al máximo los modos **Híbrido (PKL + API)** y **Solo Modelos PKL**, puedes entrenar tus propios modelos de clasificación de Tono y Tema. He creado creado un notebook de Google Colab que simplifica este proceso en dos fases principales: preparación de datos y entrenamiento.

### Fase 1: Preparación de Datos (Concatenador Inteligente)

El primer paso para un buen modelo es tener datos de alta calidad. El notebook incluye una herramienta interactiva para unificar tus datasets de entrenamiento:

1.  **Carga Múltiple:** Sube todos tus archivos Excel (`.xlsx`) que contengan datos de entrenamiento, sin importar si tienen nombres de columna diferentes.
2.  **Mapeo Interactivo:** Para cada archivo, la herramienta te mostrará una vista previa y te permitirá asignar tus columnas a los campos estándar: `TÍTULO`, `RESUMEN`, `TONO` y `TEMA`.
3.  **Unificación Automática:** El script concatenará todos los archivos mapeados en un único dataset limpio (`archivo_unificado.xlsx`), combinando `TÍTULO` y `RESUMEN` en una sola columna de texto y estandarizando las columnas de `TONO` y `TEMA`.
4.  **Opción de Omitir:** Si subes un archivo por error o no es relevante, puedes omitirlo fácilmente durante el proceso de mapeo.

### Fase 2: Entrenamiento de Modelos de Clasificación

Una vez que tienes tu `archivo_unificado.xlsx`, la segunda parte del notebook se encarga del entrenamiento:

1.  **Carga del Dataset:** Sube el archivo unificado generado en la fase anterior.
2.  **Preprocesamiento Automático:** El texto de la columna `resumen` se limpia y normaliza automáticamente (minúsculas, eliminación de stopwords, caracteres especiales, etc.) para optimizar el rendimiento del modelo.
3.  **Selección de Entrenamiento:** Puedes elegir entrenar solo el modelo de **Sentimiento (Tono)**, solo el de **Tema**, o **ambos** a la vez.
4.  **Entrenamiento y Selección del Mejor Modelo:** El script entrena y evalúa varios algoritmos de Machine Learning (como Logistic Regression, LinearSVC, RandomForest) para cada tarea. Automáticamente selecciona el modelo con el mejor rendimiento (`accuracy`) y lo prepara para la exportación.
5.  **Descarga Final:** Al finalizar, el notebook guardará los modelos finales como `pipeline_sentimiento.pkl` y `pipeline_tema.pkl` y activará su descarga a tu ordenador.

Estos archivos `.pkl` están listos para ser utilizados directamente en la aplicación Streamlit, dándote el poder de clasificar noticias con modelos entrenados específicamente para tus datos y tu contexto.

---
<div align="center">
    <p>Realizado por Johnathan Cortés</p>
</div>
