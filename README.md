# 📰 Sistema de Análisis de Noticias con IA

Una aplicación web construida con **Streamlit** para el procesamiento, limpieza y clasificación avanzada de dossieres de noticias utilizando **Inteligencia Artificial (OpenAI)** y modelos de **Machine Learning** personalizados.

![Streamlit App Screenshot](https://raw.githubusercontent.com/username/repo/main/screenshot.png) <!-- ⚠️ Reemplaza esta URL con una captura de pantalla real de tu app -->



## 📜 Descripción General

Este proyecto proporciona una solución integral para analistas de medios y comunicadores que necesitan procesar grandes volúmenes de noticias (dossieres). La aplicación automatiza tareas tediosas y complejas como la detección de duplicados, la normalización de datos y, lo más importante, el **análisis de sentimiento (Tono)** y la **clasificación temática (Tema y Subtema)** de cada artículo.

Ofrece una interfaz amigable con dos flujos de trabajo principales:
1.  **Análisis Completo:** Un proceso robusto diseñado para el formato estándar de dossieres, que incluye limpieza, mapeo de datos, deduplicación y análisis profundo.
2.  **Análisis Rápido:** Una herramienta flexible para analizar rápidamente cualquier archivo Excel que contenga títulos y resúmenes, sin necesidad de una estructura de dossier fija.

## ✨ Características Principales

### 🧠 Análisis Inteligente
-   **Análisis de Tono (Sentimiento):** Clasifica cada noticia como *Positiva*, *Negativa* o *Neutra* en relación con una marca específica, utilizando un modelo híbrido de reglas, embeddings y el poder de la API de OpenAI (GPT-4-Nano).
-   **Clasificación Temática Dinámica:**
    -   **Subtemas:** La IA genera subtemas específicos y detallados (2-6 palabras) para grupos de noticias similares, eliminando el ruido (nombres de marca, ciudades).
    -   **Temas Principales:** Consolida automáticamente los subtemas en un número manejable de temas principales, utilizando clustering de embeddings y la capacidad de síntesis de la IA para nombrarlos de forma coherente.
-   **Detección Avanzada de Duplicados:** Identifica noticias duplicadas con alta precisión mediante una combinación de:
    -   🔗 Coincidencia de URL (para medios online).
    -   ✍️ Similitud de títulos normalizados (para noticias de agencia replicadas).
    -   ⏰ Coincidencia de Mención + Medio + Hora (para Radio y TV).
-   **Agrupación Eficiente:** Utiliza embeddings vectoriales (`text-embedding-3-small`) y clustering aglomerativo para agrupar noticias semánticamente similares, optimizando las llamadas a la API y mejorando la consistencia del análisis.

### ⚙️ Flexibilidad y Personalización
-   **Múltiples Modos de Análisis (en Análisis Completo):**
    1.  **🤖 API de OpenAI (Recomendado):** Utiliza la IA para todas las tareas de clasificación (Tono, Tema, Subtema). No requiere modelos pre-entrenados.
    2.  **🧩 Híbrido (PKL + API):** Permite usar tus propios modelos `.pkl` para Tono y/o Tema, mientras la IA se encarga de generar los Subtemas.
    3.  **📦 Solo Modelos PKL:** Ejecuta el análisis de Tono y Tema exclusivamente con tus modelos locales, ideal para operar sin conexión o sin costos de API (el análisis de Subtema se omite).
-   **Mapeo y Normalización de Datos:** Limpia y estandariza datos clave como "Tipo de Medio" y enriquece las noticias con información de "Región" a partir de archivos de mapeo Excel.
-   **Manejo de Alias y Voceros:** El análisis se centra en la marca principal y en una lista configurable de alias, filiales o voceros importantes.

### 💻 Interfaz de Usuario
-   **Interfaz Web Intuitiva:** Construida con Streamlit para una experiencia de usuario sencilla y directa.
-   **Seguridad:** 🔑 Acceso protegido por contraseña.
-   **Dos Pestañas, Dos Usos:**
    -   **Análisis Completo:** Guiado paso a paso para procesar dossieres formales.
    -   **Análisis Rápido:** Herramienta ágil para análisis exploratorios sobre cualquier Excel.
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
    -   `Mapeo de Región (.xlsx)`: Un Excel con dos columnas (Medio | Región) para asignar regiones.
    -   `Mapeo Internet (.xlsx)`: Un Excel con dos columnas (URL de medio | Nombre oficial del medio) para normalizar medios online.
2.  **🏢 Configura la marca:**
    -   **Marca Principal:** El nombre de la empresa, producto o entidad a analizar.
    -   **Alias y voceros:** Nombres alternativos, filiales o personas clave (separados por `;`).
3.  **⚙️ Elige el Modo de Análisis:**
    -   **API de OpenAI:** La opción por defecto y más potente.
    -   **Híbrido:** Si quieres usar tus modelos `.pkl` para Tono/Tema. Aparecerán los campos para subirlos.
    -   **Solo Modelos PKL:** Si quieres un análisis offline sin Subtemas. Deberás subir ambos archivos `.pkl`.
4.  **🚀 Inicia el análisis** y espera a que el proceso de 5 pasos se complete.
5.  **📥 Descarga el informe** en formato Excel.

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

---
<div align="center">
    <p>Realizado por Johnathan Cortés</p>
</div>
