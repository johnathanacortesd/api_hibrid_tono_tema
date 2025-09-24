# Analizador de Noticias con IA 📰

Una aplicación web avanzada construida con **Streamlit**, diseñada para analizar y clasificar grandes volúmenes de noticias en español 🇪🇸🇨🇴. Esta herramienta utiliza un modelo híbrido que combina reglas de negocio, modelos de Machine Learning pre-entrenados y la potencia de la API de OpenAI (`gpt-4.1-nano`) para ofrecer un análisis detallado de tono, tema y subtema, adaptado a las necesidades de diferentes clientes.

---

## ✨ Características Principales

*   **🧠 Análisis Híbrido de Tono y Tema:**
    *   **Modo PKL:** Permite cargar modelos `.pkl` personalizados (`pipeline_sentimiento.pkl`, `pipeline_tema.pkl`) para un análisis rápido, consistente y específico por cliente.
    *   **Modo IA:** Si no se proporcionan modelos, utiliza un sistema robusto de reglas contextuales y la API de OpenAI para una clasificación inteligente y adaptable.

*   **ιε Jerarquía de Clasificación Tema/Subtema:**
    *   **Subtema (Generado por IA):** Siempre se genera una etiqueta granular y específica que describe el núcleo de cada noticia o grupo de noticias similares.
    *   **Tema (Generado por IA o PKL):** Agrupa los subtemas en 25 categorías principales de alto nivel, ideales para reportes ejecutivos. El prompt ha sido optimizado para generar temas descriptivos y evitar la generalidad.

*   **🎯 Detección Avanzada de Duplicados:**
    *   **Internet/Online:** Identifica duplicados por combinación de **URL + Mención** (prioridad 1) o por **Título Similar + Medio + Mención** (prioridad 2).
    *   **Radio y Televisión:** Detecta duplicados por la combinación estricta de **Medio + Hora + Mención**.

*   **⚙️ Mapeo y Normalización de Datos:**
    *   **Regiones:** Asigna automáticamente la región a cada medio utilizando un archivo de mapeo externo.
    *   **Tipos de Medio:** Estandariza las categorías (ej. "AM", "FM" -> "Radio"; "Cable", "Aire" -> "Televisión").
    *   **Nombres de Medios:** Normaliza los nombres de medios de internet para una agrupación consistente (ej. `elespectador.com` -> `El Espectador`).

*   **📊 Reportes y Visualización:**
    *   Genera un informe detallado en formato `.xlsx` con todas las columnas originales y las nuevas clasificaciones.
    *   Maneja correctamente los hipervínculos y el formato de texto para una presentación limpia.
    *   Muestra un resumen visual de los resultados en la interfaz al finalizar el proceso.

*   **🔐 Acceso Seguro:**
    *   La aplicación está protegida por una contraseña que se gestiona a través de los secretos de Streamlit.

---

## 🛠️ Stack Tecnológico

*   **🐍 Python 3.10+**
*   **🎈 Streamlit** - Para la interfaz web interactiva.
*   **🧠 OpenAI API** - Para la generación de Tono, Tema y Subtema.
*   **🐼 Pandas** - Para la manipulación de datos.
*   **📊 Scikit-learn & Joblib** - Para cargar y utilizar los modelos `.pkl` personalizados.
*   **📄 Openpyxl** - Para la lectura y escritura de archivos Excel.

---

## 🚀 Puesta en Marcha

Sigue estos pasos para ejecutar la aplicación en tu entorno local.

### 1. Prerrequisitos

*   Tener instalado Python 3.10 o superior.
*   Tener `git` instalado para clonar el repositorio.

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
venv\Scripts\activate
```

### 4. Instalar Dependencias

Asegúrate de tener el archivo `requirements.txt` en la raíz del proyecto.

```bash
pip install -r requirements.txt
```

### 5. Configurar los Secretos

Crea una carpeta `.streamlit` en la raíz del proyecto y dentro de ella, un archivo llamado `secrets.toml`. Este archivo contendrá tus credenciales y no debe ser subido a GitHub.

```toml
# .streamlit/secrets.toml

# Tu clave de la API de OpenAI
OPENAI_API_KEY = "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# La contraseña para acceder a la aplicación
APP_PASSWORD = "tu_contraseña_super_secreta"
```

### 6. Ejecutar la Aplicación

```bash
streamlit run app.py
```

---

## 📖 Guía de Uso

Una vez que la aplicación esté en funcionamiento, sigue estos pasos:

1.  **Cargar Archivos Obligatorios:**
    *   **Dossier Principal (.xlsx):** El archivo Excel con el listado de noticias a analizar. Debe contener columnas como `Titulo`, `Resumen - Aclaracion`, `Menciones - Empresa`, `Medio`, `Tipo de Medio`, `Hora`, y `Link Nota`.
    *   **Mapeo de Región (.xlsx):** Un Excel con dos columnas: la primera con los nombres de los medios (tal como aparecen en el dossier) y la segunda con la región correspondiente.
    *   **Mapeo de Internet (.xlsx):** Un Excel con dos columnas: la primera con el nombre del medio online (ej. `portafolio.co`) y la segunda con el nombre normalizado (ej. `Portafolio`).

2.  **Configurar la Marca:**
    *   **Marca Principal:** El nombre oficial del cliente.
    *   **Alias y voceros:** Nombres alternativos, siglas o personas clave asociadas a la marca, separados por punto y coma (`;`).

3.  **(Opcional) Cargar Modelos Personalizados:**
    *   Si tienes modelos pre-entrenados, puedes subirlos en la sección "Opcional":
        *   `pipeline_sentimiento.pkl`: Para clasificar el **Tono**.
        *   `pipeline_tema.pkl`: Para clasificar el **Tema** principal.
    *   Recuerda: El **Subtema** siempre será generado por la IA para mantener la granularidad.

4.  **Iniciar Análisis:**
    *   Haz clic en "INICIAR ANÁLISIS COMPLETO" y espera a que el proceso finalice.

5.  **Descargar Resultados:**
    *   Una vez completado, aparecerá un botón para descargar el informe final en formato `.xlsx`.

---

## 📁 Estructura del Proyecto

```
.
├── .streamlit/
│   └── secrets.toml    # Archivo de credenciales (NO subir a Git)
├── app.py              # El corazón de la aplicación Streamlit
├── requirements.txt    # Lista de dependencias de Python
└── README.md           # Este archivo
```

---

## ✍️ Autor

Desarrollado con 🤖 por **Johnathan Cortés**.
