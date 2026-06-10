# **Definición del Caso**

Una empresa de telecomunicaciones opera en múltiples países y recopila datos de ubicación diarios de millones de clientes a través de su red móvil. El equipo de inteligencia de negocios necesita comprender la proximidad de los clientes a puntos de interés (POIs) etiquetados con tipos de eventos específicos, para apoyar campañas de marketing dirigidas, planificación de capacidad de red y segmentación de clientes.

Están disponibles dos conjuntos de datos: una tabla geográfica con coordenadas de ubicación de clientes y comuna, y una tabla de etiquetas con clientes vinculados a tipos de eventos específicos (tipo 1 y tipo 2). Ambos conjuntos de datos se generan diariamente. Como parte de su solución, debe definir cómo y dónde se ingestará y almacenará estos datos, en qué formato y cómo se particionará para soportar consultas históricas eficientes.

El desafío analítico principal es identificar, para cada cliente, qué eventos etiquetados ocurren dentro de un radio de 50 metros. Dado el volumen de datos (millones de registros por día), un enfoque ingenuo generaría un número inmanejable de combinaciones. Su solución debe abordar este problema de escalabilidad.

El resultado procesado debe servir a dos audiencias: Científicos de Datos que trabajan en cuadernos Jupyter y requieren una tabla de características rica, y Analistas de Negocios que consultan datos a través de SQL. Su arquitectura y esquema de salida deben considerar ambos patrones de acceso.

**Debe enviar su solución dentro de los 7 días calendario posteriores a recibir esta prueba. Envíe todos los entregables (diagrama de arquitectura, script PySpark y cuaderno completado) como un solo archivo comprimido o enlace de repositorio compartido.**

---

## **Punto 1: (Arquitectura)**

Antes de definir los componentes técnicos, escriba un breve documento de planificación que cubra: (1) los principales riesgos y restricciones que identificó en el caso, (2) sus principios de diseño (por ejemplo, costo, escalabilidad, simplicidad) y (3) por qué eligió AWS, GCP o un enfoque híbrido. Luego, basándose en ese plan, defina los siguientes componentes de su arquitectura. El diseño debe soportar la adición de fuentes de datos similares en versiones futuras:

* **Herramientas y capas de almacenamiento (Raw, Staging, Analytics, Curated):** defina la ubicación de almacenamiento para cada capa, el formato de archivo y la estrategia de particionado que soporte la ingestión diaria y consultas históricas eficientes.
* **Herramientas para transformar los datos** (elija y justifique su selección; las opciones incluyen: AWS Glue, EMR + Apache Spark, Databricks, Google Dataflow, Dataproc. Si elige Databricks o Snowflake, explique cómo se adapta al requisito de rentabilidad).
* **Defina la herramienta de orquestación** (por ejemplo, Apache Airflow, AWS Step Functions, Prefect, Dagster) y justifique por qué se adapta a esta canalización.
* **Defina la herramienta de notificación y alertas** (por ejemplo, callbacks de Airflow, AWS SNS, PagerDuty) y describa qué condiciones deben activar una alerta.
* **Defina la herramienta para que los Científicos de Datos accedan al resultado** (por ejemplo, SageMaker, AI Platform, cuadernos Databricks, EC2 con JupyterHub) y explique cómo consultarán los datos.
* **Defina la herramienta para que los Analistas de Negocios consulten los datos vía SQL** (por ejemplo, Amazon Athena, Redshift, BigQuery, Snowflake, Databricks SQL, Hive) y explique cómo se expondrá la tabla de resultados a ellos.
* **Defina el formato de almacenamiento para cada capa** (por ejemplo, Parquet, Delta Lake, Apache Iceberg) y justifique la elección en términos de rendimiento de consultas y costo.

---

## **Punto 2: (Script ETL)**

Desarrolle un script en PySpark, Spark o Scala para procesar la muestra (aprox. 500K) proporcionada para este ejercicio; recuerde crear un script lo más eficiente posible porque debe funcionar con datos completos (millones de registros).

**Los datos de muestra están disponibles en un Google Drive compartido, en formato de archivos parquet en dos carpetas y estructuras diferentes: geo y etiquetas (labels). Las siguientes imágenes muestran las estructuras de tablas y un ejemplo de código para importar los datos en un entorno de Google Colab.**

Considere los siguientes puntos para desarrollar el script. Cada paso debe implementarse en orden:

* El proceso ETL puede desarrollarse en el cuaderno de Colab proporcionado como plantilla, pero se requiere un script independiente como entrega final.
* **Perfileo y limpieza de datos:** antes de cualquier transformación, inspeccione ambas tablas y documente sus hallazgos. Esto debe incluir: (a) conteo de filas y validación de esquema para cada tabla; (b) detección y manejo de valores NULL en campos clave (ID, comuna, evento, latitud, longitud); (c) detección de valores de coordenadas fuera de rango o anómalos; (d) identificación de IDs duplicados y su estrategia de deduplicación elegida con justificación. Agregue un breve bloque de comentarios resumiendo lo que encontró y qué hizo al respecto.
* **Unir los conjuntos de datos:** una vez que los datos estén limpios, una las etiquetas y la geolocalización a través del campo ID. Documente el tipo de unión elegido (inner, left, etc.) y justifíquelo basándose en sus hallazgos de perfileo, particularmente la superposición de ID entre tablas.
* **Deduplicación:** como parte del paso de limpieza, elimine IDs duplicados. Para la tabla geográfica, mantenga el primer valor de comuna, latitud y longitud. Para la tabla de etiquetas, agregue el campo de evento. Explique en un comentario por qué eligió "primero" y si otras estrategias (por ejemplo, la más frecuente, la más reciente) serían más apropiadas para este caso de uso.
* **Calcule la distancia** (puede ser Euclidiana, Manhattan o cualquier otro método) entre todos los clientes dentro de un radio de 50 metros.
* **Defina e implemente una estrategia de optimización** para manejar la explosión combinatoria del cálculo de distancias. En un breve bloque de comentarios (5-10 líneas) dentro de su script, explique: (1) qué estrategia eligió, (2) por qué es apropiada para estos datos y (3) cuáles son las compensaciones en comparación con al menos un enfoque alternativo.
* **Defina el esquema de la tabla de salida**, que puede ser diferente para científicos de datos y analistas de negocios dependiendo de la arquitectura que defina en el punto 1.
* **Guarde el resultado del punto anterior** pensando en tener un historial semanal en un archivo con el formato indicado.

---

## **Punto 3: (Analítica y Calidad de Datos)**

Conecte el cuaderno de Colab a una base de datos SQLite para validar la calidad de los datos y poner los resultados a disposición de los Científicos de Datos y los Analistas de Negocios. Para cada consulta a continuación, comente brevemente qué pregunta de negocio resuelve y qué problema de calidad de datos podría revelar.

* **Cargue la tabla de etiquetas y la tabla de resultados del Punto 2 en SQLite** (el cuaderno contiene un ejemplo con la tabla geográfica).
* **Verificaciones de Calidad de Datos:** escriba consultas para (a) contar filas con valores NULL en cualquier campo clave (ID, comuna, evento); (b) detectar IDs duplicados después de la deduplicación; (c) encontrar clientes cuyas coordenadas caen fuera del rango esperado para su comuna (use una subconsulta para calcular el cuadro delimitador de la comuna y luego marque los valores atípicos).
* **Usando una función de ventana (RANK o DENSE_RANK)**, clasifique las comunas por el número total de eventos tipo 2 y devuelva las 20 principales. Incluya una segunda columna que muestre la proporción (%) de cada comuna del total nacional de eventos tipo 2 (use una subconsulta o CTE para el total nacional).
* **Usando un CTE**, calcule para eventos tipo 1 por comuna: promedio, máximo y mínimo de latitud y longitud, el conteo de eventos y la distancia entre los puntos de latitud máxima y mínima. Luego, en la consulta externa, devuelva solo las comunas donde la latitud promedio se desvía más del 10% del promedio nacional general (esto prueba la conciencia de valores atípicos geográficos).
* **Para cada comuna**, use una sola consulta con agregación condicional (sin JOINs) para mostrar lado a lado: conteo de eventos tipo 1, conteo de eventos tipo 2 y la ratio tipo_2 / tipo_1. Ordene por ratio descendente. Esto simula una solicitud típica de un analista de negocios sobre la tabla de resultados final.
