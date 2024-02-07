# Nyctibius - Simplificación de la integración de datos sociodemográficos.


El paquete de Python Nyctibius está diseñado para simplificar la tarea compleja de recopilar y consolidar datos socio-demográficos de diversas fuentes en una base de datos relacional cohesiva. Nyctibius permite a los usuarios unificar sin esfuerzo conjuntos de datos personalizados de diversas fuentes socio-demográficas, asegurando que puedan trabajar con información actualizada y completa de manera fluida. Este paquete facilita la creación de un repositorio armonizado de datos socio-demográficos, simplificando la gestión y análisis de datos para usuarios en diversos sectores.

## Características

- Recupera datos de fuentes en lines de manera eficiente mediante web scraping.
- Extrae datos de diversas fuentes y los consolida en una base de datos relacional unificada sin esfuerzo.
- Realiza consultas precisas y aplica transformaciones según criterios específicos.
- Maneja eficazmente inconsistencias y discrepancias en los datos para una mayor precisión.
- Admite diversos formatos de datos, como .csv, .xlsx, .xls, .txt y archivos zip, garantizando versatilidad en la obtención de información.

## Instalación

Puedes instalar la biblioteca Harmonize Toolkit utilizando `pip`. Asegúrate de tener instalado Python 3.x en tu sistema.

```shell
pip install nyctibius
```

## Uso

Para utilizar la biblioteca Harmonize Toolkit, sigue estos pasos:

1. Importe el paquete en su script Python:

   ```python
   from nyctibius import Harmonizer
   ```

2. Crea una instancia de la clase `Harmonizer`:

   ```python
   harmonizer = Harmonizer()
   ```

3. Extraer datos de fuentes en línea y crear una lista de información de datos:

   ```python
   url = 'https://www.example.com'
   depth = 0
   ext = 'csv'
   list_datainfo = harmonizer.extract(url=url, depth=depth, ext=ext)
   harmonizer = Harmonizer(list_datainfo)
   ```

4. Cargar los datos de la lista de información de datos y fusionarlos en una base de datos relacional:

   ```python
   results = harmonizer.load()
   ```

5. Importa el módulo querier y crea una instancia de la clase `Querier`:

   ```python
   from nyctibius.sql.querier import Querier
   querier = Querier()
   ```
   
6. Realizar consultas y transformaciones:

   ```python
   querier.execute_query('SELECT * FROM table_name')
   querier.get_tables()
   querier.get_columns('table_name')
   querier.rename_table("table_name", "new_table_name")
   querier.rename_column("table_name", "column_name", "new_column_name")
   querier.rename_table_columns("table_name", ['column_1', 'column_2', 'column_3', ...]))
   querier.set_primary_key("table_name", "column_name")
   querier.set_foreign_key("table_name", "fk_column_name", "referenced_table_name", "referenced_column_name")
   ```

## Fuentes de Datos Soportadas

La biblioteca Harmonize Toolkit admite las siguientes organizaciones oficiales:

- Links de microdata del Departamento Administrativo Nacional de Estadística (DANE)
- Archivos locales
- Datos Abiertos

Ten en cuenta que acceder a los datos de estas organizaciones puede requerir autenticación o credenciales específicas. Asegúrate de tener los permisos necesarios antes de usar la biblioteca.

## Licencia

El paquete Nyctibius es de código abierto y se distribuye bajo la [Licencia MIT](https://opensource.org/licenses/MIT). Siéntete libre de usar, modificar y distribuir esta biblioteca de acuerdo con los términos de la licencia.

## Reconocimientos

Agradecemos a las siguientes entidades el haber facilitado los datos utilizados y el apoyo económico para el desarrollo de este paquete:

- Departamento Administrativo Nacional de Estadística (DANE)
- Centro Nacional de Supercomputación (BSC)
- Universidad de los Andes

## Contacto

Para cualquier pregunta, sugerencia o comentario sobre el paquete, por favor, ponte en contacto con:

Erick lozano 
Email: es.lozano@uniandes.edu.co

Diego Irreño
Email: dirreno@unal.edu.co

## Descargo de responsabilidad

Esta biblioteca no está oficialmente afiliada ni respaldada por ninguna de las organizaciones oficiales mencionadas. Los datos proporcionados por esta biblioteca se obtienen de información disponible públicamente y es posible que no siempre reflejen los datos más actuales o precisos. Verifica la información con las respectivas fuentes oficiales para casos de uso críticos.