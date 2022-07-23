# ITBA - Especialización en Ciencia de Datos - Almacenes de datos &amp; Procesamiento analítico en línea

## Cómo crear el Data Warehouse

Copiar los siguientes comandos en una terminal, teniendo instalado Postgres 14 y los comandos asociados (`dropdb`, `createdb`, `psql`).

```sh
dropdb dw            # Borra el warehouse si ya existe
createdb dw          # Crea el warehouse (vacío)
psql -d dw < schema.sql  # Crea el esquema del warehouse (tablas)
```

## Configuración

Antes de poblar los datos en la DB, los siguientes valores deben ser configurados en el archivo config.json:
* limpiar_db: [true / false] => Si es true, primero se borran los datos previos de la DB (recomendado)
* db => Nombre de la base de datos postgres (por defecto "dw")
* usuario_db => Nombre del usuario de la base de datos (por defecto "postgres")
* password => Password del usuario de la db. Si es false (defecto), se intentará una conexión sin password.
* fecha_desde => Fecha de inicio de la simulación, en formato dd-mm-yyyy
* fecha_hasta => Fecha de fin de la simulación, en formato dd-mm-yyyy
* hora_apertura => Hora de apertura del mercado, cada día
* hora_cierre => Hora de cierre del mercado, cada día
* ordenes_por_hora => Cantidad de órdenes a generar por hora
* precio_base_compra => Media del precio de compra (distribución normal)
* var_precio_compra => Desvío estándar del precio de compra (distribución normal)
* precio_base_venta => Media del precio de venta (distribución normal)
* var_precio_venta => Desvío estándar del precio de venta (distribución normal)
* volumen_base => Media del volumen (cantida de contratos) por órden (distribución normal)
* var_volumen => Desvío estándar de la cantidad de contratos por órden (distribución normal)

## Cómo poblar datos del DW

1. Tener Python 3.x instalado

2. Instalar las dependencias. Para ello, correr el siguiente comando:

```sh
pip3 install -r requirements.txt
```

3. Ejecutar el script

```sh
python3 seed.py
```

### Datos fijos: Cuentas, Firmas, Brokers, Operadores e Instrumentos

En el archivo "ref_data.json" se encuentran valores fijos para éstas entidades. 
El contenido puede ser modificado libremente para generar órdenes y trades para otros
participantes e instrumentos.
