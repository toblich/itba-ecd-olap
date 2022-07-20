# ITBA - Especialización en Ciencia de Datos - Almacenes de datos &amp; Procesamiento analítico en línea

## Cómo crear el Data Warehouse

Copiar los siguientes comandos en una terminal, teniendo instalado Postgres 14 y los comandos asociados (`dropdb`, `createdb`, `psql`).

```sh
dropdb dw            # Borra el warehouse si ya existe
createdb dw          # Crea el warehouse (vacío)
psql -d dw < schema.sql  # Crea el esquema del warehouse (tablas)
```

## Cómo poblar datos del DW

1. Tener Python 3.x instalado

1. Instalar las dependencias. Para ello, correr el siguiente comando:

```sh
pip3 install -r requirements.txt
```

1. Ejecutar el script

```sh
python3 seed.py
```
