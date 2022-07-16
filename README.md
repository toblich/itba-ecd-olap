# ITBA - Especialización en Ciencia de Datos - Almacenes de datos &amp; Procesamiento analítico en línea

## Cómo crear el Data Warehouse

Copiar los siguientes comandos en una terminal, teniendo instalado Postgres 14 y los comandos asociados (`dropdb`, `createdb`, `psql`).

```sh
dropdb dw            # Borra el warehouse si ya existe
createdb dw          # Crea el warehouse (vacío)
psql -d dw < schema.sql  # Crea el esquema del warehouse (tablas)
```
