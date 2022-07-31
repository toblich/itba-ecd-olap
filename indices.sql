-----------------------------------------------------------------------------------------------------------
-- INDICES POR FK: Sirven para acelearar los DELETE de todas las tablas al iniciar el simulador de trades.
-- Aparentemente el delete on cascade es muy lento sin éstos índices.
-- Si se quiere hacer rerun de una simulación que genera millones de trades, este paso es indispensable.
-----------------------------------------------------------------------------------------------------------

-- Eliminar indices anteriores
DROP INDEX IF EXISTS trades_id_orden_compra;
DROP INDEX IF EXISTS trades_id_orden_venta;
DROP INDEX IF EXISTS trades_stamp;
DROP INDEX IF EXISTS trades_ticker;

DROP INDEX IF EXISTS ordenes_id_cuenta;
DROP INDEX IF EXISTS ordenes_id_operador;
DROP INDEX IF EXISTS ordenes_ticker;
DROP INDEX IF EXISTS ordenes_timestamp_cierre;
DROP INDEX IF EXISTS ordenes_timestamp_creacion;

DROP INDEX IF EXISTS tiempo_dia_epoch;

-- Indices sobre foreign keys en: trades
CREATE INDEX trades_id_orden_compra ON trades (id_orden_compra);
CREATE INDEX trades_id_orden_venta ON trades (id_orden_venta);
CREATE INDEX trades_stamp ON trades (stamp);
CREATE INDEX trades_ticker ON trades (ticker);

-- Indices sobre foreign keys en: ordenes
CREATE INDEX ordenes_id_cuenta ON ordenes (id_cuenta);
CREATE INDEX ordenes_id_operador ON ordenes (id_operador);
CREATE INDEX ordenes_ticker ON ordenes (ticker);
CREATE INDEX ordenes_timestamp_cierre ON ordenes (timestamp_cierre);
CREATE INDEX ordenes_timestamp_creacion ON ordenes (timestamp_creacion);

-- Indices sobre foreign keys en: tiempo
CREATE INDEX tiempo_dia_epoch ON tiempo (dia_epoch);
