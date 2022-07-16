CREATE TYPE tipo_derivado AS ENUM('futuro', 'call', 'put');
CREATE TABLE instrumentos (
  ticker TEXT PRIMARY KEY,
  nombre_producto TEXT NOT NULL,
  tipo_producto TEXT NOT NULL,
  tipo_derivado tipo_derivado NOT NULL,
  vencimiento DATE NOT NULL,
  precio_ejercicio NUMERIC,
  intervalo_settlement INTERVAL NOT NULL
);

CREATE TYPE dia_semana AS ENUM('lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo');
CREATE TYPE mes AS ENUM('enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre');
CREATE TABLE fechas (
  dia_epoch INT PRIMARY KEY,
  numero_dia INT NOT NULL,
  nombre_dia dia_semana NOT NULL,
  numero_semana INT NOT NULL,
  numero_mes INT NOT NULL,
  nombre_mes mes NOT NULL,
  año INT NOT NULL,
  es_finde BOOLEAN NOT NULL,
  es_feriado BOOLEAN NOT NULL
);

CREATE TABLE tiempo (
  nano_epoch BIGINT PRIMARY KEY,
  dia_epoch INT REFERENCES fechas(dia_epoch) NOT NULL,
  nanosegundos INT NOT NULL,
  segundo INT NOT NULL,
  minuto INT NOT NULL,
  hora INT NOT NULL
);

CREATE TABLE operadores (
  id INT PRIMARY KEY,
  nombre TEXT NOT NULL
);

CREATE TABLE brokers (
  id INT PRIMARY KEY,
  nombre TEXT NOT NULL
);

CREATE TABLE firmas (
  id INT PRIMARY KEY,
  nombre TEXT NOT NULL
);

CREATE TABLE cuentas (
  id INT PRIMARY KEY,
  id_firma INT REFERENCES firmas(id) NOT NULL,
  id_broker INT REFERENCES brokers(id) NOT NULL,
  nombre TEXT NOT NULL
);

CREATE TYPE lado AS ENUM('compra', 'venta');
CREATE TYPE estado_orden AS ENUM('pendiente', 'parcialmente ejecutada', 'ejecutada', 'parcialmente cancelada', 'cancelada', 'rechazada');
CREATE TABLE ordenes (
  id BIGINT PRIMARY KEY,
  ticker TEXT REFERENCES instrumentos(ticker) NOT NULL,
  timestamp_creacion BIGINT REFERENCES tiempo(nano_epoch) NOT NULL,
  timestamp_cierre BIGINT REFERENCES tiempo(nano_epoch),
  id_cuenta INT REFERENCES cuentas(id) NOT NULL,
  id_operador INT REFERENCES operadores(id) NOT NULL,
  lado lado NOT NULL,
  precio_stop NUMERIC,
  precio_limit NUMERIC,
  estado estado_orden NOT NULL,
  es_agresiva BOOLEAN NOT NULL,
  cantidad_contratos INT NOT NULL
);

CREATE TYPE periodo_ejecucion AS ENUM('opening', 'pre-settlement', 'settlement');
CREATE TABLE trades (
  id BIGINT PRIMARY KEY,
  ticker TEXT REFERENCES instrumentos(ticker) NOT NULL, -- Esto es redundate acá!
  stamp BIGINT REFERENCES tiempo(nano_epoch) NOT NULL,
  id_orden_compra BIGINT REFERENCES ordenes(id) NOT NULL,
  id_orden_venta BIGINT REFERENCES ordenes(id) NOT NULL,
  precio_operado NUMERIC NOT NULL,
  volumen_contratos INT,
  es_agresivo BOOLEAN NOT NULL,
  es_self_match BOOLEAN NOT NULL,
  periodo_ejecucion periodo_ejecucion,
  ganancia_comprador FLOAT NOT NULL,
  ganancia_vendedor FLOAT NOT NULL
);
