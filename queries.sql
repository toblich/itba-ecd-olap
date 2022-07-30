
--------------------------------
-- Manipulación de settlement --
--------------------------------

-- drop index idx_trades_instr_fechas_dia_epoch;
-- create index if not exists idx_trades_instr_fechas_dia_epoch on trades_instrumentos_fechas (dia_epoch);
-- drop index idx_trades_instr_fechas_ticker;
-- create index if not exists idx_trades_instr_fechas_ticker on trades_instrumentos_fechas using hash (ticker);

-- ¿Qué porcentaje del volumen del día fue operado durante el período de settlement?
-- + ¿Qué porcentaje de la cantidad de trades del día fue realizado durante el período de settlement?

-- Variante 1 (sobre las tablas originales)
with por_periodo as (
  select ticker, dia_epoch, periodo_ejecucion, sum(volumen_contratos) as volumen, count(id) as cantidad_trades
  from trades
  join tiempo on trades.stamp = tiempo.nano_epoch
  where trades.ticker = 'SOY.F.12' and tiempo.dia_epoch between 1658286000 and 1658458800
  group by ticker, dia_epoch, periodo_ejecucion
),
total as (
  select dia_epoch, sum(volumen) as volumen_total, sum(cantidad_trades) as cantidad_trades_total from por_periodo group by dia_epoch
)
select ticker, total.dia_epoch, periodo_ejecucion, volumen, volumen_total, cantidad_trades, cantidad_trades_total,
  volumen / volumen_total * 100 as porcentaje_volumen,
  cantidad_trades / cantidad_trades_total * 100 as porcentaje_cantidad
from por_periodo
join total on por_periodo.dia_epoch = total.dia_epoch
order by dia_epoch asc, periodo_ejecucion asc;

-- Variante 2 (sobre el cubo como vista materializada)
select ticker, dia_epoch, periodo_ejecucion, volumen,
  sum(volumen) over (partition by ticker, dia_epoch) as volumen_total,
  cantidad_trades,
  sum(cantidad_trades) over (partition by ticker, dia_epoch) as cantidad_trades_total,
  volumen / (sum(volumen) over (partition by ticker, dia_epoch)) * 100 as porcentaje_volumen,
  cantidad_trades / (sum(cantidad_trades) over (partition by ticker, dia_epoch)) * 100 as porcentaje_cantidad
from settlement_cubo_trades
where
  ticker = 'SOY.F.12'
  and dia_epoch between 1658286000 and 1658458800
  and periodo_ejecucion is not null
  and es_agresivo is null
  and es_self_match is null
  and tipo_derivado is null
order by dia_epoch asc, periodo_ejecucion asc;


--
-- ¿Qué porcentaje del volumen operado durante el período de settlement fue agresivo?
-- En comparación, ¿Qué porcentaje del volumen operado durante todo el día fue agresivo?

select ticker, dia_epoch, periodo_ejecucion, es_agresivo,
  volumen,
  sum(volumen) over (partition by ticker, dia_epoch) as volumen_total,
  100 * volumen / (sum(volumen) over (partition by ticker, dia_epoch)) as porcentaje_volumen
from settlement_cubo_trades
where
  ticker = 'SOY.F.12'
  and dia_epoch between 1658286000 and 1658458800
  and periodo_ejecucion is not null
  and es_agresivo is not null
  and es_self_match is null
  and tipo_derivado is null
order by dia_epoch asc, periodo_ejecucion asc, es_agresivo asc;

--
-- ¿Qué porcentaje del volumen operado durante el período de settlement fue self-matched?
-- En comparación, ¿Qué porcentaje del volumen operado durante todo el día fue self-matched?
select ticker, dia_epoch, periodo_ejecucion, es_self_match,
  volumen,
  sum(volumen) over (partition by ticker, dia_epoch) as volumen_total,
  100 * volumen / (sum(volumen) over (partition by ticker, dia_epoch)) as porcentaje_volumen
from settlement_cubo_trades
where
  ticker = 'SOY.F.12'
  and dia_epoch between 1658286000 and 1658458800
  and periodo_ejecucion is not null
  and es_agresivo is null
  and es_self_match is not null
  and tipo_derivado is null
order by dia_epoch asc, periodo_ejecucion asc, es_self_match asc;

--------------------------------
-- 		    Spoofing          --
--------------------------------

-- Para un instrumento (contrato), derivado o producto, y un rango de días determinado, 
-- agregando por mes, día, hora, minuto y por firma, cuenta u operador:

-- ¿Cuál es el porcentaje de órdenes canceladas sobre trades de ese participante?

-- Ej 1: Por minuto, ticker y cuenta
SELECT 	t.año, t.numero_mes, t.numero_dia, t.hora, t.minuto, t.ticker,
		t.id_firma, t.firma, t.id_cuenta, t.cuenta,
		oc.cantidad as cantidad_cancelaciones,
		t.cantidad as cantidad_trades,
		CASE WHEN oc.cantidad IS NOT NULL THEN (oc.cantidad / t.cantidad) * 100 ELSE 0 END as porcentaje_cancelacion
FROM
	spoofing_trades_participante t
	LEFT JOIN cubo_ordenes_canceladas oc ON (
		(oc.cierre_minuto = t.minuto)
		AND (oc.cierre_hora = t.hora)
		AND (oc.cierre_numero_dia = t.numero_dia)
		AND (oc.cierre_numero_mes = t.numero_mes)
		AND (oc.cierre_año = t.año)
		AND (oc.ticker = t.ticker)
		AND (oc.id_cuenta = t.id_cuenta)
		AND (oc.id_operador IS NULL) -- Agregación por operador de órdenes canceladas
		AND (oc.lado IS NULL) -- Agregación por lado
	)
WHERE
	t.ticker IS NOT NULL
	AND t.id_cuenta IS NOT NULL
	AND t.minuto IS NOT NULL
order by porcentaje_cancelacion desc

-- Ej 2: Por hora, tipo_derivado y firma
SELECT 	t.año, t.numero_mes, t.numero_dia, t.hora, t.nombre_producto, t.tipo_derivado, t.id_firma, t.firma,
		oc.cantidad as cantidad_cancelaciones,
		t.cantidad as cantidad_trades,
		CASE WHEN oc.cantidad IS NOT NULL THEN (oc.cantidad / t.cantidad) * 100 ELSE 0 END as porcentaje_cancelacion
FROM
	spoofing_trades_participante t
	LEFT JOIN cubo_ordenes_canceladas oc ON (
		(oc.cierre_minuto IS NULL AND t.minuto IS NULL) -- Agregado por minuto
		AND (oc.cierre_hora = t.hora)
		AND (oc.cierre_numero_dia = t.numero_dia)
		AND (oc.cierre_numero_mes = t.numero_mes)
		AND (oc.cierre_año = t.año)
		AND (oc.ticker IS NULL AND t.ticker IS NULL) -- Agregado por ticket
		AND (oc.precio_ejercicio IS NULL AND t.precio_ejercicio IS NULL) -- Agregado por precio_ejercicio
		AND (oc.vencimiento IS NULL AND t.vencimiento IS NULL) -- Agregado por vencimiento
		AND (oc.tipo_derivado = t.tipo_derivado)
		AND (oc.nombre_producto = t.nombre_producto)
		AND (oc.id_cuenta IS NULL AND t.id_cuenta IS NULL) -- Agregado por cuenta
		AND (oc.id_firma = t.id_firma)
		AND (oc.id_operador IS NULL) -- Agregación por operador de órdenes canceladas
		AND (oc.lado IS NULL) -- Agregación por lado
	)
WHERE
	t.nombre_producto IS NOT NULL
	AND t.tipo_derivado IS NOT NULL
	AND t.id_firma IS NOT NULL
	AND t.hora IS NOT NULL
order by porcentaje_cancelacion desc

-- ¿Cuántos trades hizo un participante en un sentido determinado (compra/venta), luego de una cancelación de órdenes en el sentido contrario? 
-- ¿Cuál es el volumen total operado en éstos trades?

-- Ej 1: Por minuto, ticker y cuenta
SELECT 	t.año, t.numero_mes, t.numero_dia, t.hora, t.minuto, t.ticker,
		t.id_firma, t.firma, t.id_cuenta, t.cuenta,
		SUM(CASE WHEN (
			((t.ultimo_trade_venta > oc.primera_cancelacion) AND (oc.lado = 'venta'))
			OR ((t.ultimo_trade_compra > oc.primera_cancelacion) AND (oc.lado = 'compra'))
			) THEN t.cantidad ELSE 0 END) AS cantidad_trades_luego_cancelacion,
		SUM(CASE WHEN (
			((t.ultimo_trade_venta > oc.primera_cancelacion) AND (oc.lado = 'venta'))
			OR ((t.ultimo_trade_compra > oc.primera_cancelacion) AND (oc.lado = 'compra'))
			) THEN t.volumen ELSE 0 END) AS volumen_trades_luego_cancelacion
FROM
	spoofing_trades_participante t
	LEFT JOIN cubo_ordenes_canceladas oc ON (
		(oc.cierre_minuto = t.minuto)
		AND (oc.cierre_hora = t.hora)
		AND (oc.cierre_numero_dia = t.numero_dia)
		AND (oc.cierre_numero_mes = t.numero_mes)
		AND (oc.cierre_año = t.año)
		AND (oc.ticker = t.ticker)
		AND (oc.id_cuenta = t.id_cuenta)
		AND (oc.id_operador IS NULL) -- Agregación por operador de órdenes canceladas
		AND (oc.lado IS NOT NULL) -- No agrego el subtotal por lado
	)
GROUP BY
	t.año, t.numero_mes, t.numero_dia, t.hora, t.minuto, t.ticker, t.id_firma, t.firma, t.id_cuenta, t.cuenta
order by cantidad_trades_luego_cancelacion desc, volumen_trades_luego_cancelacion desc

--------------------------------
----------- Wash Trades -----------
--------------------------------
--¿Cuántos trades fueron realizados entre participantes de la misma firma? 
--¿Qué volumen total se operó en éstos trades? 

-- Ej: Por año, mes, dia y ticker
select año, numero_mes, numero_dia, ticker
,id_firma_comprador, count(*) as cantidad_trades_misma_firma
,sum(volumen_contratos) as volumen
from trades_base
where id_firma_comprador = id_firma_vendedor
group by rollup(año,numero_mes,numero_dia),rollup(ticker), id_firma_comprador
order by 1,2,3,4,5
