--------------------------------
--    Vistas materializadas   --
--------------------------------

-- Limpio vistas en order inverso para luego recrearlas
drop materialized view if exists spoofing_trades_participante;
drop materialized view if exists cubo_trades_tiempo_instrumento;
drop materialized view if exists settlement_cubo_trades;
drop materialized view if exists trades_base;
drop materialized view if exists cubo_ordenes_canceladas;
drop materialized view if exists ordenes_base;

-- Creo las vistas
create materialized view ordenes_base as (
  select 	o.*,
			i.nombre_producto,
			i.tipo_producto,
			i.tipo_derivado,
			i.vencimiento,
			i.precio_ejercicio,
			i.intervalo_settlement,
			tcreacion.segundo as creacion_segundo,
			tcreacion.minuto as creacion_minuto,
			tcreacion.hora as creacion_hora,
			tcierre.segundo as cierre_segundo,
			tcierre.minuto as cierre_minuto,
			tcierre.hora as cierre_hora,
			fcreacion.dia_epoch as creacion_dia_epoch,
			fcreacion.numero_dia as creacion_numero_dia,
			fcreacion.nombre_dia as creacion_nombre_dia,
			fcreacion.numero_semana as creacion_numero_semana,
			fcreacion.numero_mes as creacion_numero_mes,
			fcreacion.nombre_mes as creacion_nombre_mes,
			fcreacion.año as creacion_año,
			fcreacion.es_finde as creacion_es_finde,
			fcreacion.es_feriado as creacion_es_feriado,
			fcierre.dia_epoch as cierre_dia_epoch,
			fcierre.numero_dia as cierre_numero_dia,
			fcierre.nombre_dia as cierre_nombre_dia,
			fcierre.numero_semana as cierre_numero_semana,
			fcierre.numero_mes as cierre_numero_mes,
			fcierre.nombre_mes as cierre_nombre_mes,
			fcierre.año as cierre_año,
			fcierre.es_finde as cierre_es_finde,
			fcierre.es_feriado as cierre_es_feriado,
			c.id_firma as id_firma,
			c.id_broker as id_broker,
			c.nombre as nombre_cuenta,
			b.nombre as nombre_broker,
			op.nombre as nombre_operador,
			f.nombre as nombre_firma
  from ordenes o
  join tiempo tcreacion on o.timestamp_creacion = tcreacion.nano_epoch
  left join tiempo tcierre on o.timestamp_cierre = tcierre.nano_epoch
  join fechas fcreacion on tcreacion.dia_epoch = fcreacion.dia_epoch
  left join fechas fcierre on tcierre.dia_epoch = fcierre.dia_epoch
  join instrumentos i on o.ticker = i.ticker
  join cuentas c on c.id = o.id_cuenta
  join operadores op on op.id = o.id_operador
  join brokers b on b.id = c.id_broker
  join firmas f on f.id = c.id_firma
);

create materialized view cubo_ordenes_canceladas AS (
	select	o.cierre_minuto,
			o.cierre_hora,
			o.cierre_numero_dia,
			o.cierre_numero_mes,
			o.cierre_año,
			o.nombre_producto,
			o.tipo_derivado,
			o.vencimiento,
			o.precio_ejercicio,
			o.ticker,
			o.id_firma,
			o.nombre_firma,
			o.id_cuenta,
			o.nombre_cuenta,
			o.id_operador,
			o.nombre_operador,
			o.lado,
			min(o.timestamp_cierre) primera_cancelacion,
			max(o.timestamp_cierre) ultima_cancelacion,
			sum(o.cantidad_contratos) volumen,
			count(*) cantidad
	from
		ordenes_base o
	where
		o.estado in ('cancelada', 'parcialmente cancelada')
	group by
		rollup(o.cierre_año, o.cierre_numero_mes, o.cierre_numero_dia, o.cierre_hora, o.cierre_minuto),
		rollup(o.nombre_producto, o.tipo_derivado, o.vencimiento, (o.precio_ejercicio, o.ticker)),
		rollup((o.id_firma, o.nombre_firma), (o.id_cuenta, o.nombre_cuenta)),
		rollup((o.id_operador, o.nombre_operador)),
		rollup(o.lado)
);

create materialized view trades_base as (
  select 	trades.*, 
			trades.periodo_ejecucion = 'settlement' as es_settlement,
			fechas.*,
			tiempo.nanosegundos,
			tiempo.segundo,
			tiempo.minuto,
			tiempo.hora,
			i.nombre_producto,
			i.tipo_producto,
			i.tipo_derivado,
			i.vencimiento,
			i.precio_ejercicio,
			i.intervalo_settlement,
			o_compra.id_cuenta as id_cuenta_comprador,
			o_compra.id_operador as id_operador_comprador,
			o_compra.id_broker as id_broker_comprador,
			o_compra.id_firma as id_firma_comprador,
			o_compra.nombre_cuenta as cuenta_comprador,
			o_compra.nombre_operador as operador_comprador,
			o_compra.nombre_broker as broker_comprador,
			o_compra.nombre_firma as firma_comprador,
			o_venta.id_cuenta as id_cuenta_vendedor,
			o_venta.id_operador as id_operador_vendedor,
			o_venta.id_broker as id_broker_vendedor,
			o_venta.id_firma as id_firma_vendedor,
			o_venta.nombre_cuenta as cuenta_vendedor,
			o_venta.nombre_operador as operador_vendedor,
			o_venta.nombre_broker as broker_vendedor,
			o_venta.nombre_firma as firma_vendedor
  from trades
  join ordenes_base o_compra on o_compra.id = trades.id_orden_compra
  join ordenes_base o_venta on o_venta.id = trades.id_orden_venta
  join tiempo on trades.stamp = tiempo.nano_epoch
  join fechas on tiempo.dia_epoch = fechas.dia_epoch
  join instrumentos i on trades.ticker = i.ticker
);

create materialized view settlement_cubo_trades as (
  select ticker, tipo_derivado, nombre_producto, dia_epoch, periodo_ejecucion, es_agresivo, es_self_match,
    sum(volumen_contratos) as volumen,
    count(id) as cantidad_trades,
    sum(volumen_contratos * precio_operado) / sum(volumen_contratos) as vwap,
    avg(precio_operado) as precio_medio
  from trades_base
  group by cube(ticker, (tipo_derivado, nombre_producto), dia_epoch, periodo_ejecucion, es_agresivo, es_self_match)
);

create materialized view cubo_trades_tiempo_instrumento as (
	select 	minuto,
			hora,
			numero_dia,
			numero_mes,
			año,
			nombre_producto,
			tipo_derivado,
			vencimiento,
			precio_ejercicio,
			ticker,
			id_firma_comprador,
			firma_comprador,
			id_cuenta_comprador,
			cuenta_comprador,
			id_operador_comprador,
			operador_comprador,
			id_broker_comprador,
			broker_comprador,
			id_firma_vendedor,
			firma_vendedor,
			id_cuenta_vendedor,
			cuenta_vendedor,
			id_operador_vendedor,
			operador_vendedor,
			id_broker_vendedor,
			broker_vendedor,
			periodo_ejecucion,
			es_agresivo,
			es_self_match,
			sum(precio_operado * volumen_contratos) as vwp,
    		sum(volumen_contratos) as volumen,
			count(*) as cantidad,
			min(stamp) as primer_trade,
			max(stamp) as ultimo_trade
  from trades_base
  group by
	rollup(año, numero_mes, numero_dia, hora, (minuto, periodo_ejecucion)),
	rollup(nombre_producto, tipo_derivado, vencimiento, (precio_ejercicio, ticker)),
	id_firma_comprador, firma_comprador, id_broker_comprador, broker_comprador, id_cuenta_comprador, cuenta_comprador,
	id_firma_vendedor, firma_vendedor, id_broker_vendedor, broker_vendedor, id_cuenta_vendedor, cuenta_vendedor,
	id_operador_comprador, operador_comprador,
	id_operador_vendedor, operador_vendedor,
	rollup(es_agresivo),
	rollup(es_self_match)
);

create materialized view spoofing_trades_participante as (
	with trades_participante AS (
		-- LADO COMPRA
		select 	t.minuto, t.hora, t.numero_dia, t.numero_mes, t.año, 
				t.ticker, t.precio_ejercicio, t.vencimiento, t.tipo_derivado, t.nombre_producto,
				t.id_firma_comprador as id_firma, t.firma_comprador as firma,
				t.id_cuenta_comprador as id_cuenta, t.cuenta_comprador as cuenta,
				t.id_operador_comprador as id_operador, t.operador_comprador as operador,
				SUM(t.volumen) as volumen, SUM(t.cantidad) as cantidad, MIN(t.primer_trade) as primer_trade, MAX(t.ultimo_trade) as ultimo_trade,
				'compra' as lado
		from 
			cubo_trades_tiempo_instrumento t
		where
			t.es_agresivo IS NULL -- Agregación por agresivo
			AND t.es_self_match IS NULL -- Agregación por self-match. De un lado cuantifico todos los trades.
		GROUP BY
			t.minuto, t.hora, t.numero_dia, t.numero_mes, t.año, t.ticker, t.precio_ejercicio, t.vencimiento, t.tipo_derivado, t.nombre_producto,
			t.id_firma_comprador, t.firma_comprador, t.id_cuenta_comprador, t.cuenta_comprador, t.id_operador_comprador, t.operador_comprador
		UNION
		-- LADO VENTA
		select 	t.minuto, t.hora, t.numero_dia, t.numero_mes, t.año,
				t.ticker, t.precio_ejercicio, t.vencimiento, t.tipo_derivado, t.nombre_producto,
				t.id_firma_vendedor as id_firma, t.firma_vendedor as firma,
				t.id_cuenta_vendedor as id_cuenta, t.cuenta_vendedor as cuenta,
				t.id_operador_vendedor as id_operador, t.operador_vendedor as operador,
				SUM(t.volumen) as volumen, SUM(t.cantidad) as cantidad, MIN(t.primer_trade) as primer_trade, MAX(t.ultimo_trade) as ultimo_trade,
				'venta' as lado
		from 
			cubo_trades_tiempo_instrumento t
		where
			t.es_agresivo IS NULL -- Agregación por agresivo
			AND t.es_self_match = FALSE -- Agrego solo las NO self-matched. Esto es para evitar un doble conteo en las que el mismo participante esta a ambos lados.
		GROUP BY
			t.minuto, t.hora, t.numero_dia, t.numero_mes, t.año, t.ticker, t.precio_ejercicio, t.vencimiento, t.tipo_derivado, t.nombre_producto,
			t.id_firma_vendedor, t.firma_vendedor, t.id_cuenta_vendedor, t.cuenta_vendedor, t.id_operador_vendedor, t.operador_vendedor
	)
	SELECT 	t.minuto, t.hora, t.numero_dia, t.numero_mes, t.año,
			t.ticker, t.precio_ejercicio, t.vencimiento, t.tipo_derivado, t.nombre_producto,
			t.id_firma, t.firma, t.id_cuenta, t.cuenta, t.id_operador, t.operador, 
			SUM(volumen) as volumen,
			SUM(cantidad) as cantidad,
			-- 9,223,372,036,854,775,807 es MAX BIGINT
			MIN(CASE WHEN (lado = 'compra') THEN t.primer_trade ELSE 9223372036854775807 END) as primer_trade_compra, 
			MAX(CASE WHEN (lado = 'compra') THEN t.ultimo_trade ELSE 0 END) as ultimo_trade_compra,
			MIN(CASE WHEN (lado = 'venta') THEN t.primer_trade ELSE 9223372036854775807 END) as primer_trade_venta, 
			MAX(CASE WHEN (lado = 'venta') THEN t.ultimo_trade ELSE 0 END) as ultimo_trade_venta
	FROM
		trades_participante t
	GROUP BY
		t.minuto, t.hora, t.numero_dia, t.numero_mes, t.año,
		t.ticker, t.precio_ejercicio, t.vencimiento, t.tipo_derivado, t.nombre_producto,
		rollup((t.id_firma, t.firma), (t.id_cuenta, t.cuenta)),
		rollup((t.id_operador, t.operador))
);
