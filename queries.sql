select * from trades limit 5;

select * from fechas;

select * from trades_instrumentos_fechas limit 5;

--------------------------------
-- Manipulación de settlement --
--------------------------------

-- drop materialized view trades_instrumentos_fechas;
create materialized view trades_instrumentos_fechas as (
  select
    trades.id, trades.precio_operado, trades.volumen_contratos, trades.es_agresivo, trades.es_self_match,
    trades.periodo_ejecucion, trades.ganancia_comprador, trades.ganancia_vendedor, trades.periodo_ejecucion = 'settlement' as es_settlement,
    fechas.*, instrumentos.*
  from trades
  join tiempo on trades.stamp = tiempo.nano_epoch
  join fechas on tiempo.dia_epoch = fechas.dia_epoch
  join instrumentos on trades.ticker = instrumentos.ticker
);

-- drop index idx_trades_instr_fechas_dia_epoch;
-- create index if not exists idx_trades_instr_fechas_dia_epoch on trades_instrumentos_fechas (dia_epoch);
-- drop index idx_trades_instr_fechas_ticker;
-- create index if not exists idx_trades_instr_fechas_ticker on trades_instrumentos_fechas using hash (ticker);

-- drop materialized view cubo_trades;
create materialized view cubo_trades as (
  select ticker, tipo_derivado, nombre_producto, dia_epoch, periodo_ejecucion, es_agresivo, es_self_match,
    sum(volumen_contratos) as volumen,
    count(id) as cantidad_trades,
    sum(volumen_contratos * precio_operado) / sum(volumen_contratos) as vwap,
    avg(precio_operado) as precio_medio
  from trades_instrumentos_fechas
  group by cube(ticker, (tipo_derivado, nombre_producto), dia_epoch, periodo_ejecucion, es_agresivo, es_self_match)
);

select * from cubo_trades;



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
order by dia_epoch asc, periodo_ejecucion asc
;

-- Variante 2 (sobre el cubo como vista materializada)
select ticker, dia_epoch, periodo_ejecucion, volumen,
  sum(volumen) over (partition by ticker, dia_epoch) as volumen_total,
  cantidad_trades,
  sum(cantidad_trades) over (partition by ticker, dia_epoch) as cantidad_trades_total,
  volumen / (sum(volumen) over (partition by ticker, dia_epoch)) * 100 as porcentaje_volumen,
  cantidad_trades / (sum(cantidad_trades) over (partition by ticker, dia_epoch)) * 100 as porcentaje_cantidad
from cubo_trades
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
from cubo_trades
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
from cubo_trades
where
  ticker = 'SOY.F.12'
  and dia_epoch between 1658286000 and 1658458800
  and periodo_ejecucion is not null
  and es_agresivo is null
  and es_self_match is not null
  and tipo_derivado is null
order by dia_epoch asc, periodo_ejecucion asc, es_agresivo asc;
