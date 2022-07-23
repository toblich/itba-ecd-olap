import contextlib
import json
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from simulador_ordenes import SimuladorOrdenes

Base = automap_base()

# Basic configuration
config = json.load(open('config.json'))
limpiar_db = False if 'limpiar_db' not in config else config['limpiar_db']
db = 'dw' if 'db' not in config else config['db']
usuario_db = 'postgres' if 'usuario_db' not in config else config['usuario_db']
password = False if 'password' not in config else config['password']

connection_string = f"postgresql://{usuario_db}@localhost:5432/{db}" if not password else f"postgresql+psycopg2://{usuario_db}:{password}@localhost:5432/{db}"

engine = create_engine(connection_string, echo=True, future=True)

# reflect the tables
Base.prepare(autoload_with=engine)

# mapped classes are now created with names by default
# matching that of the table name.
Broker = Base.classes.brokers
Cuenta = Base.classes.cuentas
Fecha = Base.classes.fechas
Firma = Base.classes.firmas
Instrumento = Base.classes.instrumentos
Operador = Base.classes.operadores
Orden = Base.classes.ordenes
Tiempo = Base.classes.tiempo
Trade = Base.classes.trades

Dias = {0: "lunes", 1: "martes", 2: "miercoles", 3: "jueves", 4: "viernes", 5: "sabado", 6: "domingo"}
Meses = {1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril', 5: 'mayo', 6: 'junio', 7: 'julio', 8: 'agosto',
         9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'}


def main():
    ref_data = json.load(open('ref_data.json'))

    if limpiar_db:
        limpiar_db()

    with Session(engine) as session:
        ref_data_entities = crear_ref_data(ref_data)
        ordenes, fechas, tiempos = crear_ordenes(ref_data)
        trades = []  # crear_trades(ordenes)

        session.add_all(ref_data_entities + fechas + tiempos + ordenes + trades)
        session.commit()


def limpiar_db():
    with contextlib.closing(engine.connect()) as con:
        trans = con.begin()
        for table in reversed(Base.metadata.sorted_tables):
            con.execute(table.delete())
        trans.commit()


def adaptar_instrumentos(p):
    p["vencimiento"] = datetime.strptime(p["vencimiento"], '%Y-%m')
    p["intervalo_settlement"] = timedelta(minutes=p["intervalo_settlement"])
    return p


def entity_factory(entity):
    return {
        "brokers": lambda p: Broker(**p),
        "firmas": lambda p: Firma(**p),
        "operadores": lambda p: Operador(**p),
        "cuentas": lambda p: Cuenta(**p),
        "instrumentos": lambda p: Instrumento(**adaptar_instrumentos(p)),
    }[entity]


def crear_ref_data(ref_data):
    entities = []

    for entity, values in ref_data.items():
        entities += [entity_factory(entity)(params) for params in values]

    return entities


def crear_ordenes(ref_data):
    fecha_desde = "19-07-2022" if 'fecha_desde' not in config else config["fecha_desde"]
    fecha_hasta = "22-07-2022" if 'fecha_hasta' not in config else config["fecha_hasta"]
    hora_apertura = 8 if 'hora_apertura' not in config else config["hora_apertura"]
    hora_cierre = 17 if 'hora_cierre' not in config else config["hora_cierre"]
    ordenes_por_hora = 100 if 'ordenes_por_hora' not in config else config["ordenes_por_hora"]
    precio_base_compra = 10 if 'precio_base_compra' not in config else config["precio_base_compra"]
    var_precio_compra = 1 if 'var_precio_compra' not in config else config["var_precio_compra"]
    precio_base_venta = 12 if 'precio_base_venta' not in config else config["precio_base_venta"]
    var_precio_venta = 1 if 'var_precio_venta' not in config else config["var_precio_venta"]
    volumen_base = 100 if 'volumen_base' not in config else config["volumen_base"]
    var_volumen = 20 if 'var_volumen' not in config else config["var_volumen"]

    fecha_desde = datetime.strptime(fecha_desde, '%d-%m-%Y')
    fecha_hasta = datetime.strptime(fecha_hasta, '%d-%m-%Y')

    simulador = SimuladorOrdenes(
        ref_data=ref_data,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        hora_apertura=hora_apertura,
        hora_cierre=hora_cierre,
        ordenes_por_hora=ordenes_por_hora,
        precio_base_buy=precio_base_compra,
        precio_base_sell=precio_base_venta,
        var_precio_buy=var_precio_compra,
        var_precio_sell=var_precio_venta,
        volumen_base=volumen_base,
        var_volumen=var_volumen
    )

    ordenes, fechas, tiempos = simulador.correr_simulacion()

    return [Orden(**o) for o in ordenes], crear_fechas(fechas), crear_tiempos(tiempos)


def crear_fechas(fechas):
    return [Fecha(
        dia_epoch=f.timestamp(),
        numero_dia=f.day,
        nombre_dia=Dias[f.date().weekday()],
        numero_semana=f.date().isocalendar()[1],
        numero_mes=f.month,
        nombre_mes=Meses[f.month],
        año=f.year,
        es_finde=f.date().weekday() in [5, 6],
        es_feriado=False  # Por ahora hardcodeado falso
    ) for f in fechas]


def crear_tiempos(tiempos):
    return [Tiempo(
        nano_epoch=round(t.timestamp() * 1e9),
        dia_epoch=t.replace(hour=0, minute=0, second=0, microsecond=0).timestamp(),
        nanosegundos=t.microsecond * 1000,
        segundo=t.second,
        minuto=t.minute,
        hora=t.hour
    ) for t in tiempos]


def crear_trades(ordenes):
    # # Divido Ventas y Compras
    # Ventas = ordenes[lado="venta"]
    # Compras = ordenes[lado="compra"]

    # # Inicializo algunas variables, seguramente acá me falten más
    # Trade.id=0

    # For compra in Compras
    # For venta in Ventas


    # # Como las tablas están ordenadas por timestamp, debería ir matcheando contra lo primero que puede, tipo FIFO
    # # Caso precio venta <= precio compra y que me alcancen la cantidad a vender
    # If venta.ticker == compra.ticker and venta.precio_limit <= compra.precio adn venta.cantidad_contratos >= compra.cantidad_contratos and venta.cantidad_contratos > 0 and compra.cantidad_contratos > 0 then 
    # venta.cantidad_contratos = venta.cantidad_contratos-compra.cantidad_contratos


    # Trade.id = Trade.id+1
    # Trade.ticker = ticker
    # Trade.stamp = max(venta.timestamp_creacion, compra.timestamp_creacion)
    # Trade.id_orden_compra = compra.id
    # Trade.id_orden_venta = venta.id
    # Trade.precio_operado = venta.precio_limit
    # Trade.volumen_contratos = compra.cantidad_contratos
    # compra.cantidad_contratos = 0
    # Trade.es_agresivo = false
    # Tarde.es_self_match = if compra.id_cuenta==venta.id_cuenta then 1 else 0
    # # Ver período de settlement como se pone
    # Trade.periodo_ejecucion = buscar en instrumentos?
    # # Ver ganancias comprador y vendedor
    # Trade.ganancia_comprador = buscar precio de último trade del día y restar?
    # Trade.ganancia_vendedor = Trade.ganancia_comprador

    # # Cambio estados de ordenes
    # venta.estado = if venta.cantidad_contratos == 0 then "ejecutada" else "parcial"
    # compra.estado = "ejecutada"

    # else
    # # Caso precio venta <= precio compra y que no me alcancen la cantidad a vender
    # If venta.ticker == compra.ticker and venta.precio_limit <= compra.precio adn venta.cantidad_contratos <  compra.cantidad_contratos and venta.cantidad_contratos > 0 and compra.cantidad_contratos > 0 then 
    # compra.cantidad_contratos=compra.cantidad_contratos-venta.cantidad_contratos

    # Trade.id = Trade.id+1
    # Trade.ticker = ticker
    # Trade.stamp = max(venta.timestamp_creacion, compra.timestamp_creacion)
    # Trade.id_orden_compra = compra.id
    # Trade.id_orden_venta = venta.id
    # Trade.precio_operado = venta.precio_limit
    # Trade.volumen_contratos = venta.cantidad_contratos
    # venta.cantidad_contratos = 0
    # Trade.es_agresivo = false
    # Tarde.es_self_match = if compra.id_cuenta==venta.id_cuenta then 1 else 0
    # # Ver período de settlement como se pone
    # Trade.periodo_ejecucion = buscar en instrumentos?
    # # Ver ganancias comprador y vendedor
    # Trade.ganancia_comprador = buscar precio de último trade del día y restar?
    # Trade.ganancia_vendedor = Trade.ganancia_comprador

    # # Cambio estados de ordenes
    # venta.estado = "ejecutada"
    # compra.estado = "parcial"

    # end los 2 for

    return []


if __name__ == "__main__":
    main()
