import contextlib
import json
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from calculador_trades import CalculadorTrades
from progress_bar import ProgressBar
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
    print ("--- INICIANDO GENERACION DE DATOS ---")

    ref_data = json.load(open('ref_data.json'))

    if limpiar_db:
        limpiar_db()

    with Session(engine) as session:
        ref_data_entities = crear_ref_data(ref_data)
        ordenes, fechas, tiempos = crear_ordenes(ref_data)
        trades = crear_trades(ordenes, ref_data)

        session.add_all(ref_data_entities + fechas + tiempos + ordenes + trades)
        session.commit()

    print("--- GENERACION TERMINADA ---")


def limpiar_db():
    print("Eliminando datos anteriores")
    with contextlib.closing(engine.connect()) as con:
        trans = con.begin()
        for table in reversed(Base.metadata.sorted_tables):
            con.execute(table.delete())
        trans.commit()
    print("Datos anteriores eliminados")


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
    print("Creando cuentas, firmas, brokers, operadores e instrumentos...", end='')
    entities = []

    for entity, values in ref_data.items():
        entities += [entity_factory(entity)(params) for params in values]

    print("LISTO")
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
    ratio_rechazo = 0.02 if 'ratio_rechazo' not in config else config["ratio_rechazo"]

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
        var_volumen=var_volumen,
        ratio_rechazo=ratio_rechazo
    )

    ordenes, fechas, tiempos = simulador.correr_simulacion()

    pbar = ProgressBar(len(ordenes), action="CONVIRTIENDO ORDENES A SQL-ALCHEMY", bar_len=50)
    ret = []
    for o in ordenes:
        ret.append(Orden(**o))
        pbar.tick()

    return ret, crear_fechas(fechas), crear_tiempos(tiempos)


def crear_fechas(fechas):
    pbar = ProgressBar(len(fechas), action="CONVIRTIENDO FECHAS A SQL-ALCHEMY", bar_len=50)
    ret = []
    for f in fechas:
        ret.append(
            Fecha(
                dia_epoch=f.timestamp(),
                numero_dia=f.day,
                nombre_dia=Dias[f.date().weekday()],
                numero_semana=f.date().isocalendar()[1],
                numero_mes=f.month,
                nombre_mes=Meses[f.month],
                año=f.year,
                es_finde=f.date().weekday() in [5, 6],
                es_feriado=False  # Por ahora hardcodeado falso
            )
        )
        pbar.tick()

    return ret


def crear_tiempos(tiempos):
    pbar = ProgressBar(len(tiempos), action="CONVIRTIENDO TIEMPOS A SQL-ALCHEMY", bar_len=50)
    ret = []
    for t in tiempos:
        ret.append(
            Tiempo(
                nano_epoch=round(t.timestamp() * 1e9),
                dia_epoch=t.replace(hour=0, minute=0, second=0, microsecond=0).timestamp(),
                nanosegundos=t.microsecond * 1000,
                segundo=t.second,
                minuto=t.minute,
                hora=t.hour
            )
        )
        pbar.tick()

    return ret


def crear_trades(ordenes, ref_data):
    hora_apertura = 8 if 'hora_apertura' not in config else config["hora_apertura"]
    hora_cierre = 17 if 'hora_cierre' not in config else config["hora_cierre"]
    instrumentos = ref_data["instrumentos"]
    ratio_cancelacion = 0.01 if 'ratio_cancelacion' not in config else config["ratio_cancelacion"]

    calculador = CalculadorTrades(instrumentos, hora_apertura, hora_cierre, ratio_cancelacion)
    trades = calculador.calcular_trades(ordenes)
    pbar = ProgressBar(len(trades), action="CONVIRTIENDO TRADES A SQL-ALCHEMY", bar_len=50)
    ret = []
    for t in trades:
        ret.append(Trade(**t))
        pbar.tick()

    return ret


if __name__ == "__main__":
    main()
