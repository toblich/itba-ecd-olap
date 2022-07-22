import contextlib
import json
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from datetime import datetime, timedelta

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


def main():
    ref_data = json.load(open('ref_data.json'))

    if limpiar_db:
        limpiar_db()

    with Session(engine) as session:
        ref_data_entities = crear_ref_data(ref_data)
        tiempo = []  # crear_tiempo()
        ordenes = []  # crear_ordenes(tiempo)
        trades = []  # crear_trades(ordenes)

        session.add_all(ref_data_entities + tiempo + ordenes + trades)
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


def crear_tiempo():
    print("creado tiempo")
    return [
        # insert into fechas values(123, 1, 'lunes', 32, 1, 'enero', 2020, FALSE, FALSE);
        Tiempo(
            nano_epoch=1234567890,
            dia_epoch=123,
            nanosegundos=98765,
            segundo=4,
            minuto=35,
            hora=12,
        )
    ]


def crear_ordenes(tiempo):
    return [
        # insert into instrumentos values('petroleo.dic.45', 'petroleo', 'commodity', 'futuro', '2022-12-1', 45.0, '1 minute');
        # insert into firmas values(1, 'firma1');
        # insert into brokers values(1, 'broker1');
        # insert into cuentas values(1, 1, 1, 'hola');
        # insert into operadores values (1, 'op1');
        Orden(
            id=9876543,
            ticker="petroleo.dic.45",
            timestamp_creacion=tiempo[0].nano_epoch,
            timestamp_cierre=tiempo[0].nano_epoch,
            id_cuenta=1,
            id_operador=1,
            lado="compra",
            precio_stop=None,
            precio_limit=10.0,
            estado="pendiente",
            es_agresiva=False,
            cantidad_contratos=3,
        )
    ]


def crear_trades(ordenes):
    return []


if __name__ == "__main__":
    main()
