from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine


Base = automap_base()

engine = create_engine(
    "postgresql://postgres@localhost:5432/dw", echo=True, future=True
)

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
    with Session(engine) as session:
        tiempo = crear_tiempo()
        ordenes = crear_ordenes(tiempo)
        trades = crear_trades(ordenes)

        session.add_all(tiempo)
        session.add_all(ordenes)
        session.add_all(trades)

        session.commit()


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
