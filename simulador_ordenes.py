import datetime
import random
import json
from datetime import timedelta
from random import randrange
import csv

from progress_bar import ProgressBar

random.seed(7)

class SimuladorOrdenes:

    def __init__(self,
                 ref_data,
                 fecha_desde,
                 fecha_hasta,
                 hora_apertura,
                 hora_cierre,
                 ordenes_por_hora,
                 precio_base_buy,
                 var_precio_buy,
                 precio_base_sell,
                 var_precio_sell,
                 volumen_base,
                 var_volumen,
                 ratio_rechazo=0.02):
        self.ref_data = ref_data
        self.fecha_desde = fecha_desde
        self.fecha_hasta = fecha_hasta
        self.hora_apertura = hora_apertura
        self.hora_cierre = hora_cierre
        self.ordenes_por_hora = ordenes_por_hora
        self.precio_buy = precio_base_buy
        self.var_buy = var_precio_buy
        self.precio_sell = precio_base_sell
        self.var_sell = var_precio_sell
        self.vol_base = volumen_base
        self.var_vol = var_volumen
        self.ratio_rechazo = ratio_rechazo
        self.id_actual = 1

    def correr_simulacion(self):
        fecha_actual = self.fecha_desde.replace(hour=self.hora_apertura, minute=0, second=0)
        fecha_final = self.fecha_hasta.replace(hour=self.hora_cierre, minute=0, second=0)
        (ordenes, fechas, tiempos) = [], [], []
        cambia_dia = True
        cant_total_horas = ((fecha_final - fecha_actual).days + 1) * (self.hora_cierre - self.hora_apertura)
        pbar = ProgressBar(cant_total_horas, action="CALCULANDO ORDENES", bar_len=50)
        while fecha_actual < fecha_final:
            if cambia_dia:
                fechas.append(fecha_actual.replace(hour=0, minute=0, second=0))
            (o, t) = self.generar_ordenes(fecha_actual)
            ordenes += o
            tiempos += t
            fecha_actual, cambia_dia = self.avanzar_hora(fecha_actual)
            pbar.tick()

        return ordenes, fechas, tiempos

    def avanzar_hora(self, fecha_actual):
        cierre_actual = fecha_actual.replace(hour=self.hora_cierre, minute=0, second=0)
        fecha_actual = fecha_actual + timedelta(hours=1)
        avanza_dia = False
        if fecha_actual >= cierre_actual:
            fecha_actual = (fecha_actual + timedelta(days=1)).replace(hour=self.hora_apertura, minute=0, second=0)
            avanza_dia = True
        return fecha_actual, avanza_dia

    def generar_ordenes(self, fecha_actual):
        #  Genero órdenes al azar dentro de esa hora, según ordenes_por_hora
        #  Primero genero tiempos al azar al microsegundo dentro de la hora actual, y los ordeno
        tiempos = self.generar_tiempos(fecha_actual)

        #  Genero instrumentos y participantes al azar para esas órdenes
        tickers = self.generar_entidades("ticker", "instrumentos")
        cuentas = self.generar_entidades("id", "cuentas")
        operadores = self.generar_entidades("id", "operadores")

        #  Armo las órdenes
        ordenes = []
        for i in range(self.ordenes_por_hora):
            lado = random.choice(["compra", "venta"])
            precio_base = self.precio_buy if lado == "compra" else self.precio_sell
            var_precio = self.var_buy if lado == "compra" else self.var_sell
            orden = {
                "id": self.id_actual,
                "ticker": tickers[i],
                "timestamp_creacion": round(tiempos[i].timestamp() * 1e9),
                "timestamp_cierre": None,
                "id_cuenta": cuentas[i],
                "id_operador": operadores[i],
                "lado": lado,
                "precio_stop": None,
                "precio_limit": round(random.gauss(mu=precio_base, sigma=var_precio), 2),
                "estado": "pendiente" if random.uniform(0, 1) > self.ratio_rechazo else "rechazada",
                "es_agresiva": False,
                "cantidad_contratos": round(random.gauss(mu=self.vol_base, sigma=self.var_vol))
            }
            orden["precio_limit"] = orden["precio_limit"] if orden["precio_limit"] > 0 else 0.01  #  Corrección de precio >= 0.01
            orden["cantidad_contratos"] = orden["cantidad_contratos"] if orden["cantidad_contratos"] > 1 else 1 #  Corrección de volumen >= 1
            ordenes.append(orden)
            self.id_actual += 1

        return ordenes, list(set(tiempos))

    def generar_tiempos(self, fecha_actual):
        return sorted([fecha_actual.replace(minute=0, second=0)
                       + timedelta(minutes=randrange(60), seconds=randrange(60), milliseconds=randrange(999), microseconds=randrange(999))
                       for _ in range(self.ordenes_por_hora)])

    def generar_entidades(self, clave, entidad):
        return [e[clave] for e in random.choices(self.ref_data[entidad], k=self.ordenes_por_hora)]


#   Testing del simulador
if __name__ == "__main__":
    ref_data = json.load(open('ref_data.json'))
    fecha_desde = datetime.datetime(year=2022, month=7, day=19)
    fecha_hasta = datetime.datetime(year=2022, month=7, day=22)
    simulador = SimuladorOrdenes(ref_data, fecha_desde, fecha_hasta, 8, 17, 100, 10, 1, 12, 1, 100, 20)
    ordenes, _, _ = simulador.correr_simulacion()
    print("Pendientes:", len([o for o in ordenes if o["estado"] == "pendiente"]))
    print("Rechazadas:", len([o for o in ordenes if o["estado"] == "rechazada"]))
    order_cols = list(ordenes[0].keys())
    with open('ordenes.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=order_cols)
        writer.writeheader()
        writer.writerows(ordenes)
