from datetime import datetime, timedelta
from random import uniform
from progress_bar import ProgressBar


class Posicion:

    def __init__(self):
        self.detalle_pos = []
        self.pos = 0

    def computar_trade(self, precio, volumen, lado):
        if (lado == "compra" and self.pos >= 0) or (lado == "venta" and self.pos <= 0):
            self._agregar(precio, volumen, lado)
            return 0

        return self._offset_trades(precio, volumen, lado)

    def _agregar(self, precio, volumen, lado):
        self.detalle_pos.append({
            "precio": precio,
            "volumen": volumen
        })
        self.pos += volumen if lado == "compra" else -volumen

    def _offset_trades(self, precio, volumen, lado):
        vol_actual = volumen
        ganancia = 0
        while vol_actual > 0 and len(self.detalle_pos) > 0:
            pos_fifo = self.detalle_pos[0]
            offset_pos = min(pos_fifo["volumen"], vol_actual)
            pos_fifo["volumen"] -= offset_pos
            vol_actual -= offset_pos
            self.pos += offset_pos if lado == "compra" else -offset_pos
            signo = 1 if lado == "venta" else -1
            ganancia += signo * (precio - pos_fifo["precio"]) * offset_pos
            if pos_fifo["volumen"] == 0:
                self.detalle_pos.pop(0)

        if vol_actual > 0:
            self._agregar(precio, vol_actual, lado)

        return ganancia


class CalculadorTrades:

    def __init__(self, instrumentos, hora_apertura, hora_cierre, ratio_cancelacion=0.01):
        self.instrumentos = instrumentos
        self.hora_apertura = hora_apertura
        self.hora_cierre = hora_cierre
        self.ratio_cancelacion = ratio_cancelacion
        self.posiciones = {}
        self._inicializar_libros()
        self.id_actual = 1

    def _inicializar_libros(self):
        self.libros_compras = {}
        self.libros_ventas = {}
        for i in self.instrumentos:
            self.libros_compras[i["ticker"]] = []
            self.libros_ventas[i["ticker"]] = []

    def calcular_trades(self, ordenes):
        trades = []
        ordenes_filtradas = [o for o in ordenes if o.estado == "pendiente"]  # Filtro órdenes rechazadas
        cant_actual = dict([(o.id, o.cantidad_contratos) for o in ordenes_filtradas])
        pbar = ProgressBar(len(ordenes_filtradas), action="CALCULANDO TRADES", bar_len=50)
        for orden in ordenes_filtradas:
            es_compra = (orden.lado == "compra")
            libro = self.libros_compras[orden.ticker] if es_compra else self.libros_ventas[orden.ticker]
            libro.append(orden)
            libro.sort(key=lambda o: o.precio_limit, reverse=not es_compra)
            trade = self._procesar_trade(orden, es_compra, cant_actual)
            if trade is not None:
                trades.append(trade)

            # Cancelar ordenes random cada 1/ratio_cancelacion rondas de ambos libros para el ticker actual (ordenes activas que no matchearon)
            # Uso ticker y timestamp de la orden actual para cancelar las ordenes viejas (timestamp ya existe en tabla tiempo y es posterior = MESSIRVE)
            if uniform(0, 1) < self.ratio_cancelacion:
                self._cancelar_ordenes_al_azar(orden.ticker, orden.timestamp_creacion)

            pbar.tick()

        return trades

    def _cancelar_ordenes_al_azar(self, ticker, timestamp_cierre):
        self._cancelar(self.libros_compras[ticker], "pendiente", "cancelada", timestamp_cierre)
        self._cancelar(self.libros_ventas[ticker], "pendiente", "cancelada", timestamp_cierre)
        self._cancelar(self.libros_compras[ticker], "parcialmente ejecutada", "parcialmente cancelada", timestamp_cierre)
        self._cancelar(self.libros_ventas[ticker], "parcialmente ejecutada", "parcialmente cancelada", timestamp_cierre)

    def _cancelar(self, libro, estado, nuevo_estado, timestamp):
        # Cancelo un ratio_cancelacion % de las ordenes activas
        ordenes_a_cancelar = [o for o in libro if o.estado == estado and uniform(0, 1) < self.ratio_cancelacion]
        for o in ordenes_a_cancelar:
            o.estado = nuevo_estado
            o.timestamp_cierre = timestamp
            libro.remove(o)

    def _procesar_trade(self, orden, es_compra, cant_actual):
        libro_compras = self.libros_compras[orden.ticker]
        libro_ventas = self.libros_ventas[orden.ticker]

        # Un libro vacío: No hay trade
        if (len(libro_compras) == 0) or (len(libro_ventas) == 0):
            return None

        mejor_compra = libro_compras[len(libro_compras) - 1]
        mejor_venta = libro_ventas[len(libro_ventas) - 1]

        # No se cruzan los precios: No hay trade
        if mejor_compra.precio_limit < mejor_venta.precio_limit:
            return None

        precio_trade = mejor_venta.precio_limit if es_compra else mejor_compra.precio_limit
        es_agresivo = mejor_compra.precio_limit > mejor_venta.precio_limit  # Si son iguales es a valor de mercado, no agresivo
        volumen_trade = min(cant_actual[mejor_compra.id], cant_actual[mejor_venta.id])

        ganancia_compra, ganancia_venta = self._calcular_ganancias(mejor_compra, mejor_venta, precio_trade, volumen_trade)

        trade = {
            "id": self.id_actual,
            "ticker": orden.ticker,
            "stamp": orden.timestamp_creacion,
            "id_orden_compra": mejor_compra.id,
            "id_orden_venta": mejor_venta.id,
            "precio_operado": precio_trade,
            "volumen_contratos": volumen_trade,
            "es_agresivo": es_agresivo,
            "es_self_match": (mejor_venta.id_cuenta == mejor_compra.id_cuenta),
            "periodo_ejecucion": self._calcular_periodo_ejecucion(orden),
            "ganancia_comprador": ganancia_compra,
            "ganancia_vendedor": ganancia_venta
        }

        compra_totalmente_ejec = self._actualizar_orden(mejor_compra, (es_agresivo and es_compra), volumen_trade, cant_actual, trade["stamp"])
        if compra_totalmente_ejec:
            libro_compras.pop()

        venta_totalmente_ejec = self._actualizar_orden(mejor_venta, (es_agresivo and not es_compra), volumen_trade, cant_actual, trade["stamp"])
        if venta_totalmente_ejec:
            libro_ventas.pop()

        self.id_actual += 1
        return trade

    def _actualizar_orden(self, orden, es_agresivo, volumen_trade, cant_actual, timestamp_trade):
        orden.es_agresiva = es_agresivo
        cant_actual[orden.id] -= volumen_trade
        orden.estado = 'ejecutada' if cant_actual[orden.id] <= 0 else 'parcialmente ejecutada'
        orden.timestamp_cierre = None if not (orden.estado == 'ejecutada') else timestamp_trade
        return orden.estado == 'ejecutada'

    def _calcular_ganancias(self, mejor_compra, mejor_venta, precio_trade, volumen_trade):
        # Posicion compra y venta
        pos_compra = self._obtener_posicion(mejor_compra)
        pos_venta = self._obtener_posicion(mejor_venta)

        # Actualizar posiciones y calcular ganancias
        ganancia_compra = pos_compra.computar_trade(precio_trade, volumen_trade, "compra")
        ganancia_venta = pos_venta.computar_trade(precio_trade, volumen_trade, "venta")

        return ganancia_compra, ganancia_venta

    def _calcular_periodo_ejecucion(self, orden):
        tiempo_orden = datetime.fromtimestamp(orden.timestamp_creacion * 1e-9)
        inicio_settle, fin_settle = self._calcular_settlement(orden)
        inicio_apertura, cierre_apertura = self._calcular_apertura(orden)
        if inicio_apertura <= tiempo_orden <= cierre_apertura:
            return "opening"
        elif inicio_settle <= tiempo_orden <= fin_settle:
            return "settlement"

        return "pre-settlement"

    def _calcular_settlement(self, orden):
        instrumento = next(i for i in self.instrumentos if i["ticker"] == orden.ticker)
        dia_orden = datetime.fromtimestamp(orden.timestamp_creacion * 1e-9)
        fin_settle = dia_orden.replace(hour=self.hora_cierre, minute=0, second=0, microsecond=0)
        duracion_settle = instrumento["intervalo_settlement"]
        inicio_settle = fin_settle - duracion_settle
        return inicio_settle, fin_settle

    def _calcular_apertura(self, orden):
        dia_orden = datetime.fromtimestamp(orden.timestamp_creacion * 1e-9)
        inicio_apertura = dia_orden.replace(hour=self.hora_apertura, minute=0, second=0, microsecond=0)
        duracion_apertura = timedelta(hours=1)  # Por ahora apertura lo considero la primer hora
        cierra_apertura = inicio_apertura + duracion_apertura
        return inicio_apertura, cierra_apertura

    def _obtener_posicion(self, orden) -> Posicion:
        # Agregar el ticker al diccionario de posiciones si aún no existe
        if orden.ticker not in self.posiciones:
            self.posiciones[orden.ticker] = {}

        # Agregar la cuenta al diccionario del ticker si aun no existe e inicializar posición en 0
        if orden.id_cuenta not in self.posiciones[orden.ticker]:
            self.posiciones[orden.ticker][orden.id_cuenta] = Posicion()

        return self.posiciones[orden.ticker][orden.id_cuenta]
