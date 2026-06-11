# Preguntas y bugs pendientes

## Client (`src/client/main.py`)
- `recv_results` solo incrementa `query_result_counter` cuando llega un EOF de **Q1**. Las ramas Q2/Q3/Q4/Q5 no lo incrementan 

## Gateway (`src/gateway/main.py`)
- **Bug** `main` registra `SIGTERM`/`SIGINT` llamando a `gateway.stop()`, pero `Gateway` no define `stop()`. 
- Por que los `msg_id` se generan con `uuid.uuid4()` random en vez de un counter incremental por cliente? podria ser más barato y útil para dedup/ordering downstream. (no estoy seguro si lo necesitamos igual)
- **Revisar el doble publish a `transactions_date_exchange`**: `send_transactions_data` (`src/gateway/main.py:171-179`) publica cada batch a `transactions_usd_exchange` Y a `transactions_date_exchange`. Por qué? no coincide con el diagrama. Por ahora los scenarios apuntan `TRANSACTIONS_DATE_EXCHANGE` a un exchange muerto (`transactions_unused_exchange`) para dropear los duplicados. Además el conteo de workers USD está hardcodeado en 3 (`transactions_usd_workers`).
- Puede que falte proyectar columnas entre los stages? revisar porque no estoy 100% (al menos q3)

# Minuta de desciciones de diseño
Tener presente que puede ser necesaria la clase clase `StatefullWorker`
Delegar a los strategies el estado

Tolerancia a fallos:
Cada vez que a un worker le llega un mensaje, lo primero que hace, es mandarselo a sus replicas
Cada replica es una copia exacta. Tiene el mismo strategy y debe procesar los mismos mensajes que el `master` a medida que el `master` se los manda. De esta forma el estado es el mismo siempre

Cuando llega el momendo de propagar respuestas (llegan los `eof`), el master debe primero mandar el mensaje a la siguiente instancia y luego debe avisarle a sus replicas que mando el primer mensaje. Como la copia es una copia exacta con solo avisarle que mando el siguiente mensaje la replica deberia saber cual es y poder desecharlo.

Para la deteccion de caidas se debe implementar un sistema de heartbeats entre maestro y replicas. Esto, capac, se puede deledar a una clase maestra dentro del codigo. Se tiene que usar un callback que se active cada X tiempo. En el caso de una caida, se arrancara una eleccion de lider (bully o ring/anillo).

Ver de cambiar a colas persistentes para que una replica tome el lugar de maestro y pueda seguir consumiendo los mensajes que hayan quedado sin atender durante la caida



# VIEJO

## Q3 Routing Layout

Para la query de "cuenta de origen y monto de transacciones USD en el período [2022-09-06, 2022-09-15] con monto menor a 1 centésimo del promedio encontrado para el mismo formato de pago en el período [2022-09-01, 2022-09-05]", el layout de RabbitMQ queda fijo así:

- Exchange entre `group` y `aggregator`: `aggregate_route`
- Routing keys de shard para `group -> aggregator`: `aggregate_route_0`, `aggregate_route_1`, ..., `aggregate_route_{N-1}`
- Cola de entrada de cada aggregator shard: una cola estable con el mismo nombre que su routing key, por ejemplo `aggregate_route_0`
- Cola compartida de salida de agregators hacia `join`: `q3_join_queue`
- Cola final de resultados: `result_queue`

Regla de sharding:

- La partición se calcula con `shard_id = crc32(payment_format) % N`
- El `group` publica cada batch al routing key `aggregate_route_{shard_id}`
- Todos los mensajes de un mismo `payment_format` caen siempre en el mismo shard

Flujo:

1. `filter` deja pasar solo el rango de fechas requerido.
2. `group` agrupa y enruta por `payment_format`.
3. `aggregator` acumula el promedio por `payment_format` dentro de cada shard.
4. `join` solo une los resultados parciales y publica el resultado final.

## BUGS A REVISAR

Lista de problemas identificados en el manejo actual de EOFs y propagación entre stages. Pendientes de validar y corregir.

### 1. NACK con requeue infinito en mensajes envenenados
En `src/common/worker/worker.py` (`_on_message`), cualquier excepción en `process_data` / `flush_state` dispara `nack()`, que por default reencola el mensaje. Si el error es determinista (payload corrupto, bug en una strategy), el worker entra en *poison message loop*: lo procesa, falla, lo reencola, lo vuelve a procesar. No hay límite de reintentos ni dead-letter queue.

### 2. Pérdida de EOF si el worker crashea entre flush y ack
En `BaseWorker._on_message` el orden es: `handle_eof` → `_internal_on_flush` (flushea estado y envía EOF downstream) → recién después `ack()`. Si el proceso muere entre el send downstream y el ack:
- El EOF original se redelivera al rearrancar.
- Pero `EofCoordinator` ya hizo `pop` del counter para ese cliente, así que el contador arranca de 0.
- Como los EOFs anteriores ya fueron ack'd, nunca se vuelve a llegar a `expected_eofs`. **El cliente queda colgado para siempre.**

Tampoco hay persistencia del counter, así que un restart limpio tampoco ayuda.

### 3. Sin idempotencia ante redeliveries
Si por reconexión o redelivery RabbitMQ entrega el mismo EOF dos veces (mismo `msg_id`), el contador se incrementa dos veces. Con `expected_eofs=N`, alcanza con que un publisher tenga un redelivery para que el flush se dispare con `N-1` publishers realmente terminados, y se flushee estado parcial.

El `msg_id` que se incluye en `build_eof_message` existe pero **no se usa para deduplicar** en `EofCoordinator`.

### 4. Lock vestigial en `EofCoordinator`
`EofCoordinator.handle_eof` envuelve todo en un `threading.Lock`, pero hoy se invoca desde un único thread (el de pika en `start_consuming`), así que no protege de nada real. El problema: la estructura sugiere multi-threading, y si alguien (con razón) mete un listener thread aparte para EOFs intra-stage, el flush actualmente emite downstream **dentro del lock**, lo que bloquearía cualquier handler de datos que quisiera tomar ese mismo lock. La versión vieja comentada del archivo tenía un `@contextmanager lock()` separado del flush justamente para esto, y se borró sin reemplazo.

### 5. Asimetría en el envío de EOF a accounts vs transactions
En `gateway/main.py`, el EOF a transactions va por `send_transactions_eof` con `routing_key="eof_broadcast"`, pero el EOF a accounts va por `accounts_mw.send(eof_message)` sin routing key. Hay que confirmar contra la config concreta de accounts que los workers downstream realmente reciban el EOF; la asimetría es propensa a divergir.

### 6. Dos protocolos de EOF conviviendo
`src/join/main.py` y `src/currency_converter/main.py` no heredan de `BaseWorker`: el primero instancia un `EofCoordinator` con `eof_fanout` aparte y los EOFs van por un exchange fanout dedicado; el segundo maneja `eof` a mano. Esto significa que dos protocolos de EOF coexisten en el sistema, y el `expected_eofs` de un stage downstream depende de cuál usa su upstream. Cambiar el protocolo de un upstream puede romper silenciosamente el conteo aguas abajo.

### 7. Doble señalización de "fin" en mensajes de resultado
En `src/common/communication/internal.py`, los mensajes de resultado llevan un flag `eof: true|false` dentro del payload, separado del `type: "eof"` usado entre stages. El cliente revisa `decoded["eof"]`, mientras que el gateway revisa `decoded.get("type") == "eof"`. Son dos mecanismos paralelos para señalar "se terminó X", uno por flag y otro por tipo. No es un bug per se, pero es una fuente fácil de bugs futuros.
