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
