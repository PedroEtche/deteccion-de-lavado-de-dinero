TODO:
 - Crear pruebas de integracion desde Make
 - Conversor de monedas
 - Manejo de EOF
 - Workers Unit Test

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
