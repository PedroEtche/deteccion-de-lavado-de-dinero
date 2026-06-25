# TODO

## Funcionalidades

- Q2 falla con unkwons -> MV
- Persistir msg_ids en workers con estado -> MV
- Escritura atomica de directorios (batches cuando llegan y msg_id) -> MV
- Stateless solo persiste el EOF_count, Stateful persiste batches + last_msg_id_seen + msg_id_counter + state + EOF_count -> MV
- Gateway + cliente tolerancia a fallos -> P

# Para el informe


## Heartbeat de pika
Las conexiones de pika tienen un heartbeat para asegurarse que la conexion siga viva, 
ese heartbeat tiene un timeout. En el caso de los workers que acumulan estado, se 
esta consumiendo constantemente asi que esa conexion no muere. El problema es que 
mientras se consume, el worker queda bloqueado en start_consuming y el exchange 
output no manda el heartbeat, al ser datasets tan grandes eso hace que se bloquee 
mucho mas tiempo que el timeout y muere la conexion del output, haciendo que no se 
puedan forwardear los mensajes. Por eso a los outputs les ponemos heartbeat=0. Esto 
no es un problema porque el output es un publisher, no necesita mandar acks para 
saber si hay que reenviar un mensaje. En el caso de los consumers, el timeout lo 
bajamos a 60s para que si se cae un worker antes de mandar el ack, pika se entere 
que se cayó el worker luego de 60s y sepa que tiene que reencolar el mensaje. 
