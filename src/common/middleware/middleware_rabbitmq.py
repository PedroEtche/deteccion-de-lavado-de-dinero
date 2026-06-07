import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host, heartbeat=600, blocked_connection_timeout=300
                )
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name)
            self.queue_name = queue_name
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while initializing queue."
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An error occurred while initializing queue: {str(e)}"
            )

    def send(self, message, routing_key=None):
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while sending message."
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An error occurred while sending message: {str(e)}"
            )

    def start_consuming(self, on_message_callback):
        def pika_callback_wrapper(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            # prefetch_count=1: el broker no le da al consumer un mensaje nuevo
            # hasta que el anterior fue ack-eado. Esto (a) mantiene memoria local
            # acotada, (b) permite balanceo real con --scale en queues compartidas.
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=pika_callback_wrapper,
                auto_ack=False,
            )

            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while consuming messages."
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An error occurred while consuming messages: {str(e)}"
            )

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while stopping consumption."
            )
        except Exception:
            pass

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError(
                "Failed to close the connection properly."
            )


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys=None, queue_name=None, exchange_type="direct"):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host, heartbeat=600, blocked_connection_timeout=300
                )
            )
            self.channel = self.connection.channel()
            self.exchange_name = exchange_name
            
            self.channel.exchange_declare(
                exchange=exchange_name, exchange_type=exchange_type
            )

            self.routing_keys = routing_keys or []

            if self.routing_keys:
                if queue_name:
                    self.channel.queue_declare(queue=queue_name)
                    self.queue_name = queue_name
                else:
                    result = self.channel.queue_declare(queue="", exclusive=True)
                    self.queue_name = result.method.queue

                for routing_key in self.routing_keys:
                    self.channel.queue_bind(
                        exchange=exchange_name,
                        queue=self.queue_name,
                        routing_key=routing_key,
                    )
            else:
                result = self.channel.queue_declare(queue="", exclusive=True)
                self.queue_name = result.method.queue

        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while initializing exchange."
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An error occurred while initializing exchange: {str(e)}"
            )

    def send(self, message, routing_key=None):
        try:
            if routing_key is not None:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_key, body=message
                )
            else:
                for rk in self.routing_keys:
                    self.channel.basic_publish(
                        exchange=self.exchange_name, routing_key=rk, body=message
                    )
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while sending message."
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An error occurred while sending message: {str(e)}"
            )

    def start_consuming(self, on_message_callback):
        def pika_callback_wrapper(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=pika_callback_wrapper,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while consuming messages."
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An error occurred while consuming messages: {str(e)}"
            )

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while stopping consumption."
            )
        except Exception:
            pass

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError(
                "Failed to close the connection properly."
            )
