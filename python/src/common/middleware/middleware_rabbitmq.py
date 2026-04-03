import pika
import random
import string
import logging
from .middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError
)

logger = logging.getLogger(__name__)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host: str, queue_name: str):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"Connected to RabbitMQ. Queue declared: {self.queue_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("Could not connect to RabbitMQ")

    def send(self, message: str):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host: str, exchange_name: str, routing_keys: list):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys if isinstance(routing_keys, list) else [routing_keys]
        self.connection = None
        self.channel = None
        self.consumer_queue = None

        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
            logger.info(f"Connected to RabbitMQ. Exchange declared: {self.exchange_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("Could not connect tos RabbitMQ")

    def send(self, message: str, routing_key: str = None):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")

            key = routing_key if routing_key else self.routing_keys[0]

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            logger.info(f"Message published to exchange: {self.exchange_name} with routing key {key}: {message}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")
