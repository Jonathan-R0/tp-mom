import pika
import random
import string
import logging
from .middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange
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
