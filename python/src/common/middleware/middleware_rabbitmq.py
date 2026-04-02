import pika
import random
import string
import logging
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

logger = logging.getLogger(__name__)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        pass

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
