import pika

from typing import Optional
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection


class RabbitMQWorkerBase:
    """ Pika base class for working with RabbitMQ """

    def __init__(
            self,
            queue_name: str,
            exchange_name: str,
    ):
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.connection: Optional[BlockingConnection] = None
        self.channel: Optional[BlockingChannel] = None

    def _connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="localhost",
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="direct")
        self.channel.queue_bind(queue=self.queue_name, exchange=self.exchange_name)
