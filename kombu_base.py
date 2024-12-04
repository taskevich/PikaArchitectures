from kombu import Connection, Queue
from kombu.mixins import ConsumerProducerMixin

AMQP_CONNECTION_CONFIG = {
    'hostname': f"amqp://guest:guest@localhost:5672//",
    'transport_options': {'confirm_publish': True}
}


class AMQPWorkerBase(ConsumerProducerMixin):
    def __init__(
            self,
            queue_name: str,
            exchange_name: str,
            create_queue: bool = True
    ):
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.create_queue = create_queue
        self.connection = None
        self.queue = None
        self._reconnect()

    def _reconnect(self):
        self.connection = Connection(**AMQP_CONNECTION_CONFIG)
        if self.create_queue:
            self.queue = Queue(
                name=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.queue_name,
                channel=self.connection.channel(),
                durable=True,
            )
            self.queue.declare()

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
