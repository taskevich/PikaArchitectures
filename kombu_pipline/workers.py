import logging
import multiprocessing
import time

from uuid import uuid4
from kombu.exceptions import MessageStateError
from kombu_base import AMQPWorkerBase
from typing import Callable, Any, Type
from kombu.mixins import Consumer


class AMQPWorkerConsumer(AMQPWorkerBase):
    def __init__(self, handler: Callable = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handler = self.basic_message_consume or handler

    def basic_message_consume(self, message: Any):
        print(f"[{self.__class__.__name__}] Received message: {message}")
        return message

    def get_consumers(self, Consumer: Type[Consumer], _channel):
        return [
            Consumer(
                queues=[self.queue],
                accept=["json"],
                on_message=self.handler,
                prefetch_count=1
            )
        ]

    def handle_message(self, message: Any):
        try:
            print(f"[{self.__class__.__name__}] New message from {self.queue_name}: {message}\nHANDLER: {self.handler}")
            self.handler(message)
        except Exception as e:
            print(f"[{self.__class__.__name__}] Unknown error: {e}\nQueue message: {message.body}")
        finally:
            try:
                message.ack()
            except MessageStateError:
                pass


class AMQPWorkerProducer(AMQPWorkerBase):
    messages: multiprocessing.Queue = multiprocessing.Queue()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def send_message(self, message: Any):
        self.messages.put(message)

    def publish(self, message: Any):
        try:
            self.producer.publish(
                body=message,
                routing_key=self.queue_name,
                correlation_id=uuid4().hex,
                exchange=self.exchange_name,
                serializer="json",
                retry=True,
                retry_policy={
                    'interval_start': 0,
                    'interval_step': 1,
                    'interval_max': 5,
                    'max_retries': 3,
                },
                timeout=10,
            )
        except Exception as ex:
            logging.error(f"[{self.__class__.__name__}] Exception: {ex}")
            raise ex

    def run(self, *args, **kwargs):
        while True:
            try:
                if self.messages.empty():
                    continue
                self.publish(self.messages.get())
            except Exception as ex:
                logging.error(f"[{self.__class__.__name__}] Exception: {ex}", stack_info=True)
                self._reconnect()
            finally:
                time.sleep(1)
