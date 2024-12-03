import json
import logging
import multiprocessing
import time
from queue import Empty

import pika

from pika_base import RabbitMQWorkerBase
from typing import Callable, Any


class RabbitMQConsumer(RabbitMQWorkerBase):
    def __init__(self, handler: Callable = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handler = handler or self.basic_message_consume

    def basic_message_consume(
            self,
            channel,
            method_frame,
            header_frame,
            body
    ):
        try:
            print(f"[{self.__class__.__name__}] Received message from {channel}: {body}")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as ex:
            print(ex)
            channel.basic_nack(delivery_tag=method_frame.delivery_tag)

    def run(self, *args, **kwargs):
        self._connect()
        while True:
            try:
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.handler)
                self.channel.start_consuming()
            except Exception as ex:
                logging.error(ex, stack_info=True)
            finally:
                try:
                    self.channel.close()
                except:
                    pass
            time.sleep(5)


class RabbitMQProducer(RabbitMQConsumer):
    messages: multiprocessing.Queue = multiprocessing.Queue()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def publish(self, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.queue_name,
                body=message,
            )
        except Exception as ex:
            raise ex

    def send_message(self, message: Any):
        self.messages.put(message)

    def run(self, *args, **kwargs):
        self._connect()
        while True:
            try:
                if self.messages.empty():
                    continue
                message = self.messages.get()
                self.publish(message)
                time.sleep(1)
            except Exception as ex:
                logging.error(ex, stack_info=True)
                self._connect()
            time.sleep(1)
