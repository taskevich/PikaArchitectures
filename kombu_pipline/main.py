import json
import logging
import time
from threading import Thread
from central_controller import CentralController
from kombu_pipline.workers import AMQPWorkerProducer, AMQPWorkerConsumer
from pipeline_controller import PipelineController

controller_producer = AMQPWorkerProducer(
    queue_name="service-1",
    exchange_name="services",
)

controller_consumer = AMQPWorkerConsumer(
    queue_name="service-1",
    exchange_name="services",
)

pipeline_producer = AMQPWorkerProducer(
    queue_name="service-2",
    exchange_name="services",
)

pipeline_consumer = AMQPWorkerConsumer(
    queue_name="service-2",
    exchange_name="services",
)

central_controller = CentralController(
    consumer=pipeline_consumer,
    producer=controller_producer,
)
pipeline_consumer.handler = central_controller.handle_message

pipline_controller = PipelineController(
    consumer=controller_consumer,
    producer=pipeline_producer,
)
controller_consumer.handler = pipline_controller.handle_message


def main():
    t1 = Thread(target=controller_consumer.run)
    t2 = Thread(target=pipeline_consumer.run)
    t3 = Thread(target=controller_producer.run)
    t4 = Thread(target=pipeline_producer.run)

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    while True:
        controller_producer.publish(repr({"message": "Hello, world!"}))
        time.sleep(10)


if __name__ == "__main__":
    main()
