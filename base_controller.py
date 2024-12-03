import logging


class ControllerBase:
    def __init__(
            self,
            consumer,
            producer=None,
            pipline_producer=None
    ):
        self.consumer = consumer
        self.producer = producer
        self.pipline_producer = pipline_producer

    def handle_packet(
            self,
            message
    ):
        raise NotImplementedError

    def handle_message(
            self,
            channel,
            method_frame,
            header_frame,
            message
    ):
        print(f"[{self.__class__.__name__}] Handling message: {message}")
        try:
            message = self.handle_packet(message)
        except Exception as ex:
            logging.error(ex)
        finally:
            if self.pipline_producer and message:
                self.pipline_producer.publish(message)
            else:
                print(f"[{self.__class__.__name__}] Done with: {message}")
