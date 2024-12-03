from base_controller import ControllerBase


class PipelineController(ControllerBase):
    def handle_packet(
            self,
            message
    ):
        print(f"[{self.__class__.__name__}] Pipeline Controller: {message}")
        return message
