from base_controller import ControllerBase


class CentralController(ControllerBase):
    def handle_packet(
            self,
            message
    ):
        print(f"[{self.__class__.__name__}] Central Controller: {message}")
        return message
