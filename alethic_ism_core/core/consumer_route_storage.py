from .messaging.base_message_route_model import BaseRoute


class RouterStorage:

    def fetch_router(self, selector: str):
        raise NotImplementedError()

    def fetch_route(self):
        raise NotImplementedError()

    def register_route(self, route: BaseRoute):
        raise NotImplementedError()
