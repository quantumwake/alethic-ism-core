from .base_message_router import Route

class RouterStorage:

    def fetch_router(self, selector: str):
        raise NotImplementedError()

    def fetch_route(self):
        raise NotImplementedError()

    def register_route(self, route: Route):
        raise NotImplementedError()
