class RouteNotFoundError(Exception):
    def __init__(self, route, message="Route not found"):
        self.route = route
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.route} -> {self.message}'


