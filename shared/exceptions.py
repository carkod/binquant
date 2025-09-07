class BinquantErrors(Exception):
    def __init__(self, msg, code=None):
        self.code = code
        self.message = msg
        super().__init__(self.code, self.message)
        return None

    def __str__(self) -> str:
        return f"Binquant Error: {self.code} {self.message}"


class WebSocketError(BinquantErrors):
    pass


class BinanceErrors(Exception):
    pass


class BinbotError(Exception):
    pass


class InvalidSymbol(BinanceErrors):
    pass


class AutotradeError(Exception):
    def __init__(self, message) -> None:
        self.message = message

    pass


class RestartStreaming(Exception):
    pass
