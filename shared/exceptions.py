
class BinquantErrors(Exception):
    def __init__(self, msg, code=None):
        self.code = code
        self.message = msg
        super().__init__(self.code, self.message)
        return None

    def __str__(self) -> str:
        return f"Binance Error: {self.code} {self.message}"

class KlinesConnectorSocketException(BinquantErrors):
    pass
