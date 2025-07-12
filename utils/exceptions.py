class NoneError(Exception):
    def __init__(self, message="Erro ao processar a solicitação"):
        self.message = message
        super().__init__(self.message)