class TemporaryError(Exception):
    def __init__(self, code, delay=None):
        self.code, self.delay = code, delay
        super().__init__(code)


class PermanentError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class RecipientMissing(Exception):
    pass


def retry_delay(attempt):
    return (10, 60, 300, 1800)[attempt - 1] if 1 <= attempt < 5 else None

