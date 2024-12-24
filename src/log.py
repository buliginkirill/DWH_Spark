
class Logger:
    _log = []

    @classmethod
    def clear_log(cls):
        cls._log = []

    @classmethod
    def log(cls, msg):
        print(msg)
        cls._log.append(msg)

    @classmethod
    def read_log(cls):
        return cls._log
