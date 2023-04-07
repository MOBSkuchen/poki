class Exceptions:
    class BaseException(Exception):
        def __init__(self, head, msg, level, *args):
            self.head = head
            self.msg = msg
            self.level = level
            super().__init__(self, *args)

    class HeaderError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__("Unexpected header", msg, level, *args)

    class BufferError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__("Unexpected end of data", msg, level, *args)

    class SubHeadError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__("Unexpected sub header", msg, level, *args)

    class BorrowError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__("Invalid Borrow", msg, level, *args)

    class CorruptionError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__("Invalid format", msg, level, *args)

    class UnsupportedError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__("Unsupported operation", msg, level, *args)
