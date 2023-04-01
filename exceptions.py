class Exceptions:
    class BaseException(Exception):
        def __init__(self, head, msg, level, *args):
            self.head = head
            self.msg = msg
            self.level = level
            self.__head = head
            super().__init__(self)

    class HeaderError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__(self, "Unexpected header", msg, level, *args)

    class BufferError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__(self, "Unexpected end of data", msg, level, *args)

    class SubHeadError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__(self, "Unexpected sub header", msg, level, *args)

    class BorrowError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__(self, "Invalid Borrow", msg, level, *args)

    class CorruptionError(BaseException):
        def __init__(self, msg, level, *args):
            super().__init__(self, "Invalid format", msg, level, *args)

