import zstandard as zst
import pickle

index = {}


class Borrow:
    def __init__(self, value):
        self.value: Atom | Pin = value

    def get(self):
        return self.value

    def format(self):
        return f'@{index[self.value.name]}'


class Atom:
    def __init__(self, name, value):
        _t = type(value)
        if _t == str:
            self.value = value.encode()
        elif _t == Borrow:
            self.value = value.format().encode()
        else:
            self.value = bytes(value)
        self.name = name

        index[name] = self

    def eval(self):
        return self.name.encode() + b";" + self.value + chr(0).encode()

    def borrow(self):
        return Borrow(self)

    def __repr__(self):
        return self.name


class Pin:
    def __init__(self, value, name=None):
        _t = type(value)
        if _t == str:
            self.value = value.encode()
        elif _t == Borrow:
            self.value = value.format().encode()
        else:
            self.value = bytes(value)

        if name is None:
            self.name = f'PAD{len(index)}'
        else:
            self.name = name

        index[self.name] = self

    def eval(self):
        return b'!' + self.value + chr(0).encode()

    def borrow(self):
        return Borrow(self)

    def __repr__(self):
        return self.name


class Cluster:
    def __init__(self, title, particles: list[Atom]):
        self.title = title
        self.particles = particles

    def add(self, particle):
        self.particles.append(particle)

    def format(self):
        return b"\n".join([at.eval() for at in self.particles])

    def __repr__(self):
        _ = "\n> "
        return f'{self.title}\n> {_.join([p.__repr__() for p in self.particles])}'


class Jar:
    def __init__(self, title, obj):
        self.title = title
        self.obj = obj

    def format(self):
        return pickle.dumps(self.obj)

    def __repr__(self):
        return self.obj.__repr__()


class DataBase:
    def __init__(self, name, location):
        self.name = name
        self.location = location
        self.elements: list[Cluster | Jar] = []

    def _get_header(self, secure, compression, size):
        _header = f"{self.name};{self.location};{size if size else ''}\n"
        if secure and compression:
            _header = "X" + _header
        elif secure:
            _header = "S" + _header
        elif compression:
            _header = "C" + _header
        else:
            _header = "N" + _header

        return _header.encode()

    def export(self, secure=False, compression=False):
        size = 0
        if compression:
            gen = self.assemble()
            zstd = zst.ZstdCompressor()
            with open(self.location, 'wb') as file:
                stream = zstd.stream_writer(file)
                for asm in gen:
                    stream.write(asm)
                    stream.write(b'\n')
                    size += (len(asm) + 1)
                file.seek(0)
                file.write(self._get_header(secure, compression, size))
                stream.close()
            return

        gen = self.assemble()
        with open(self.location, 'wb') as file:
            file.write(self._get_header(secure, compression, None))
            for asm in gen:
                file.write(asm)
                file.write(b'\n')
                size += (len(asm) + 1)

    def assemble(self):
        # print(f'> Assembling {len(self.elements)} elements')
        for element in self.elements:
            _t = type(element)
            if _t == Cluster:
                _header = "$"
            else:
                _header = "?"
            fmt = element.format()
            _header += f"{element.title}:{len(fmt)}\n"
            _j = _header.encode() + fmt
            yield _j

    def add(self, element):
        self.elements.append(element)