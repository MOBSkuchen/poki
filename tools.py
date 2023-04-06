from exceptions import Exceptions
import os
import copy
import pickle
import zstandard as zst
from structures import Atom, Borrow, Jar, Pin, Cluster, DataBase
import structures


part = {}


def get_db_header(file, size):
    val = b''
    name = None
    realname = None
    ptr = 0
    level = file.read(1).decode()
    idx = 0
    while size > idx:
        idx += 1
        b = file.read(1)
        if idx == size:
            break
        if b == b';':
            if ptr == 1:
                realname = val
                break
            else:
                ptr += 1
                name = val
                val = b''
        else:
            val += b

    if level == "N":
        idx += 1
        file.read(1)
    else:
        _raw_size = b""
        while size > idx:
            idx += 1
            b = file.read(1)
            if b == b'\n':
                break
            _raw_size += b
        size = int(_raw_size.decode()) + idx

    if name is None or realname is None:
        raise Exceptions.BufferError(f"Too few bytes in headers", "get_names")

    return level, realname.decode(), name.decode(), idx, size


def decompress(file, size):
    zstd = zst.ZstdDecompressor()
    stream = zstd.stream_reader(file, size)
    return stream


def compress(file, size):
    zstd = zst.ZstdCompressor()
    stream = zstd.stream_writer(file, size)
    return stream


def level_decompile(level, file, size):
    match level:
        case "C":
            return decompress(file, size)
        case "S":
            raise NotImplementedError("Not Implemented : Secured database")
        case "X":
            file = decompress(file, size)
            return file
        case "N":
            return file
        case _:
            raise Exceptions.HeaderError(f"'{level}' is invalid", "level_decompile")


def level_compile(level, file, size):
    match level:
        case "C":
            return compress(file, size)
        case "S":
            raise NotImplementedError("Not Implemented : Secured database")
        case "X":
            file = compress(file, size)
            return file
        case "N":
            return file
        case _:
            raise Exceptions.HeaderError(f"'{level}' is invalid", "level_compile")


def get_atom_value(file, idx, size):
    value = ""
    h = file.read(1).decode()
    idx += 1
    if h == "@":
        borrow = True
    else:
        borrow = False
        value += h
    while size > idx:
        idx += 1
        piece = file.read(1).decode()
        if ord(piece) == 0:
            break
        value += piece

    return borrow, value, idx


def get_pin_value(file, idx, size):
    value = ""
    h = file.read(1).decode()
    idx += 1
    if h == "@":
        borrow = True
    else:
        borrow = False
        value += h
    while size > idx:
        idx += 1
        piece = file.read(1).decode()
        if ord(piece) == 0:
            break
        value += piece

    return borrow, value, idx


def get_piece(file, idx, size):
    name = ""
    h = file.read(1).decode()
    idx += 1
    if h == "!":
        t = 1
        borrow, value, idx = get_pin_value(file, idx, size)
        name = f'PAD{len(structures.index)}'
        return borrow, name, value, idx, t
    else:
        t = 0
        name += h
    while size > idx:
        idx += 1
        piece = file.read(1).decode()
        if piece == ";":
            break
        name += piece

    borrow, value, idx = get_atom_value(file, idx, size)

    return borrow, name, value, idx, t


def get_sub_header(file, idx, size):
    name = ""
    while size > idx:
        idx += 1
        piece = file.read(1).decode()
        if piece == ":":
            break
        name += piece

    _size = ""
    while size > idx:
        idx += 1
        piece = file.read(1).decode()
        if piece == "\n":
            break
        _size += piece

    size = int(_size)

    return idx, size, name


def make_atom(borrow, name, value, mt_br):
    if borrow:
        if value not in part.keys():
            raise Exceptions.BorrowError(f"Unable to borrow '{value}' for '{name}', as it is not initialized",
                                         "make_atom")
        if mt_br:
            atom = Atom(name, Borrow(part[value]))
        else:
            atom = part[value]
    else:
        atom = Atom(name, value)

    part[name] = atom

    return atom


def make_pin(borrow, name, value, mt_br):
    if borrow:
        if value not in part.keys():
            raise Exceptions.BorrowError(f"Unable to borrow '{value}' for '{name}', as it is not initialized",
                                         "make_atom")
        if mt_br:
            pin = Pin(name, Borrow(part[value]))
        else:
            pin = part[value]
    else:
        pin = Pin(value, name)

    part[name] = pin

    return pin


def load_cluster_B(cluster_name, mt_br, file, cluster_size, idx, size, __idx):
    cluster_content = []
    while (cluster_size + __idx) > idx:
        borrow, name, value, idx, t = get_piece(file, idx, size)
        file.read(1)
        idx += 1

        if t == 0:
            at = make_atom(borrow, name, value, mt_br)
        else:
            at = make_pin(borrow, name, value, mt_br)

        cluster_content.append(at)

    cluster = Cluster(cluster_name, cluster_content)

    return cluster, idx


def load_cluster_A(file, idx, size, mt_br):

    idx, cluster_size, cluster_name = get_sub_header(file, idx, size)
    __idx = copy.copy(idx)

    return load_cluster_B(cluster_name, mt_br, file, cluster_size, idx, size, __idx)


def open_jar_B(file, jar_name, jar_size, idx):
    content = file.read(jar_size)
    jar = Jar(jar_name, pickle.loads(content))
    del content
    file.read(1)
    idx += 1
    return jar, idx


def open_jar_A(file, idx, size, mt_br):
    idx, jar_size, jar_name = get_sub_header(file, idx, size)
    return open_jar_B(file, jar_name, jar_size, idx)


def __load(filename, maintain_borrows):
    size = os.path.getsize(filename)
    with open(filename, 'rb') as file:
        level, realname, name, idx, size = get_db_header(file, size)
        database = DataBase(name, realname)
        file = level_decompile(level, file, size - idx)
        while size > idx:
            head = file.read(1)
            idx += 1
            match head:
                case b"$":  # Cluster
                    cluster, idx = load_cluster_A(file, idx, size, maintain_borrows)
                    database.add(cluster)
                case b"?":  # Jar (Pickle)
                    jar, idx = open_jar_A(file, idx, size, maintain_borrows)
                    database.add(jar)
                case b"":   # EOF
                    break
                case _:
                    raise Exceptions.SubHeadError(f"Invalid sub-header '{head.decode()}'", "load")

    return database


def clean():
    global part
    part = {}
    structures.index = {}


def load(filename, maintain_borrows=False, _clean=True):
    try:
        loaded = __load(filename, maintain_borrows)
        if _clean:
            clean()
        return loaded
    except zst.ZstdError:
        raise Exceptions.CorruptionError(f"Compressed-Database '{filename}' is not in ZST format", "load")