import regex
from exceptions import Exceptions
import os
import copy
import pickle
import zstandard as zst

from remote import RemoteDataBaseAccessor
from structures import Atom, Borrow, Jar, Pin, Cluster, DataBase
import structures

part = {}


# This is modified from:
# https://stackoverflow.com/a/43060761/16595859
def stream_regex(pattern, file, chunksize=8192):
    window = pattern[:0]
    sentinel = object()

    last_chunk = False

    while not last_chunk:
        chunk = file.read(chunksize)
        if not chunk:
            last_chunk = True
        window += chunk

        match = sentinel
        for match in regex.finditer(pattern, window, partial=not last_chunk):
            if not match.partial:
                pos = match.start(), match.end()
                yield pos

        if match is sentinel or not match.partial:
            window = window[:0]
        else:
            window = window[match.start():]
            if match.start() == 0:
                chunksize *= 2


def find_sub_header(file, name, size=None):
    if size is None:
        size = "*"
    pat = '[\=|\?]{name}+:{size}'.format(name=name, size=size).encode()
    gen = stream_regex(pat, file)
    return gen


def get_db_header(file, size):
    """
    Get database headers:
    - level (S, C, X, N)
    - realname (filename)
    - name
    - size (in compressed databases, the size is contained in the header)
    :param file:
    File `BufferReader` (rb)
    :param size:
    Naive filesize (as returned by os.path.getsize)
    :return:
    - level: str
    - realname: str
    - name: str
    - index: int
    - size: int
    """
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

    try:
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
    except ValueError:
        raise Exceptions.HeaderError(f"Invalid header", "get_db_header")

    if name is None or realname is None:
        raise Exceptions.BufferError(f"Too few bytes in headers", "get_db_header")

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
    """
    Make file compatible with the given
    level.
    :param level:
    One of
    C: compressed,
    S: secured,
    X: secured and compressed,
    N: normal
    :param file:
    Ordinary `BufferReader` (rb)
    :param size:
    Total file size
    :return:
    Compatible file `BufferReader`
        """
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
    """
    Make file compatible with the given
    level.
    :param level:
    One of
    C: compressed,
    S: secured,
    X: secured and compressed,
    N: normal
    :param file:
    Ordinary `BufferWriter` (wb)
    :param size:
    Total file size
    :return:
    Compatible file `BufferWriter`
    """
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
    if h == "!":  # Indicate PIN
        t = 1
        borrow, value, idx = get_pin_value(file, idx, size)
        name = f'PIN{len(structures.index)}'
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


def local_prep_load(filename):
    size = os.path.getsize(filename)
    file = open(filename, 'rb')
    return file, size


def remote_prep_load(db_acc: RemoteDataBaseAccessor):
    return db_acc.open(), db_acc.size


def __load(file, size, maintain_borrows, RDA):
    with file:
        level, realname, name, idx, size = get_db_header(file, size)
        database = DataBase(name, realname, RDA)
        file = level_decompile(level, file, size - idx)
        while size > idx:
            head = file.read(1)
            idx += 1
            match head:
                case b"=":  # Cluster
                    cluster, idx = load_cluster_A(file, idx, size, maintain_borrows)
                    database.add(cluster)
                case b"?":  # Jar (Pickle)
                    jar, idx = open_jar_A(file, idx, size, maintain_borrows)
                    database.add(jar)
                case b"":  # EOF
                    break
                case _:
                    raise Exceptions.SubHeadError(f"Invalid sub-header '{head.decode()}'", "load")

    return database


def clean():
    """
    Clean the `index`
    """
    global part
    part = {}
    structures.index = {}


def load(filename, maintain_borrows=False, _clean=True, RDA: RemoteDataBaseAccessor = None):
    """
    Load an entire database as a `DataBase` object.
    (Everything is loaded into memory, so not optimal)
    :param filename:
    Filename of the database
    :param maintain_borrows:
    Load a borrowed element as a `Borrow`, otherwise load
    as the object to be borrowed
    :param _clean:
    Destroy object index (please enable)
    :param RDA:
    RemoteDataBaseAccessor used for accessing a file on the remote server,
    leave None if it is a local file
    :return:
    A fully loaded `DataBase`
    """
    try:
        if not RDA:
            file, size = local_prep_load(filename)
        else:
            file, size = remote_prep_load(RDA)
        loaded = __load(file, size, maintain_borrows, RDA)
        if _clean:
            clean()
        return loaded
    except zst.ZstdError:
        raise Exceptions.CorruptionError(f"Compressed-Database '{filename}' is not in ZST format", "load")
