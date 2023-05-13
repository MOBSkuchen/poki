import copy
import threading
from dataclasses import dataclass
import os
import asyncio
import zstandard as zst
from exceptions import Exceptions
from remote import RemoteDataBaseAccessor
from hashlib import sha1
import pickle

"""
The index is a table of all registered value,
it is [for example] used to address PINs
"""
index = {}


class Borrow:
    """
    A borrowed value is an exact copy of an
    original mini value -> Atom or Pin
    """
    def __init__(self, value):
        self.value: Atom | Pin = value
        self.name = self.value.name

    def get(self):
        return self.value

    def eval(self):
        return f'@{index[self.value.name].name}'.encode()

    def __repr__(self):
        return f'Borrow of {self.value.name}'


class Atom:
    """
    An atom is a named value,
    it assigns a name to a value
    which it can be addressed from
    """
    def __init__(self, name, value):
        _t = type(value)
        if _t == str:
            self.value = value.encode()
        elif _t == Borrow:
            self.value = value.eval()
        else:
            self.value = bytes(value)
        self.name = name

        index[name] = self

    def eval(self):
        return self.name.encode() + b";" + self.value + chr(0).encode()

    def borrow(self):
        return Borrow(self)

    def __repr__(self):
        return f'{self.name}[A]'


class Pin:
    """
    A pin is a value without a name,
    it is addressed by its index
    """
    def __init__(self, value, name=None):
        _t = type(value)
        if _t == str:
            self.value = value.encode()
        elif _t == Borrow:
            self.value = value.eval().encode()
        else:
            self.value = bytes(value)

        if name is None:
            self.name = f'PIN{len(index)}'
        else:
            self.name = name

        index[self.name] = self

    def eval(self):
        return b'!' + self.value + chr(0).encode()

    def borrow(self):
        return Borrow(self)

    def __repr__(self):
        return f'{self.name}[P]'


class Cluster:
    def __init__(self, title, particles: list[Atom | Pin]):
        self.title = title
        self.particles: dict[str, Atom | Pin] = {}
        for p in particles:
            self.particles[p.name] = p

    def add(self, particle: Atom | Pin):
        self.particles[particle.name] = particle

    def delete(self, name):
        del self.particles[name]

    def eval(self):
        x = [at.eval() for n, at in self.particles.items()]
        return b"\n".join(x)

    def __attr_get(self, item):
        if type(item) is int:
            return list(self.particles.keys())[item]
        return item

    def __getitem__(self, item):
        item = self.__attr_get(item)
        return self.particles[item]

    def __delitem__(self, item):
        item = self.__attr_get(item)
        self.delete(item)

    def __contains__(self, item):
        return item in self.particles.keys()

    def __setitem__(self, key, value):
        key = self.__attr_get(key)
        self.particles[key] = value

    def __repr__(self):
        return f"Cluster ('{self.title}') of {len(self.particles)} elements"


class Jar:
    def __init__(self, title, obj):
        self.title = title
        self.obj = obj

    def eval(self):
        return pickle.dumps(self.obj)

    def __repr__(self):
        return repr(self.obj)


@dataclass(frozen=True, order=True)
class DataBaseHeader:
    level: str
    realname: str
    name: str
    idx: int
    size: int


class DataBase:
    def __init__(self, name, location, RDA: RemoteDataBaseAccessor = None):
        self._size = None
        self.__mprocs = {}
        self.name = name
        self.location = location
        self.RDA = RDA
        self._loaded_header = self._load_header()
        self.elements = {}

    def reload(self):
        """
        Reload the entire database
        """
        self._loaded_header = self._load_header()

    def file_open(self, filename, mode, *args):
        """
        Open the file
        :param filename:
        :param mode:
        Mode (like 'r', 'rb', 'r', 'wb')
        :param args:
        Rest of the arguments
        :return:
        Opened file
        """
        if self.RDA is None:
            return open(filename, mode, *args)
        else:
            return self.RDA.open(filename, mode, *args)

    def get_size(self):
        """
        Get (file) size of the DB
        """
        if self.RDA is None:
            self._size = os.path.getsize(self.location) if os.path.exists(self.location) else -1
            return self._size
        else:
            return self.RDA.size if self.RDA.exists() else -1

    def _load_header(self):
        from tools import get_db_header
        self._size = self.get_size()
        if self._size == -1:
            return
        file = self.file_open(self.location, 'rb')
        level, realname, _name, idx, size = get_db_header(file, self._size)
        idx += 1
        dbh = DataBaseHeader(level, realname, _name, idx, size)
        return dbh

    def _get_ready_file(self):
        from tools import level_decompile
        file = self.file_open(self.location, 'r+b')
        level = self._loaded_header.level
        size = self._loaded_header.size
        file.seek(self._loaded_header.idx)
        file = level_decompile(level, file, size)
        return file

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
        """
        Export an entire database to a file
        :param secure:
        Not implemented
        :param compression:
        Whether to compress using ZSTD
        """
        size = 0
        if compression:
            gen = self.assemble()
            zstd = zst.ZstdCompressor()
            with self.file_open(self.location, 'wb') as file:
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
        with self.file_open(self.location, 'wb') as file:
            file.write(self._get_header(secure, compression, None))
            for asm in gen:
                file.write(asm)
                file.write(b'\n')
                size += (len(asm) + 1)

    def _indiv_asm(self, element):
        _t = type(element)
        if _t == Cluster:
            _header = "="
        else:
            _header = "?"
        fmt = element.eval()
        _header += f"{element.title}:{len(fmt)}\n"
        return _header.encode() + fmt

    def assemble(self):
        """
        Assemble the database to a bytes representation
        :return:
        A generator of the sub-headers and their contents
        """
        for element_name in self.elements.keys():
            element = self.elements[element_name]
            yield self._indiv_asm(element)

    def add(self, element):
        """
        Add an element to the database IMR (In-Memory-Representation)
        :param element:
        Element
        """
        self.elements[element.title] = element

    def delete(self, name):
        """
        Delete an element from the database IMR (In-Memory-Representation)
        :param name:
        Name of the element
        """
        del self.elements[name]

    def find(self, name, maintain_borrows=True):
        """
        Find an element (sub-header) in the database
        :param name:
        Name of the sub-header
        :param maintain_borrows:
        Whether to maintain borrows
        :return:
        Generator of results
        """
        from tools import find_sub_header
        size = self._loaded_header.size
        idx = self._loaded_header.idx
        _reg_stream_file = self._get_ready_file()
        _reg_stream_file.seek(idx)
        res = find_sub_header(_reg_stream_file, name)
        file = self._get_ready_file()
        for (start, end) in res:
            file.seek(idx + start)
            _type, _size, idx, _name = self._get_sub_header_item(file, size, start)
            idx += _size
            __idx = copy.copy(idx)
            yield self._construct_sub_header(file, _size, size, __idx, idx, maintain_borrows, _type, _name)

        _reg_stream_file.close()
        file.close()

    def __mproc_getID(self):
        return f'id{len(self.__mprocs)}'

    async def __mproc_asm(self, _id, element) -> bytes:
        def _(_element):
            self.__mprocs[_id] = self._indiv_asm(_element)

        thread = threading.Thread(target=_, args=(element,))
        thread.start()

    def _main_update_block(self, file, idx, blocksize, filesize, blockname, asm):
        _asm_size = len(asm)
        _header_size = len(str(blocksize)) + len(blockname) + 2
        file.seek(idx - _header_size, 0)
        idx = idx - _header_size
        saved = copy.copy(idx)
        if _asm_size == blocksize:  # Same size
            file.write(asm)
        elif _asm_size > blocksize:  # New one bigger than old one
            idx += blocksize + _header_size + 1
            file.seek(idx, 0)
            mid = file.read(filesize - idx)
            file.seek(saved)
            file.write(asm)
            file.write(mid)
        else:  # New one smaller than old one
            file.write(asm)
            ts = (blocksize + _header_size + 1) - _asm_size
            idx += _asm_size
            rest_size = filesize - idx
            file.seek(idx + ts)
            mid = file.read(rest_size)
            file.seek(idx)
            file.write(mid)
            idx += len(mid)
            file.truncate(idx)

    def update(self, name, element=None, check=False):
        """
        Update a specified element
        NOTE : Please only use if you have small changes,
        it is more effective to use `export` for
        big changes.
        :param check:
        Whether to check if the block is the same,
        can save a lot of time if the time taken
        to write the change is significantly bigger
        than the time it takes to do a hash compare.
        """
        _id = self.__mproc_getID()
        if element is None:
            element = self.elements[name]

        asyncio.run(self.__mproc_asm(_id, element))
        level = self._loaded_header.level
        size = self._loaded_header.size
        idx = self._loaded_header.idx
        file = self._get_ready_file()
        while True:
            _type, _size, idx, _name = self._get_sub_header_item(file, size, idx)
            if name == _name:
                break
            else:
                file.seek(_size + 1, 1)

        asm = self.__mprocs[_id]
        if check and self._hash_compare(file, _size, asm, level == "N"):
            return

        if not level == "N":
            raise Exceptions.UnsupportedError(f"The level '{level}' is not supported (might get changed in the future)",
                                              "update_all")
        self._main_update_block(file, idx, _size, size, _name, asm)

        file.close()

    def __attr_get(self, item):
        if type(item) is int:
            return list(self.elements.keys())[item]
        return item

    def __getitem__(self, item):
        item = self.__attr_get(item)
        return self.elements[item]

    def __delitem__(self, item):
        item = self.__attr_get(item)
        self.delete(item)

    def __contains__(self, item):
        return item in self.elements.keys()

    def __setitem__(self, key, value):
        key = self.__attr_get(key)
        self.elements[key] = value

    @staticmethod
    def _hash_compare(file, size, cmp, rewindable, with_header=True):
        if with_header:
            header, cmp = cmp.split(b"\n", 1)
        _cmp = sha1(cmp)
        loaded = file.read(size)
        hashed = sha1(loaded)
        if rewindable:
            file.seek(-size, 1)  # Normally we could just use file.seek(-size, 1), but we must support compression
        return _cmp.digest() == hashed.digest()

    def update_all(self, check=True):
        """
        Update all elements
        NOTE : Please only use if you have small changes,
        it is more effective to use `export` for
        big changes.
        :param check:
        Whether to check if the block is the same,
        can save a lot of time if the time taken
        to write the change is significantly bigger
        than the time it takes to do a hash compare.
        """
        level = self._loaded_header.level
        size = self._loaded_header.size
        idx = self._loaded_header.idx
        file = self._get_ready_file()
        while True:
            _id = self.__mproc_getID()
            _type, _size, idx, _name = self._get_sub_header_item(file, size, idx)
            if _type is None:
                break
            asyncio.run(self.__mproc_asm(_id, self.elements[_name]))
            asm = self.__mprocs[_id]
            if check and self._hash_compare(file, _size, asm, level == "N"):
                return
            if not level == "N":
                raise Exceptions.UnsupportedError(f"The level '{level}' is not supported", "update_all")
            self._main_update_block(file, idx, _size, size, _name, asm)
        file.close()

    def _get_sub_header_item(self, file, size, idx):
        from tools import get_sub_header
        while size > idx:
            head = file.read(1).decode()
            idx += 1
            match head:
                case "=":  # Cluster
                    idx, _size, _name = get_sub_header(file, idx, size)
                    t = "="
                case "?":  # Jar (Pickle)
                    idx, _size, _name = get_sub_header(file, idx, size)
                    t = "?"
                case "":  # EOF
                    return None, None, idx, None
                case _:
                    print(head.encode())
                    raise Exceptions.SubHeadError(f"Invalid sub-header '{head}'", "_get_sub_header_item")

            return t, _size, idx, _name

    def _construct_sub_header(self, file, _size, size, __idx, idx, mt_br, _type, name):
        from tools import load_cluster_B, open_jar_B
        match _type:
            case "=":  # Cluster
                return load_cluster_B(name, mt_br, file, _size, idx, size, __idx)[0]
            case "?":  # Jar (Pickle)
                return open_jar_B(file, name, _size, idx)[0]

    def _get_sub_header_by_name(self, name, file, size, idx, mt_br):
        while True:
            _type, _size, idx, _name = self._get_sub_header_item(file, size, idx)
            if name == _name:
                break
            else:
                file.read(_size + 1)
                idx += _size + 1
        idx += _size
        __idx = copy.copy(idx)
        return self._construct_sub_header(file, _size, size, __idx, idx, mt_br, _type, _name)

    def _get_sub_headers_by_amount(self, amount, file, size, idx, mt_br):
        for i in range(amount):
            _type, _size, idx, _name = self._get_sub_header_item(file, size, idx)
            idx += _size
            __idx = copy.copy(idx)
            yield self._construct_sub_header(file, _size, size, __idx, idx, mt_br, _type, _name)

    def __gen_load(self, file, amount, size, idx, maintain_borrows):
        for i in self._get_sub_headers_by_amount(amount, file, size, idx, maintain_borrows):
            self.add(i)
            yield i

        file.close()

    def __repr__(self):
        res = f"Database {self.name} ({self.get_size()}) at {self.location}"
        if self.RDA:
            res += " [REMOTE]"
        res += "\n"
        res += f"ELEMENTS (LOADED) : {len(self.elements)}\n"
        return res

    def load(self, name=None, amount=None, maintain_borrows=False):
        """
        Load one or more sub-header items from a database file,
        the items are automatically added to self.elements.
        Please note : When using a generator, it must be consumed until
                      the items are added.

                      Either name or amount must be specified!
        :param name:
        Sub-header name
        :param amount:
        Amount of elements to load
        :param maintain_borrows:
        Maintain borrows (for Clusters)
        :return:
        When searching for name:
            Loaded `Cluster` or `Jar`
        When using amount:
            Generator of loaded `Cluster` or `Jar`
        """
        if name == amount is None or name and amount:
            raise Exception
        size = self._loaded_header.size
        idx = self._loaded_header.idx
        file = self._get_ready_file()
        if name:
            i = self._get_sub_header_by_name(name, file, size, idx, maintain_borrows)
            self.add(i)
            return i
        else:
            gen = self.__gen_load(file, amount, size, idx, maintain_borrows)
            return gen
