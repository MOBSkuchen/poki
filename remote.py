import paramiko
from exceptions import Exceptions
from dataclasses import dataclass


######### Usage added in later versions ############

@dataclass(frozen=True)
class Address:
    address: str
    port: int

    def __repr__(self):
        return f'{self.address}:{self.port}'

    def __call__(self, *args, **kwargs):
        return self.address, self.port


@dataclass(frozen=True)
class Errors:
    invalid_hash_auth: str = "E1:AUTH_ERR"
    invalid_level_auth: str = "E2:INV_LVL"
    invalid_call_sign: str = "E3:INV_CALL_SIGN"
    non_num_size: str = "E4:NON_NUM_SIZE"


###################################################


@dataclass(frozen=True)
class SSHCredentials:
    """
    Credentials for an SSH-server
    """
    server: str
    username: str
    password: str

    def __repr__(self):
        return f'{self.username}@{self.server}:{self.password}'

    def __call__(self, *args, **kwargs):
        return self.server, self.username, self.password


class RemoteDataBaseAccessor:
    """
    The RemoteDataBaseAccessor (RDA) is a wrapper
    around an SFTP file
    """

    def __init__(self, sftp, filename):
        self.sftp: paramiko.SFTPClient = sftp
        self.filename = filename

    @property
    def size(self):
        info = self.sftp.stat(self.filename)
        return info.st_size

    def open(self, filename=None, mode="rb", bufsize=-1):
        if filename is None:
            filename = self.filename
        return self.sftp.open(filename, mode, bufsize)

    def exists(self, filename=None):
        if filename is None:
            filename = self.filename
        return filename in self.sftp.listdir()

    def __repr__(self):
        return f'{self.filename} [{self.size}]'


class Connector:
    """
    The connector allows to connect to an SSH-server an open databases on it
    """

    def __init__(self, credentials: SSHCredentials, join=None):
        self.server, self.username, self.password = credentials()

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._join = join

        self._connected = False
        self.sftp: paramiko.SFTPClient = None

    def _get_sftp(self):
        self.sftp: paramiko.SFTPClient = self.client.open_sftp()

    def connect(self):
        if self.connected:
            raise Exceptions.AlreadyConnected("Tried to connect to a database which you are already connected to",
                                              "connect")

        self.client.connect(self.server, username=self.username, password=self.password)
        self._get_sftp()
        self.join(self._join)

    def close(self):
        """
        Close connection, can NOT do anything with the connector anymore
        """
        self.client.close()
        self.sftp.close()

    def join(self, folder):
        """
        Join a folder on the remote server,
        for easy access
        :param folder:
        The name of the folder
        """
        self.sftp.chdir(folder)

    def create_RDA(self, filename):
        return RemoteDataBaseAccessor(self.sftp, filename)

    def get_database(self, filename, maintain_borrows=False, _clean=True):
        """
        Get the database as fully loaded by ``tools.load()``
        :param filename:
        Filename of the database
        :param _clean:
        See ``tools.load()``
        :param maintain_borrows:
        See ``tools.load()``
        :return:
        Loaded DataBase
        """
        from tools import load
        RDA = self.create_RDA(filename)
        return load(filename, maintain_borrows, _clean, RDA)

    def open_database(self, name, filename):
        """
        Get the database ``tools.load()``
        :param name:
        Name of the database (may be the filename)
        :param filename:
        Filename of the database
        :return:
        Loaded DataBase
        """
        from structures import DataBase
        RDA = self.create_RDA(filename)
        return DataBase(name, filename, RDA)

    @property
    def connected(self):
        """
        Whether you are connected to the server
        """
        return self._connected

    def __enter__(self):
        self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
