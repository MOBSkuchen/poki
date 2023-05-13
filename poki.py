import io
from exceptions import Exceptions
from structures import Atom, Cluster, Jar, Pin
from remote import Connector, SSHCredentials


def create_connector():
    con = Connector(SSHCredentials("server.addr", "username", "password"))
    con.connect()
    return con


con = create_connector()


def db_make():
    _ = Atom("hello", "world")
    parts = [_, Atom("msg", _.borrow())]
    cluster = Cluster("words", parts)
    cluster.add(Pin("hello"))
    db = con.open_database("database", "database.db")

    db.add(cluster)
    jar = Jar("string", io.StringIO("Hello World, string!!"))  # The size of this is 69, nice
    db.add(jar)

    db.export()


def main():
    db_make()
    db = con.open_database("database", "database.db")
    element = db.load(name="words")
    element.add(element[1].borrow())

    gen = db.find("words")
    for i in gen:
        print(i)

    print(db)

    con.close()


if __name__ == '__main__':
    try:
        main()
    except Exceptions.BaseException as ex:
        try:
            print(f'{ex.__class__.__name__} [{ex.head}] : {ex.msg} (in {ex.level})')
        except RecursionError as ex:
            print(f'Internal Error (while formatting error message) : {str(ex)}')
    except Exception as ex:
        print(f'Internal Error : {str(ex)}')
        raise ex
