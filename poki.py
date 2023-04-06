import io
from exceptions import Exceptions
from structures import Atom, Cluster, DataBase, Jar, Pin
from tools import load


def db_make():
    _ = Atom("hello", "world")
    parts = [_, Atom("msg", _.borrow())]
    cluster = Cluster("words", parts)
    cluster.add(Pin("hello"))

    db = DataBase("database", "database.db")
    db.add(cluster)
    jar = Jar("string", io.StringIO("Hello World, string!!"))  # The size of this is 69, nice
    db.add(jar)

    db.export(compression=True)


def main():
    db_make()
    db = DataBase("database", "database.db")
    element = db.load(name="words")
    element.add(element[1].borrow())


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
