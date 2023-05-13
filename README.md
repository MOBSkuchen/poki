# Poki
Poki is a database intended to be used as a python module.
It is all-purpose, so good for every project.

# Usage
## Create a database
````python
from structures import DataBase
database = DataBase("MyDB", "my_db.db")
````
## Add stuff to the database
You can add:
- Cluster (collection of Atoms / Pins)
- Jar (a python object)

### Cluster
A Cluster is a collection objects,
Atoms or Pins
````python
from structures import Cluster, Atom, Pin
parts = [
    Atom("msg", "Hello World"),
    Pin("Hello World")
]
cluster = Cluster("cluster", parts)
cluster.add(Atom("msg2", "How are you"))

database.add(cluster)
````
Here we are adding 2 Atoms and 1 Pin to a cluster,
- Atom(msg, Hello World)
- Atom(msg2, How are you)
- Pin(Hello World)

A Pin is carrying one thing, like a string for example.

An Atom is a named value,
basically name = value

### Jar
A Jar is a python object, which is loaded and dumped using pickle

````python
from structures import Jar
jar = Jar("jar", [1, 2, 3, 4, 5])

database.add(jar)
````
Here we are adding a python object like a list for example to the database

## Export a database
Exporting to a file is as simple as calling the `export` method of `database`
````python
database.export()
````
You may also compress and or encrypt (secure) it

To do that just set the flag as true in `database.export()`

## Load a database (fully)
To load from a file just use
````python
from tools import load
database = load("my_db.db")

print(database)
````
This will fully load the database into memory
## Load a database (partially)
With a big database, you may not want to load
the entire database into memory.
````python
from structures import DataBase
database = DataBase("MyDB", "my_db.db")
# Load a Cluster / Jar
database.load(name="cluster")
# Load a certain amount of elements
database.load(amount=2)
````
## Remote DataBases (on SSH)
Using poki->remote.py you can access a
database file on a given SSH-server.
Please note that this is relatively slow
because individual bytes have to be downloaded,
processed and then an answer must be returned.
````python
from remote import Connector, SSHCredentials
con = Connector(SSHCredentials("server.addr", "username", "password"))
con.connect()

db = con.open_database("database", "database.db")
element = db.load(name="words")
element.add(element[1].borrow())

gen = db.find("words")
for i in gen:
    print(i)
````
## Common questions
### What is 'maintain_borrows'?
Maintain borrows is simply whether the borrows in the database should be converted to the actual element when it is loaded. Otherwise it will just have the element as a borrow of the element.