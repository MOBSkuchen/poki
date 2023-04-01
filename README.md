# Poki
Poki is a database intended to be used as a python module.
It is all-purpose, so good for every project.

# Usage
## Step 1 : Create a database
````python
from structures import DataBase
database = DataBase("MyDB", "my_db.db")
````
## Step 2 : Add stuff
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

## Step 3 : Export
Exporting to a file is as simple as calling the `export` method of `database`
````python
database.export()
````
You may also compress and or encrypt (secure) it

To do that just set the flag as true in `database.export()`

## Step 4 : Load
To load from a file just use
````python
from tools import load
database = load("my_db.db")

print(database)
````
This will fully load the database into memory