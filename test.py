import zstandard as zst

with open("database.db", 'rb') as file:
    content = file.read()
    decompd = zst.decompress(content)

print(decompd)
