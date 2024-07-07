from tinydb import TinyDB, Query

# Create or open the database file
db = TinyDB('db.json')

# Insert some data
# db.insert({'name': 'John', 'age': 30})
# db.insert({'name': 'Jane', 'age': 25})

# Query the database
User = Query()
result = db.search(User.name == 'John')
print(result)

# Close the database
db.close()
