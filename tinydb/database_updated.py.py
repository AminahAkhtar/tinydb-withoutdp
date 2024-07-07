from tinydb import TinyDB, JSONStorage
from tinydb.storages import Storage, MemoryStorage
from tinydb.table import Table, Document
from tinydb.utils import with_typehint
from typing import Dict, Iterator, Set, Type, Callable
import unittest
import os
import shutil
import json  # Import the json module


# The table's base class. This is used to add type hinting from the Table
# class to TinyDB. Currently, this supports PyCharm, Pyright/VS Code and MyPy.
TableBase: Type[Table] = with_typehint(Table)


class ExtendedStorage(Storage):
    """
    Base class for storages with transaction and backup/restore functionalities.
    """
    def begin_transaction(self) -> None:
        raise NotImplementedError("begin_transaction not implemented")

    def commit_transaction(self) -> None:
        raise NotImplementedError("commit_transaction not implemented")

    def rollback_transaction(self) -> None:
        raise NotImplementedError("rollback_transaction not implemented")

    def backup(self, backup_path: str) -> None:
        raise NotImplementedError("backup not implemented")

    def restore(self, backup_path: str) -> None:
        raise NotImplementedError("restore not implemented")


class ExtendedMemoryStorage(MemoryStorage, ExtendedStorage):
    def __init__(self):
        super().__init__()
        self._transaction_backup = None

    def begin_transaction(self) -> None:
        data = self.read() or {}  # Ensure we get a dictionary
        self._transaction_backup = data.copy()  # Create a deep copy of the data for rollback

    def commit_transaction(self) -> None:
        self._transaction_backup = None  # Clear the backup after commit

    def rollback_transaction(self) -> None:
        if self._transaction_backup is not None:
            self.write(self._transaction_backup)  # Restore from the backup
            self._transaction_backup = None  # Clear the backup

    def backup(self, backup_path: str) -> None:
        data = self.read()
        if data is not None:
            with open(backup_path, 'w') as f:
                json.dump(data, f)

    def restore(self, backup_path: str) -> None:
        if os.path.exists(backup_path):
            with open(backup_path, 'r') as f:
                data = json.load(f)
                self.write(data)


class ExtendedJSONStorage(JSONStorage, ExtendedStorage):
    def __init__(self, path: str, **kwargs):
        super().__init__(path, **kwargs)
        self._transaction_backup_path = path + '.bak'

    def begin_transaction(self) -> None:
        shutil.copyfile(self._handle.name, self._transaction_backup_path)

    def commit_transaction(self) -> None:
        if os.path.exists(self._transaction_backup_path):
            os.remove(self._transaction_backup_path)

    def rollback_transaction(self) -> None:
        if os.path.exists(self._transaction_backup_path):
            shutil.copyfile(self._transaction_backup_path, self._handle.name)
            os.remove(self._transaction_backup_path)

    def backup(self, backup_path: str) -> None:
        shutil.copyfile(self._handle.name, backup_path)

    def restore(self, backup_path: str) -> None:
        shutil.copyfile(backup_path, self._handle.name)


class TinyDB(TableBase):
    """
    The main class of TinyDB.
    """
    table_class = Table
    default_table_name = '_default'
    default_storage_class = ExtendedJSONStorage

    def __init__(self, *args, **kwargs) -> None:
        storage = kwargs.pop('storage', self.default_storage_class)
        self._storage: Storage = storage(*args, **kwargs)
        self._opened = True
        self._tables: Dict[str, Table] = {}
        self._schemas: Dict[str, dict] = {}

    def __repr__(self):
        args = [
            'tables={}'.format(list(self.tables())),
            'tables_count={}'.format(len(self.tables())),
            'default_table_documents_count={}'.format(self.__len__()),
            'all_tables_documents_count={}'.format(
                ['{}={}'.format(table, len(self.table(table)))
                 for table in self.tables()]),
        ]
        return '<{} {}>'.format(type(self).__name__, ', '.join(args))

    def table(self, name: str, **kwargs) -> Table:
        if name in self._tables:
            return self._tables[name]
        table = self.table_class(self.storage, name, **kwargs)
        self._tables[name] = table
        return table

    def tables(self) -> Set[str]:
        return set(self.storage.read() or {})

    def drop_tables(self) -> None:
        self.storage.write({})
        self._tables.clear()

    def drop_table(self, name: str) -> None:
        if name in self._tables:
            del self._tables[name]
        data = self.storage.read()
        if data is None:
            return
        if name not in data:
            return
        del data[name]
        self.storage.write(data)

    @property
    def storage(self) -> Storage:
        return self._storage

    def close(self) -> None:
        self._opened = False
        self.storage.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._opened:
            self.close()

    def __getattr__(self, name):
        return getattr(self.table(self.default_table_name), name)

    def __len__(self):
        return len(self.table(self.default_table_name))

    def __iter__(self) -> Iterator[Document]:
        return iter(self.table(self.default_table_name))

    def begin_transaction(self) -> None:
        self.storage.begin_transaction()

    def commit_transaction(self) -> None:
        self.storage.commit_transaction()

    def rollback_transaction(self) -> None:
        self.storage.rollback_transaction()

    def backup(self, backup_path: str) -> None:
        self.storage.backup(backup_path)

    def restore(self, backup_path: str) -> None:
        self.storage.restore(backup_path)

    def set_schema(self, table_name: str, schema: dict) -> None:
        self._schemas[table_name] = schema

    def validate_document(self, table_name: str, document: dict) -> bool:
        schema = self._schemas.get(table_name, {})
        return all(field in document for field in schema)

    def insert(self, document: dict) -> None:
        self.table(self.default_table_name).insert(document)

    def remove(self, cond: Callable) -> None:
        self.table(self.default_table_name).remove(cond)


# Unit tests

class TestTinyDB(unittest.TestCase):
    def setUp(self):
        self.db = TinyDB(storage=ExtendedMemoryStorage)
        self.db.set_schema('test', {'name': 'str', 'age': 'int'})

    def tearDown(self):
        self.db.close()

    def test_begin_commit_rollback_transaction(self):
        # Test rollback
        self.db.begin_transaction()
        self.db.insert({'name': 'John', 'age': 30})
        self.assertEqual(len(self.db), 1)
        self.db.rollback_transaction()
        self.assertEqual(len(self.db), 0)

        # Test commit
        self.db.begin_transaction()
        self.db.insert({'name': 'Jane', 'age': 25})
        self.assertEqual(len(self.db), 1)
        self.db.commit_transaction()
        self.assertEqual(len(self.db), 1)

    def test_backup_restore(self):
        self.db.insert({'name': 'John', 'age': 30})
        backup_path = 'test_backup.json'
        self.db.backup(backup_path)

        self.db.drop_tables()
        self.assertEqual(len(self.db), 0)

        self.db.restore(backup_path)
        self.assertEqual(len(self.db), 1)
        os.remove(backup_path)

    def test_set_validate_schema(self):
        valid_document = {'name': 'Alice', 'age': 22}
        invalid_document = {'name': 'Bob'}
        self.assertTrue(self.db.validate_document('test', valid_document))
        self.assertFalse(self.db.validate_document('test', invalid_document))

    def test_insert_remove_document(self):
        self.db.insert({'name': 'Alice', 'age': 22})
        self.assertEqual(len(self.db), 1)
        self.db.remove(lambda doc: doc['name'] == 'Alice')
        self.assertEqual(len(self.db), 0)

# Create a new TinyDB instance with extended memory storage
db = TinyDB(storage=ExtendedMemoryStorage)

# Set a schema for a table
db.set_schema('users', {'name': 'str', 'age': 'int'})

# Insert a document
db.insert({'name': 'Alice', 'age': 30})

# Validate a document
print(db.validate_document('users', {'name': 'Alice', 'age': 30}))  # Should print True

# Begin a transaction
db.begin_transaction()
db.insert({'name': 'Bob', 'age': 25})

# Rollback the transaction
db.rollback_transaction()

# Check the number of documents
print(len(db))  # Should print 1 (only Alice is present)

# Backup the database
db.backup('backup.json')

# Drop all tables
# db.drop_tables()

# Restore the database from the backup
db.restore('backup.json')

# Close the database
db.close()

if __name__ == '__main__':
    unittest.main()
