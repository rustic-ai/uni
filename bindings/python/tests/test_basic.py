import os
import shutil
import sys
import unittest

# Ensure we can import the module from the current directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni


class TestUni(unittest.TestCase):
    def setUp(self):
        self.test_dir = "./test_db_python"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        self.db = uni.Database(self.test_dir)

    def tearDown(self):
        # We can't easily close the DB in the current bindings,
        # so we might fail to clean up if the DB holds locks.
        # But Uni is embedded, so it should drop when the object is collected.
        del self.db
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_basic_query(self):
        # Create schema
        self.db.create_label("Person")

        # Create a node
        self.db.query("CREATE (n:Person {name: 'Alice', age: 30})")

        # Query it back
        results = self.db.query("MATCH (n:Person) RETURN n.name as name, n.age as age")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Alice")
        self.assertEqual(results[0]["age"], 30)

    def test_params(self):
        # Create schema
        # (Person might be created by other test if running in same DB, but here we use separate dirs or setup)
        # setUp creates fresh DB.
        self.db.create_label("Person")

        # Create using params
        params = {"name": "Bob", "age": 25}
        self.db.query("CREATE (n:Person {name: $name, age: $age})", params)

        # Query back
        # Note: returning 'n' might return VID string in current vectorized engine.
        # We return specific properties to verify params worked.
        results = self.db.query(
            "MATCH (n:Person {name: 'Bob'}) RETURN n.name as name, n.age as age"
        )
        self.assertEqual(len(results), 1)
        row = results[0]
        self.assertEqual(row["name"], "Bob")
        self.assertEqual(row["age"], 25)

    def test_list_and_map(self):
        self.db.create_label("Item")
        # Create a complex node
        # Note: Uni might not support complex nested maps in properties directly if not supported by storage,
        # but let's see if the binding handles passing them.
        # Actually Uni supports JSON/Map properties.

        # We'll just test returning them for now if storage is complex.
        # Let's test passing a list parameter.
        self.db.query("CREATE (n:Item {tags: $tags})", {"tags": ["a", "b"]})

        results = self.db.query("MATCH (n:Item) RETURN n.tags as tags")
        self.assertEqual(len(results), 1)
        # Depending on how list is returned (Uni Value::List -> PyList)
        self.assertEqual(results[0]["tags"], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
