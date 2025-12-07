import unittest
from fluxdb import FluxDB

class TestFluxDB(unittest.TestCase):

    def setUp(self):
        self.db = FluxDB()
        self.db.create_index("username", 0)

    def tearDown(self):
        self.db.close()

    def test_insert_data(self):
        user = {"username": "TestUser", "role": "User", "level": 10}
        user_id = self.db.insert(user)
        self.assertIsNotNone(user_id, "Failed to insert data")

    def test_find_data(self):
        user = {"username": "FindUser", "role": "User", "level": 15}
        user_id = self.db.insert(user)
        results = self.db.find({"username": "FindUser"})
        self.assertIn(user_id, results, f"User ID {user_id} not found in results")

    def test_update_data(self):
        user = {"username": "UpdateUser", "role": "User", "level": 20}
        user_id = self.db.insert(user)
        updated_data = {"username": "UpdateUser", "role": "SuperUser", "level": 30}
        update_success = self.db.update(user_id, updated_data)
        self.assertTrue(update_success, "Failed to update user data")
        results = self.db.find({"username": "UpdateUser"})
        self.assertIn(user_id, results, "User ID not found after update")

    def test_delete_data(self):
        user = {"username": "DeleteUser", "role": "User", "level": 5}
        user_id = self.db.insert(user)
        delete_success = self.db.delete(user_id)
        self.assertTrue(delete_success, "Failed to delete user data")
        results = self.db.find({"username": "DeleteUser"})
        self.assertNotIn(user_id, results, "Deleted user found in database")

    def test_insert_duplicate_data(self):
        user1 = {"username": "DuplicateUser", "role": "User", "level": 10}
        user2 = {"username": "DuplicateUser", "role": "User", "level": 5}
        user1_id = self.db.insert(user1)
        user2_id = self.db.insert(user2)
        self.assertNotEqual(user1_id, user2_id, "Duplicate insert returned the same ID")

if __name__ == "__main__":
    unittest.main()
