import unittest
from unittest.mock import MagicMock, patch
from typing import Union, List, Dict, Any, Optional

from fluxdb import FluxDB

def first_doc(result: Union[Dict[str, Any], List[Dict[str, Any]], None]) -> Optional[Dict[str, Any]]:
    """Helper to safely get the first document from a result."""
    if isinstance(result, list) and result:
        return result[0]
    elif isinstance(result, dict):
        return result
    return None

class TestFluxDB(unittest.TestCase):

    def setUp(self):
        with patch('fluxdb.client.FluxDB.connect'):
            self.db = FluxDB()
        self.db.sock = MagicMock()

    def test_connection(self):
        self.assertIsNotNone(self.db.sock)

    def test_insert_and_get(self):
        self.db._send_command = MagicMock(side_effect=[
            "OK ID=1",  # insert response
            "OK {\"name\":\"Alice\",\"age\":41}"  # get response
        ])

        doc_id = self.db.insert({"name": "Alice", "age": 41})
        self.assertIsNotNone(doc_id)
        self.assertEqual(doc_id, 1)

        if doc_id is not None:
            result: Union[Dict[str, Any], List[Dict[str, Any]], None] = self.db.get(doc_id)
            doc = first_doc(result)
            self.assertIsNotNone(doc)
            if doc:
                self.assertEqual(doc["name"], "Alice")
                self.assertEqual(doc["age"], 41)

    def test_find(self):
        self.db._send_command = MagicMock(return_value="OK COUNT=1\nID 1 {\"name\":\"Bob\",\"age\":25}")
        result: Union[Dict[str, Any], List[Dict[str, Any]], None] = self.db.find({"age": {"$gt": 20}})
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        doc = first_doc(result)
        self.assertIsNotNone(doc)
        if doc:
            self.assertEqual(doc["name"], "Bob")

    def test_update_and_delete(self):
        self.db._send_command = MagicMock(side_effect=[
            "OK UPDATED",  # update
            "OK DELETED"   # delete
        ])

        updated = self.db.update(1, {"age": 42})
        self.assertTrue(updated)

        deleted = self.db.delete(1)
        self.assertTrue(deleted)

    def test_stats(self):
        self.db._send_command = MagicMock(return_value='OK {"count": 10, "adaptive": true}')
        stats = self.db.stats()
        self.assertIsInstance(stats, dict)
        self.assertEqual(stats.get("count"), 10)
        self.assertTrue(stats.get("adaptive"))

    def test_toggle_features(self):
        self.db._send_command = MagicMock(side_effect=[
            "ADAPTIVE=ON",
            "PUBSUB=ON"
        ])
        self.assertTrue(self.db.toggle_adaptive(True))
        self.assertTrue(self.db.toggle_pubsub(True))

if __name__ == "__main__":
    unittest.main()
