import unittest
from airflow.models import DagBag


class TestDAGIntegrity(unittest.TestCase):
    '''Check DAGs are defined correctly'''

    def setUp(self):
        self.dagbag = DagBag('./', include_examples=False)

    def test_no_import_errors(self):
        self.assertEqual(len(self.dagbag.import_errors), 0)