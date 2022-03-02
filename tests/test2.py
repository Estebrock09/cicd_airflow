import os
import time
import logging
import unittest
from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        DAGS_DIR = os.getenv("AIRFLOW_HOME")
        logging.info(f"DAGs dir : {DAGS_DIR}")
        self.dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            f"DAG import failures. Errors: {self.dagbag.import_errors}",
        )

    def test_import_time(self):
        """Test that all DAGs can be parsed under the threshold time."""
        for dag_id in self.dagbag.dag_ids:
            start = time.time()

            self.dagbag.process_file(self.dagbag.get_dag(dag_id).filepath)

            end = time.time()
            total = end - start

            self.assertLessEqual(total, self.LOAD_SECOND_THRESHOLD)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)
