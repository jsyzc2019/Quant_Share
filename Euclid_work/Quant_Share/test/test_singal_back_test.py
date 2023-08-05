import os
import unittest
from pathlib import Path

os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
import numpy as np
import pandas as pd
from Euclid_work.Quant_Share.signal_analysis.signal_back_test import back_test
from Euclid_work.Quant_Share.H5DataSet import H5DataSet


class MyTestCase(unittest.TestCase):
    def test_write_to_cache(self):
        data = pd.DataFrame(
            data=np.array([[1, 2, 3], [2, 1, 3], [3, 1, 2]]),
            index=pd.date_range(start="2020-01-01", end="2020-01-03", freq="D"),
            columns=["A", "B", "C"],
        )
        target_path = Path("h5_file", "demo1.h5")
        if target_path.exists():
            os.remove(target_path)
        back_test.write_to_cache(data, "h5_file", "demo1", "key1")
        self.assertEqual(True, target_path.exists())

    def test_write_to_cache_add(self):
        data = pd.DataFrame(
            data=np.array([[1, 2, 3], [2, 1, 3], [3, 1, 2]]),
            index=pd.date_range(start="2020-01-01", end="2020-01-03", freq="D"),
            columns=["A", "B", "C"],
        )
        # del cache
        target_path = Path("h5_file", "demo1.h5")
        if target_path.exists():
            os.remove(target_path)
        # test write a pivoted table to h5 cache
        back_test.write_to_cache(data, "h5_file", "demo1", "key1")
        self.assertEqual(
            True,
            target_path.exists(),
        )
        # test add a pivoted table to h5 cache
        back_test.write_to_cache(data, "h5_file", "demo1", "key2", add=True)
        self.assertEqual(
            True,
            "key1" in H5DataSet(target_path).known_data
            and "key2" in H5DataSet(target_path).known_data,
        )
        # test rewrite h5 cache
        back_test.write_to_cache(data, "h5_file", "demo1", "key2", rewrite=True)
        self.assertEqual(False, "key1" in H5DataSet(target_path).known_data)

        # test rewrite a pivoted table in h5 cache
        new_data = data * 2
        back_test.write_to_cache(
            new_data, "h5_file", "demo1", "key2", rewrite=True, add=True
        )
        self.assertEqual(
            True,
            np.array_equal(new_data.values, H5DataSet(target_path)["key2"]),
        )


if __name__ == "__main__":
    unittest.main()
