import unittest
from Euclid_work.Quant_Share.warehouse import load_gmData_history


class wareHouseTestCase(unittest.TestCase):
    def test_load_gmData_history(self):
        data = load_gmData_history(
            symbols=["SHSE.600000", "SZSE.002405"], begin="20200101", end="20220101"
        )
        self.assertEqual(True, len(data) > 0)

        data = load_gmData_history(
            symbols="SHSE.600000", begin="20200101", end="20220101"
        )
        self.assertEqual(True, len(data) > 0)

        data = load_gmData_history(symbols="SHSE.600000", begin="20200101")
        self.assertEqual(True, len(data) > 0)

        data = load_gmData_history(begin="20200101")
        self.assertEqual(True, len(data) > 0)

        data = load_gmData_history()
        self.assertEqual(True, len(data) > 0)


if __name__ == "__main__":
    unittest.main()
