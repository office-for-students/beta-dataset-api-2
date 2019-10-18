import unittest

from SharedCode.string_validator import StringValidator


class TestGetStringValidator(unittest.TestCase):
    def test_string_validator_with_valid_type(self):
        self.assertTrue(StringValidator.is_valid_type("valid_string_type"))


if __name__ == "__main__":
    unittest.main()
