import re


class StringValidator:
    """Checks a string meets certain criteria

    Checks string meets caller's specified criteria
    in terms of type, length, and permitted chars.
    """

    def __init__(self, str_to_be_validated, min_length, max_length, regex):
        self.str_to_be_validated = str_to_be_validated
        self.min_length = min_length
        self.max_length = max_length
        self.regex = regex

    @staticmethod
    def is_valid_type(obj):
        return isinstance(obj, str)

    def is_valid_length(self):
        return self.min_length <= len(self.str_to_be_validated) <= self.max_length

    def valid_chars_only(self):
        return re.match(self.regex, self.str_to_be_validated) is not None
