import logging

from SharedCode.string_validator import StringValidator


def valid_institution_params(params):
    if not mandatory_params_present(("institution_id",), params):
        logging.error(f"Mandatory parameters missing from: {params}")
        return False

    if not is_valid_param("institution_id", params["institution_id"], 8, 8, r"[\d]+$"):
        return False

    return True


def mandatory_params_present(mandatory_params, params):
    return all(k in params for k in mandatory_params)


def is_valid_param(name, param, min_length, max_length, regex):
    """Test that the param looks reasonable."""

    if not StringValidator.is_valid_type(param):
        logging.error(f"{name} is an invalid type - expecting string")
        return False

    string_validator = StringValidator(
        param, min_length=min_length, max_length=max_length, regex=regex
    )

    if not string_validator.is_valid_length():
        logging.error(f"{name} is invalid length {param}")
        return False

    if not string_validator.valid_chars_only():
        logging.error(f"{param} the param for {name} contains invalid characters.")
        return False

    return True
