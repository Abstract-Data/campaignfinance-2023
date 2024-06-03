import funcs.validator_functions as funcs
from functools import partial


ok_date_validation = partial(funcs.validate_date(fmt='%m/%d/%Y'))