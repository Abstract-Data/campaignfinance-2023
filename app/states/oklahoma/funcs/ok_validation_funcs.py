from functools import partial

import funcs.validator_functions as funcs

ok_date_validation = partial(funcs.validate_date(fmt='%m/%d/%Y'))
