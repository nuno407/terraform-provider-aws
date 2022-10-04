from datetime import datetime
from math import log10


def from_epoch_seconds_or_milliseconds(epoch_value: int) -> datetime:
    if epoch_value > 0:
        number_of_digits = int(log10(epoch_value))+1
    elif epoch_value == 0:
        number_of_digits = 1
    else:
        number_of_digits = int(log10(-epoch_value))+1

    if(number_of_digits >= 12):
        return datetime.fromtimestamp(epoch_value/1000.0)
    else:
        return datetime.fromtimestamp(epoch_value)
