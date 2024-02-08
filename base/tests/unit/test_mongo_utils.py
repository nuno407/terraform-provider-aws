import pytest
from base.mongo.utils import flatten_dict


@pytest.mark.unit
@pytest.mark.parametrize("input,result",[
    (
        {
            "a":1,
            "b": {"c":2,"d":3}
        },
        {
            "a":1,
            "b.c":2,
            "b.d":3
        }

    ),
    (
        {
            "a":1,
            "b": {"c":{"d":3}}
        },
        {
            "a":1,
            "b.c.d":3
        }

    ),
    ({},{})
])
def test_flatten_dict(input, result):
    assert flatten_dict(input) == result
