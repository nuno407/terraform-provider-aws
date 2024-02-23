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
    (
        {
            "a": [1,{"b":2}]
        },
        {
            "a.0":1,
            "a.1.b":2
        }
    ),
    (
        {
            "a": [1,{},[]],
            "b":[]
        },
        {
            "a.0":1,
            "a.1":{},
            "a.2":[],
            "b":[]
        }
    ),
    ({},{})
])
def test_flatten_dict(input, result):
    assert flatten_dict(input) == result
