""" pydantic model config """
from pydantic import ConfigDict

dataclass_config = ConfigDict(
    validate_assignment=True,
    use_enum_values=True,
    allow_population_by_field_name=True
)
