"""Errors for metadata component."""


class EmptyDocumentQueryResult(Exception):
    """Empty recording query result exception."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class MalformedRecordingEntry(ValueError):
    """Recording entry with missing fields."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
