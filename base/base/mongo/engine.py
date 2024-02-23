
from typing import Any, AsyncIterator, Generic, NamedTuple, Optional, Type, TypeVar

from kink import inject
from base.model.base_model import ConfiguredBaseModel
from base.mongo.utils import flatten_dict
from motor.motor_asyncio import AsyncIOMotorClient


T = TypeVar("T", bound=ConfiguredBaseModel)
strDict = dict[str, Any]


class UpdateOneResult(NamedTuple):
    found: bool
    updated: bool


class UpdateManyResult(NamedTuple):
    found: int
    updated: int


@inject
class Engine(Generic[T]):
    def __init__(self, model: Type[T], database: str, collection: str, client: AsyncIOMotorClient):  # type: ignore
        self.__col = client[database][collection]  # type: ignore
        self.__model = model

    @staticmethod
    def dump_model(model: ConfiguredBaseModel) -> dict:
        return model.model_dump(by_alias=True, exclude_none=True)

    async def save(self, item: T):
        await self.__col.insert_one(self.dump_model(item))

    async def save_all(self, items: list[T]):
        dumps = [self.dump_model(item) for item in items]
        await self.__col.insert_many(dumps)

    async def find(self, query: Optional[strDict] = None) -> AsyncIterator[T]:
        if query is None:
            docs = self.__col.find()
        else:
            docs = self.__col.find(query)
        async for doc in docs:
            yield self.__model.model_validate(doc)

    async def find_one(self, query: strDict) -> Optional[T]:
        if (result := await self.__col.find_one(query)) is not None:
            return self.__model.model_validate(result)
        return None

    async def update_one_flatten(self, query: strDict, set_command: T, upsert: bool = False) -> UpdateOneResult:
        """
        Updates one document in the collection.
        Flattens the set_command before updating the document. This ensure that embedded documents
        are not replaced.

        This should be avoided on very large embedded documents, as it can have a significant performance impact.

        WARNING: Calling the set command with empty arrays/objects will override the tarfet field!

        Args:
            query (strDict): The query to find the document to update.
            set_command (strDict): The data that should be updated (Without set).
            upsert (bool, optional): Wheter to do an upsert or just an update. Defaults to False.

        Returns:
            UpdateOneResult: _description_
        """
        command = {
            "$set": flatten_dict(self.dump_model(set_command))
        }
        result = await self.__col.update_one(query, command, upsert)
        return UpdateOneResult(result.matched_count == 1, result.modified_count == 1)

    async def update_one(self, query: strDict, command: strDict, upsert: bool = False) -> UpdateOneResult:
        """
        Updates one document in the collection.

        REMARKS: Any embedded documents will be replaced by the new document.
        """
        result = await self.__col.update_one(query, command, upsert)
        return UpdateOneResult(result.matched_count == 1, result.modified_count == 1)

    async def update_many(self, query: strDict, command: strDict, upsert: bool = False) -> UpdateManyResult:
        """
            Updates multiple document in the collection.

            REMARKS: Any embedded documents will be replaced by the new document.
        """
        result = await self.__col.update_many(query, command, upsert)
        return UpdateManyResult(result.matched_count, result.modified_count)
