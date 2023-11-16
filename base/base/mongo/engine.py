
from typing import Any, AsyncIterator, Generic, NamedTuple, Optional, Type, TypeVar

from kink import inject
from base.model.base_model import ConfiguredBaseModel
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
    def __init__(self, model: Type[T], database: str, collection: str, client: AsyncIOMotorClient):
        self.__col = client[database][collection]
        self.__model = model

    def __dump(self, model: ConfiguredBaseModel) -> dict:
        return model.model_dump(by_alias=True)

    async def save(self, item: T):
        await self.__col.insert_one(self.__dump(item))

    async def save_all(self, items: list[T]):
        dumps = [self.__dump(item) for item in items]
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

    async def update_one(self, query: strDict, command: strDict, upsert: bool = False) -> UpdateOneResult:
        result = await self.__col.update_one(query, command, upsert)
        return UpdateOneResult(result.matched_count == 1, result.modified_count == 1)

    async def update_many(self, query: strDict, command: strDict, upsert: bool = False) -> UpdateManyResult:
        result = await self.__col.update_many(query, command, upsert)
        return UpdateManyResult(result.matched_count, result.modified_count)
