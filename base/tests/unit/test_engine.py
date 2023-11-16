from unittest.mock import AsyncMock, MagicMock, Mock
from pydantic import Field
from pytest import fixture, mark
from base.model.base_model import ConfiguredBaseModel
from base.mongo.engine import Engine

DB = "db"
COLLECTION = "collection"


class ModelClass(ConfiguredBaseModel):
    foo: str = Field(alias="_foo")


model_instance = ModelClass(foo="bar")
dict_instance = {"_foo": "bar"}


@mark.unit
class TestEngine:
    @fixture
    def client(self) -> MagicMock:
        return MagicMock()

    @fixture
    def collection(self, client: MagicMock) -> MagicMock:
        return client[DB][COLLECTION]

    @fixture
    def engine(self, client: MagicMock) -> Engine:
        return Engine[ModelClass](ModelClass, DB, COLLECTION, client)

    @mark.asyncio
    async def test_save(self, collection: MagicMock, engine: Engine):
        # GIVEN
        collection.insert_one = AsyncMock()  # type: ignore

        # WHEN
        await engine.save(model_instance)

        # THEN
        collection.insert_one.assert_called_once_with(dict_instance)

    @mark.asyncio
    async def test_save_all(self, collection: MagicMock, engine: Engine):
        # GIVEN
        collection.insert_many = AsyncMock()  # type: ignore

        # WHEN
        await engine.save_all([model_instance, model_instance])

        # THEN
        collection.insert_many.assert_called_once_with([dict_instance, dict_instance])

    @mark.asyncio
    async def test_find_all(self, collection: MagicMock, engine: Engine):
        # GIVEN
        found = MagicMock()
        found.__aiter__.return_value = [dict_instance]
        collection.find.return_value = found  # type: ignore

        # WHEN
        count = 0
        async for res in engine.find():
            result = res
            count += 1

        # THEN
        assert count == 1
        assert result == model_instance
        collection.find.assert_called_once_with()

    @mark.asyncio
    async def test_find_with_filter(self, collection: MagicMock, engine: Engine):
        # GIVEN
        found = MagicMock()
        found.__aiter__.return_value = [dict_instance]
        collection.find.return_value = found  # type: ignore
        query = {'some': 'query'}

        # WHEN
        count = 0
        async for res in engine.find(query):
            result = res
            count += 1

        # THEN
        assert count == 1
        assert result == model_instance
        collection.find.assert_called_once_with(query)

    @mark.asyncio
    async def test_find_one(self, collection: MagicMock, engine: Engine):
        # GIVEN
        collection.find_one = AsyncMock(return_value=dict_instance)  # type: ignore
        query = {'some': 'query'}

        # WHEN
        result = await engine.find_one(query)

        # THEN
        assert result == model_instance
        collection.find_one.assert_called_once_with(query)

    @fixture
    def update_methods(self, request, collection: MagicMock, engine: Engine):
        if request.param == "one":
            collection.update_one = AsyncMock()
            return engine.update_one, collection.update_one
        collection.update_many = AsyncMock()
        return engine.update_many, collection.update_many

    @mark.parametrize("matched", [0, 1, 2])
    @mark.parametrize("modified", [0, 1, 2])
    @mark.parametrize("upsert", [False, True])
    @mark.parametrize("update_methods", ["one", "many"], indirect=True)
    @mark.asyncio
    async def test_update(self, matched: int, modified: int, upsert: bool, update_methods):
        # GIVEN
        engine_update, collection_update = update_methods

        update_result = MagicMock()
        update_result.matched_count = matched
        update_result.modified_count = modified
        collection_update.return_value = update_result
        query = {'some': 'query'}
        command = {'some': 'command'}

        # WHEN
        result = await engine_update(query, command, upsert)

        # THEN
        if isinstance(result.found, bool):
            assert result.found == (matched == 1)
            assert result.updated == (modified == 1)
        else:
            assert result.found == matched
            assert result.updated == modified
        collection_update.assert_called_once_with(query, command, upsert)
