"""Helper module to manage and assert mongo and voxel docs"""
import os
import json
from datetime import datetime
from typing import Any
import fiftyone as fo
from base.testing.utils import load_relative_json_file
from motor.motor_asyncio import AsyncIOMotorClient


def __serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def assert_json_dict(real_dict: dict, expected_dict: dict):
    """
    This saves all the dict into a string and convert them back into dict.
    This bypasses JSON limitation of not having a date type.

    The conversion back to a dictionary is to avoid any issues with the reposition of the fields.

    Args:
        real_dict (dict): _description_
        expected_dict (dict): _description_
    """
    converted_real = json.loads(json.dumps(real_dict, default=__serialize_datetime))
    converted_expected = json.loads(json.dumps(expected_dict, default=__serialize_datetime))
    assert converted_real == converted_expected


def filter_variant_fields(data: dict[str, Any]) -> dict[str, Any]:
    """
    Filter the variant fields to only keep the ones that are relevant for the test.
    """
    filtered_dict = {}
    for key, val in data.items():
        if key == "_id":
            continue
        if isinstance(val, dict):
            filtered_dict[key] = filter_variant_fields(val)
            continue
        if isinstance(val, list):
            lst = []
            for item in val:
                if isinstance(item, dict):
                    lst.append(filter_variant_fields(item))
                else:
                    lst.append(item)
            filtered_dict[key] = lst
            continue

        filtered_dict[key] = val
    return filtered_dict


async def dump_mongo_db(mongo_client: AsyncIOMotorClient):
    """ Helper function that can be used to dump the mongo database into a file for debug purposes"""
    dump_data = {}
    # Dump everything from mongo into a file
    for database in await mongo_client.list_database_names():
        db_dump = {}
        for collection in await mongo_client[database].list_collection_names():
            docs = await mongo_client[database][collection].find().to_list(length=None)
            real_docs = list(docs)
            for doc in real_docs:
                del doc["_id"]
            db_dump[collection] = real_docs

        dump_data[database] = db_dump

    with open("mongo_dump.json", "w", encoding="utf-8") as file:
        json.dump(dump_data, file, default=__serialize_datetime)


async def assert_mongo_state(file_name: str, mongo_client: AsyncIOMotorClient, debug=False):
    """
    Checks if a json file is presented in the database.
    The json file has to contain the following format:
    {
        <database_name_1>: {
            <collection_name_1> : ANY,
            <collection_name_2> : ANY
        },
        <database_name_2>: {
            <collection_name_1> : ANY,
        }

    }
    Where ANY can be any structure supported by the mongo engine.

    To be noted that it will only check the collection that are present in the json file.
    If those are only present in the mongo database, no assert will be done against it.

    Args:
        file_name (str): Name of the expected json file (present under test_data)
        mongo_client (MongoMockClient): A mock for the mongo client
        debug (bool, optional): If true, will dump the current mongo state into a file for debug purposes.
    """
    if debug:
        await dump_mongo_db(mongo_client)

    mongo_expected_data: dict[str, dict[str, Any]] = load_relative_json_file(
        __file__, os.path.join("test_data/mongo_states", file_name))

    for db_name, db_entry in mongo_expected_data.items():
        for col_name, collection in db_entry.items():
            docs = await mongo_client[db_name][col_name].find().to_list(length=None)
            real_docs = list(docs)
            for doc in real_docs:
                del doc["_id"]

            assert_json_dict(real_docs, collection)


def dump_all_voxel_datasets_to_file():
    """
    Dump all the datasets into a json file.
    This can be used for debug purposes.
    """
    datasets = fo.list_datasets()
    dataset_dict = {}
    for dataset_name in datasets:
        dataset = fo.load_dataset(dataset_name)
        dataset_dict[dataset_name] = {
            "tags": dataset.tags,
            "samples": [sample.to_dict() for sample in dataset]
        }

    dataset_dict = filter_variant_fields(dataset_dict)

    with open("voxel_dump.json", "w", encoding="utf-8") as file:
        json.dump(dataset_dict, file, indent=4, default=__serialize_datetime)


def assert_voxel_state(file_name: str, debug=False):
    """
    Checks if a json file is presented in the database.
    The json file has to contain the following format:
    {
        <dataset_name_1>: {
            tags : [<tage_name_1>, <tage_name_2>]
            samples : {
                {<field_name_1>: <value_field_1>},
                {<field_name_2>: <value_field_2>, <field_name_3>: <value_field_3>}
            }
        }
        <dataset_name_2>: {
            tags : []
            samples : {}
        }

    }
    To be noted that only field present in the each sample (in the file) will be asserted against Voxel.

    Args:
        file_name (str): Name of the expected json file (present under test_data)
        debug (bool, optional): If true, will dump the current voxel state into a file for debug purposes.
    """

    if debug:
        dump_all_voxel_datasets_to_file()

    voxel_expected_data: dict[str, dict[str, Any]] = load_relative_json_file(
        __file__, os.path.join("test_data/voxel_states", file_name))

    assert len(voxel_expected_data) > 0

    # Loop trough datasets
    for dataset_name, expected_dataset in voxel_expected_data.items():
        real_dataset = fo.load_dataset(dataset_name)
        expected_tags = expected_dataset.get("tags", [])
        assert expected_tags == real_dataset.tags

        # Loop trough samples
        for expected_sample in expected_dataset.get("samples", []):
            real_sample = real_dataset.one(fo.ViewField("filepath") == expected_sample["filepath"])
            real_sample_dict = filter_variant_fields(real_sample.to_dict())
            assert_json_dict(real_sample_dict, expected_sample)
