"""
Initial python test
"""
import json
from main import snapshot_path_generator, event_type_identifier


def test_snapshot_path_generator():
    with open('src/tests/artifacts/test_cases.json', 'r') as f:
        test_cases = json.load(f)["snapshot_path_generator"]
    for test_case in test_cases:
        tenant = test_case['tenant']
        device = test_case['device']
        start = test_case['start']
        end = test_case['end']
        generated_paths = snapshot_path_generator(tenant, device, start, end)
        assert (generated_paths == test_case['results'])


def test_event_type_identifier():
    import ast
    with open('src/tests/artifacts/test_cases.json', 'r') as f:
        test_cases = json.load(f)["event_type_identifier"]
    for test_case in test_cases:
        with open(test_case['path'], 'r') as f:
            sqs_message = ast.literal_eval(f.read())
        test_tenant = test_case["tenant"]
        test_eventType = test_case["eventType"]
        eventType, tenant = event_type_identifier(sqs_message)
        assert (eventType == test_eventType)
        assert (tenant == test_tenant)
