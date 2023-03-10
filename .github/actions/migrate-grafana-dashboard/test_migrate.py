from migrate import parse_args, __prepare_dump_before_import


def test_parse_args_with_base_url():
    """ Test parse_args function with base URL. """
    args = parse_args(['export', '--base-url', 'http://localhost:3000'])
    assert args.base_url == 'http://localhost:3000'


def test_parse_args_without_base_url():
    """ Test parse_args function without base URL. """
    args = parse_args(['export'])
    assert args.base_url is None


def test_parse_args_with_api_key():
    """ Test parse_args function with API key. """
    args = parse_args(['export', '--api-key', 'my-api-key'])
    assert args.api_key == 'my-api-key'


def test_parse_args_without_api_key():
    """ Test parse_args function without API key. """
    args = parse_args(['export'])
    assert args.api_key is None


def test_parse_args_with_file():
    """ Test parse_args function with file. """
    args = parse_args(['export', '--file', 'test.json'])
    assert args.file == 'test.json'


def test_parse_args_without_file():
    """ Test parse_args function without file. """
    args = parse_args(['export'])
    assert args.file == '.'

def test_prepare_dump_before_import():
    """ Test export dump preparation. """
    export_dump = {
        'id': 1,
        'uid': 'testtest',
        'panels': [
            {
                'some': 'decorative panel'
            },
            {
                'datasource': {
                    'type': 'boschcom-devcloudmongodbgrafanaplugin-datasource',
                    'uid': 'myorigin-uid'
                },
                'targets': [
                    {
                        'aggregation': 'test',
                        'collection': 'dev-recordings',
                        'database': 'DataIngestion',
                        'datasource': {
                            'type': 'boschcom-devcloudmongodbgrafanaplugin-datasource',
                            'uid': 'myoirigin-uid'
                        }
                    }
                ]
            }
        ]
    }
    prepared_export_dump = __prepare_dump_before_import(export_dump, 'qa', 'new-datasource')
    assert prepared_export_dump['id'] is None
    assert prepared_export_dump['uid'] is None
    for panel in export_dump['panels'][1:]:
        assert panel['datasource']['uid'] == 'new-datasource'
        for target in panel['targets']:
            assert target['collection'].startswith('qa')
            assert target['datasource']['uid'] == 'new-datasource'
