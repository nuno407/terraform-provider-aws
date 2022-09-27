from datetime import datetime, timedelta
from mdfparser import main

def test_extract_timestamps():
    # GIVEN
    last_hour = int((datetime.now() - timedelta(hours=1)).timestamp()*1000)
    now = int(datetime.now().timestamp()*1000)
    test_filename = 'tenant_device_abcd_' + str(last_hour) + '_' + str(now) + '_metadata_full.json'

    # WHEN
    ts_from, ts_to = main.extract_timestamps(test_filename)

    # THEN
    assert(ts_from == last_hour)
    assert(ts_to == now)

