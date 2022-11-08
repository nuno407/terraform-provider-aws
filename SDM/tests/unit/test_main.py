"""
Initial python test
"""
import json
import pytest
from sdm.main import identify_file
from unittest.mock import Mock

@pytest.mark.unit
def test_identify_file():
    #GIVEN
    s3_path = "Debug_Lync/ridecare_companion_trial_rc_srx_prod_5bfe33136f2a43afc3f534f535d402af175914e2_InteriorRecorder_1667387817727_1667388470705.mp4"

    #WHEN
    res = identify_file(s3_path)
    #THEN
    expected = "Debug_Lync","ridecare_companion_trial_rc_srx_prod_5bfe33136f2a43afc3f534f535d402af175914e2_InteriorRecorder_1667387817727_1667388470705.mp4","mp4"
    assert(res == expected)
