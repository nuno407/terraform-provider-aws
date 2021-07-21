"""
This file have the responsability of
creating an mock endpoint to test the
API requests
"""


def test_alive(application, client):
    """
    Assert true if the status code is 200

    Arguments:
        Application
        Client
    Return:
    """
    del application
    res = client.get('/alive')
    assert res.status_code == 200

def test_ready(application, client):
    """
    Assert true if the status code is 200

    Arguments:
        Application
        Client
    Return:
    """
    del application
    res = client.get('/ready')
    assert res.status_code == 200

'''
# Test not yet implemented
def test_get_all_data(application, client):
    """
    Assert true if the status code is 200

    Arguments:
        Application
        Client
    Return:
    """
    del application
    res = client.get('/anonymized')
    assert res.status_code == 200
'''