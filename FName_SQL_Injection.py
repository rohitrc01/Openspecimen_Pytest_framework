# the code use pytest to test for possible sql injection in First name for a participant 

import pytest
import requests

BASE_URL = "https://test.openspecimen.org/rest/ng"

USERNAME = "rohit@krishagni.com"  
PASSWORD = "Manchester@123"
DOMAIN = "KSPL-LDAP"      


@pytest.fixture(scope="session")
def token():
    url = f"{BASE_URL}/sessions"

    payload = {
        "loginName": USERNAME,
        "password": PASSWORD,
        "domainName": DOMAIN    
    }

    response = requests.post(url, json=payload)

    assert response.status_code == 200, "Login failed"

    return response.json().get("token")


@pytest.fixture
def headers(token):
    return {
        "Content-Type": "application/json",
        "X-OS-API-TOKEN": token
    }


@pytest.fixture
def api_url():
    return f"{BASE_URL}/collection-protocol-registrations/"


def test_sql_injection_add_participant(api_url, headers):
    malicious_input = "'; DROP TABLE catissue_collection_protocol_registrations; --"

    payload = {
        "cpId": 2,   
        "ppid": 3457,
        "registrationDate": "2024-01-01",
        "participant": {
            "firstName": malicious_input,
            "lastName": "Test",
            "gender": "Male",
            "vitalStatus": "Alive",
            "birthDate": "1995-05-20"
        }
    }

    response = requests.post(api_url, json=payload, headers=headers)

    print("\nStatus Code:", response.status_code)
    print("Response:", response.text)

    # Injection should not break system
    assert response.status_code in [200, 201]



'''
platform linux -- Python 3.12.3, pytest-9.0.2, pluggy-1.6.0 -- /home/rohit/Desktop/Pytest/venv/bin/python3
cachedir: .pytest_cache
rootdir: /home/rohit/Desktop/Pytest
collected 1 item                                                                                                                                                                                                            

new.py::test_sql_injection_add_participant PASSED                                                                                                                                                                     [100%]
'''
