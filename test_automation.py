
"""
OpenSpecimen API — Data-Driven Participant Registration Tests
=============================================================
Reads test cases from participant.csv and validates each against
the OpenSpecimen REST API.  Credentials loaded from .env file.
"""

import csv
import os
import uuid
from pathlib import Path

import pytest
import requests
from dotenv import load_dotenv

# ── Load environment variables ────────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

BASE_URL = os.getenv("OS_BASE_URL")
USERNAME = os.getenv("OS_USERNAME")
PASSWORD = os.getenv("OS_PASSWORD")
DOMAIN = os.getenv("OS_DOMAIN")
CP_ID = int(os.getenv("OS_CP_ID", "2"))

# ── CSV helpers ───────────────────────────────────────────────────────────────

CSV_PATH = Path(__file__).parent / "participant.csv"


def _load_test_cases():
    """Read participant.csv and return a list of (test_id, row_dict) tuples."""
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cases = []
        for row in reader:
            test_id = row.get("Test Case", "unknown")
            cases.append(pytest.param(row, id=test_id))
        return cases


def _build_payload(row):
    """
    Map flat CSV columns → nested OpenSpecimen JSON payload.

    Handles:
      • Top-level registration fields (ppid, registrationDate, etc.)
      • Nested participant object (names, dates, demographics)
      • Multi-value fields (Race, Ethnicity — comma-separated in CSV)
      • PMI list (Site Name + MRN pairs)
    """
    # — Multi-value helpers (comma-separated in CSV) —
    races = [r.strip() for r in row.get("Race", "").split(",") if r.strip()]
    ethnicities = [e.strip() for e in row.get("Ethnicity", "").split(",") if e.strip()]

    # — PMI list —
    pmis = []
    site = row.get("PMI Site Name", "").strip()
    mrn = row.get("PMI MRN", "").strip()
    if site or mrn:
        pmis.append({"siteName": site, "mrn": mrn})

    # — Build participant sub-object —
    participant = {
        "firstName": row.get("First Name", ""),
        "lastName": row.get("Last Name", ""),
    }

    # Only include optional fields when CSV provides a value (keeps payloads lightweight)
    _optional_participant = {
        "middleName": row.get("Middle Name", ""),
        "emailAddress": row.get("Email Address", ""),
        "birthDate": row.get("Date Of Birth", ""),
        "deathDate": row.get("Death Date", ""),
        "gender": row.get("Gender", ""),
        "vitalStatus": row.get("Vital Status", ""),
        "nationalIdentificationNumber": row.get("National ID", ""),
    }
    for key, val in _optional_participant.items():
        if val:
            participant[key] = val

    if races:
        participant["races"] = races
    if ethnicities:
        participant["ethnicities"] = ethnicities
    if pmis:
        participant["pmis"] = pmis

    # — Build top-level payload —
    payload = {
        "cpId": CP_ID,
        "participant": participant,
    }

    _optional_top = {
        "ppid": row.get("PPID", "").strip(),
        "registrationDate": row.get("Registration Date", ""),
        "site": row.get("Registration Site", ""),
        "externalSubjectId": row.get("External Subject ID", ""),
        "activityStatus": row.get("Activity Status", ""),
    }
    for key, val in _optional_top.items():
        if val:
            payload[key] = val

    return payload


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def token():
    """Authenticate once per session and return the API token."""
    url = f"{BASE_URL}/sessions"
    payload = {
        "loginName": USERNAME,
        "password": PASSWORD,
        "domainName": DOMAIN,
    }
    response = requests.post(url, json=payload)
    assert response.status_code == 200, f"Login failed: {response.status_code} — {response.text}"
    print(f"\n✅ Logged in to {BASE_URL}")
    return response.json().get("token")


@pytest.fixture
def headers(token):
    """Build request headers with the auth token."""
    return {
        "Content-Type": "application/json",
        "X-OS-API-TOKEN": token,
    }


@pytest.fixture
def api_url():
    """Registration endpoint URL."""
    return f"{BASE_URL}/collection-protocol-registrations/"


# ── SQL / Error-leak keywords ─────────────────────────────────────────────────

SQL_LEAK_KEYWORDS = [
    "sql", "exception", "stack trace", "syntax error",
    "unexpected token", "ora-", "mysql", "postgresql",
    "hibernate", "jdbc",
]


# ── Test ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("row", _load_test_cases())
def test_participant_registration(row, api_url, headers):
    """
    Data-driven test: one CSV row = one test case.

    • Builds the API payload from the CSV row.
    • POSTs to the registration endpoint.
    • Asserts no SQL / internal-error strings leak in the response.
    • Asserts response status matches the expected outcome.
    """
    expected = row.get("Expected Status", "pass").strip().lower()
    payload = _build_payload(row)

    response = requests.post(api_url, json=payload, headers=headers)
    body = response.text.lower()

    test_label = row.get("Test Case", "unknown")
    print(f"\n── {test_label} ──")
    print(f"   Status : {response.status_code}")
    print(f"   Payload: {payload}")
    print(f"   Response: {response.text[:500]}")

    # ⬤ Assert no SQL / internal error leaks regardless of pass/fail
    for keyword in SQL_LEAK_KEYWORDS:
        assert keyword not in body, (
            f"⚠️  SQL/error keyword '{keyword}' found in response for [{test_label}]"
        )

    # ⬤ Assert expected outcome
    if expected == "pass":
        assert response.status_code in (200, 201), (
            f"❌ [{test_label}] expected success (200/201), got {response.status_code}"
        )
    else:
        assert response.status_code not in (200, 201), (
            f"❌ [{test_label}] expected failure, but got {response.status_code}"
        )


--------------------------------------------------------------------------------------------------OUTPUT--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

✅ Logged in to https://test.openspecimen.org/rest/ng

── Invalid_Registration_Site ──
   Status : 400
   Payload: {'cpId': 2, 'participant': {'firstName': 'John', 'lastName': 'Doe', 'birthDate': '1990-05-20', 'gender': 'Male', 'vitalStatus': 'Alive', 'races': ['White'], 'ethnicities': ['Not Hispanic or Latino']}, 'registrationDate': '2024-01-15', 'site': 'Fake_Hospital_XYZ', 'externalSubjectId': 'EXT-FAIL-04', 'activityStatus': 'Active'}
   Response: [{"code":"SITE_NOT_FOUND","message":"Site specified does not exist."}]
============================================================================================ short test summary info ============================================================================================
FAILED FM_test.py::test_participant_registration[Invalid_Registration_Site] - AssertionError: ❌ [Invalid_Registration_Site] expected success (200/201), got 400
=============================================================================================== 1 failed in 3.31s ===============================================================================================











