"""
OpenSpecimen Participant API – Automated Test Framework
=======================================================
- Reads test cases from INPUT_FILE (CSV)
- Supports operations: create
- Writes results (pass/fail + error) to OUTPUT_FILE
- Role-based auth via .env
"""

import os, csv, pytest, requests, functools
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE_URL    = os.getenv("OS_BASE_URL", os.getenv("BASE_URL", "https://test.openspecimen.org/rest/ng"))
INPUT_FILE  = os.getenv("INPUT_FILE", "input_participants.csv")
_env_output = os.getenv("OUTPUT_FILE", "output_results.csv")
_base, _ext = os.path.splitext(_env_output)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = f"{_base}_{timestamp}{_ext}"

OUTPUT_EXTRA = ["TC_Status", "Error_Received", "HTTP_Status_Code"]
META_FIELDS  = ["TC_ID", "TC_Description", "Expected_Result", "Role"]

@functools.lru_cache(maxsize=10) # this is decorator and it will modify the function to store cache(token)
def get_token(role: str) -> str:
    key = role.upper()
    login = os.getenv(f"ROLE_{key}_LOGIN_NAME", os.getenv(f"ROLE_{key}_USER", ""))
    pwd   = os.getenv(f"ROLE_{key}_PASSWORD", os.getenv(f"ROLE_{key}_PASS", ""))
    if not login or not pwd:
        raise ValueError(f"No credentials for role '{role}' in .env")
    
    creds = {"loginName": login, "password": pwd}
    if domain := os.getenv(f"ROLE_{key}_DOMAIN_NAME", os.getenv(f"ROLE_{key}_DOMAIN", "")):
        creds["domainName"] = domain
        
    resp = requests.post(f"{BASE_URL}/sessions", json=creds, timeout=10)
    resp.raise_for_status()
    return resp.json()["token"]

def row_to_payload(row: dict) -> dict:
    val = lambda k: row.get(k, "").strip() # lambda function is used to get the value of the key and strip it (how are saved based on the role )

    pmis_dict, races, ethns = {}, [], []
    for k, v in row.items():
        v = v.strip() if v else ""
        if not k or not v: continue
        
        if k.startswith("PMI#") and k.count("#") == 2:
            _, idx, field = k.split("#")
            pmi = pmis_dict.setdefault(idx, {"siteName": "", "mrn": ""})
            pmi["siteName" if field == "Site Name" else "mrn"] = v
        elif k.startswith("Race#"):
            races.append(v)
        elif k.startswith("Ethnicity#"):
            ethns.append(v)
                    
    return {
        "cpShortTitle": val("CP Short Title"), "ppid": val("PPID"),
        "registrationDate": val("Registration Date"), "site": val("Registration Site"),
        "externalSubjectId": val("External Subject ID"), "activityStatus": val("Activity Status") or "Active",
        "participant": {
            "firstName":  val("First Name"), "lastName":   val("Last Name"), "middleName": val("Middle Name"),
            "emailAddress": val("Email Address"), "phoneNumber":  val("Phone Number"),
            "dob":          val("Date Of Birth"), "deathDate":    val("Death Date"), "gender":       val("Gender"),
            "races":        races, "ethnicities":  ethns, "vitalStatus":  val("Vital Status"),
            "nationalId":   val("National ID"), "empi":         val("eMPI"),
            "pmis":         [p for p in pmis_dict.values() if p["siteName"] or p["mrn"]] # returns {"siteName": "Hospital A", "mrn": "123"}
            #  this is the exact format openspecimen needs ...no walkaround needs to be hardcoded 
        }
    }

def execute_tc(row: dict) -> dict:
    result = dict(row, TC_Status="FAIL", Error_Received="", HTTP_Status_Code="")
    try:
        operation = row.get("Operation", "").strip().lower()
        if operation != "create": raise ValueError(f"Unknown operation: '{operation}'")

        token = get_token(row.get("Role", "admin").strip()) # uses admin as default 
        headers = {"X-OS-API-TOKEN": token, "Content-Type": "application/json"}
        
        start = datetime.now()
        resp = requests.post(
            f"{BASE_URL}/collection-protocol-registrations/",
            headers=headers, json=row_to_payload(row), timeout=15
        )

        result["HTTP_Status_Code"] = resp.status_code

        pass_expected = row.get("Expected_Result", "").strip().lower() in ("pass", "p", "success")
        
        if pass_expected == resp.ok:
            result["TC_Status"] = "PASS"
            if not resp.ok: 
                try: result["Error_Received"] = resp.json().get("code", resp.text[:200])
                except: result["Error_Received"] = resp.text[:200]#first 200 char
        else:
            result["Error_Received"] = "Expected failure but API succeeded" if resp.ok else resp.text[:200]
            try:
                if not resp.ok: result["Error_Received"] = resp.json().get("message", resp.json().get("error", resp.text[:200]))
            except: pass

    except ValueError as exc:
        result["TC_Status"] = "SKIP"
        result["Error_Received"] = str(exc)
    except Exception as exc:
        result["Error_Received"] = f"Error: {exc}"

    return result


_results = []

@pytest.fixture(scope="session", autouse=True)
def write_results_after_suite():
    yield
    if not _results: return
    
    base_keys = list(_results[0].keys())
    output_cols = [c for c in META_FIELDS if c in base_keys] + \
                  [c for c in base_keys if c not in META_FIELDS and c not in OUTPUT_EXTRA] + \
                  [c for c in OUTPUT_EXTRA if c in base_keys]
                  
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(_results)
    print(f"\n✅  Results written → {OUTPUT_FILE}")

def pytest_generate_tests(metafunc):
    if "tc_row" in metafunc.fixturenames:
        with open(INPUT_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        metafunc.parametrize("tc_row", rows, ids=[r.get("TC_ID", f"tc_{i}") for i, r in enumerate(rows)])

def test_participant(tc_row):
    result = execute_tc(tc_row)
    _results.append(result)
    if result.get("TC_Status") == "SKIP":
        pytest.skip(result.get("Error_Received", "Skipped"))
    if result["TC_Status"] != "PASS":
        pytest.fail(f"[{tc_row.get('TC_ID')}] {result.get('Error_Received', '')} | HTTP {result.get('HTTP_Status_Code')}", pytrace=False)

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    total   = terminalreporter._numcollected
    passed  = len(terminalreporter.stats.get("passed", []))
    failed  = len(terminalreporter.stats.get("failed", []))
    skipped = len(terminalreporter.stats.get("skipped", []))

    parts = [f"{total} total", f"{failed} failed", f"{passed} passed"]
    if skipped:
        parts.append(f"{skipped} skipped")
    terminalreporter.write_sep("=", ", ".join(parts))

'''
------------------------------------------------------------OUTPUT----------------------------------------------------------
                                                         FAILURES 
__________________________________________________ test_participant[TC_001] __________________________________________________
[TC_001] [{"code":"CPR_DUP_PPID","message":"A participant with same PPID 4563 already exists in this protocol."}] | HTTP 400
                                                   short test summary info 
FAILED test_participants.py::test_participant[TC_001] - Failed: [TC_001] [{"code":"CPR_DUP_PPID","message":"A participant with same PPID 4563 already exists in this protocol."}]...
================================================ 1 failed, 2 passed in 7.95s =================================================
