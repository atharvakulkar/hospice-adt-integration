import requests
import json

BASE_URL = "http://127.0.0.1:8000"

# Sample HL7 ADT A01 message
SAMPLE_HL7_MESSAGE = r"""MSH|^~\&|AccMgr|1|||20050110045504||ADT^A01|599102|P|2.3|||
EVN|A01|20050110045502|||||
PID|1||10006579^^^1^MRN^1||DUCK^DONALD^D||19241010|M||1|111 DUCK ST^^FOWL^CA^999990000^^M|1|8885551212|8885551212|1|2||40007716^^^AccMgr^VN^1|123121234|||||||||||NO
PV1|1|I|PREOP^101^1^1^^^S|3|||37^DISNEY^WALT^^^^^^AccMgr^^^^CI|||01||||1|||37^DISNEY^WALT^^^^^^AccMgr^^^^CI|2|40007716^^^AccMgr^VN|4|||||||||||||||||||1||G|||20050110045253||||||
DG1|1|I9|71596^OSTEOARTHROS NOS-L/LEG ^I9|OSTEOARTHROS NOS-L/LEG ||A|"""

def test_health():
    """Test health check endpoint"""
    print("\n" + "="*60)
    print("TEST: Health Check")
    print("="*60)
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    assert response.status_code == 200


def test_ingest_patient_raw():
    """Test /ingest/patient endpoint with raw HL7"""
    print("\n" + "="*60)
    print("TEST: Ingest Patient (Raw HL7)")
    print("="*60)
    
    print(f"\nSending HL7 message:\n{SAMPLE_HL7_MESSAGE}\n")
    
    response = requests.post(
        f"{BASE_URL}/ingest/patient",
        data=SAMPLE_HL7_MESSAGE,
        headers={"Content-Type": "text/plain"}
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"\n✅ SUCCESS!")
        print(f"\nFHIR Patient Response:")
        print(json.dumps(result['fhir_patient'], indent=2))
        print(f"\nAdmission Details:")
        print(json.dumps(result['admission_details'], indent=2))
    else:
        print(f"❌ ERROR: {response.text}")
    
    assert response.status_code == 200


def test_ingest_patient_json():
    """Test /ingest/patient/json endpoint with JSON body"""
    print("\n" + "="*60)
    print("TEST: Ingest Patient (JSON body)")
    print("="*60)
    
    payload = {
        "hl7_message": SAMPLE_HL7_MESSAGE
    }
    
    response = requests.post(
        f"{BASE_URL}/ingest/patient/json",
        json=payload
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"\n✅ SUCCESS!")
        print(f"\nFHIR Patient MRN: {result['fhir_patient']['identifier'][0]['value']}")
        print(f"Episode Status: {result['admission_details']['episode_status']}")
        print(f"SOC/ROC Completed: {result['admission_details']['soc_roc_completed']}")
    else:
        print(f"❌ ERROR: {response.text}")
    
    assert response.status_code == 200


if __name__ == "__main__":
    print("\n" + "🚀 "*30)
    print("HOSPICE ADT INTEGRATION - API TESTS")
    print("🚀 "*30)
    
    try:
        test_health()
        test_ingest_patient_raw()
        test_ingest_patient_json()
        
        print("\n" + "✅ "*30)
        print("ALL TESTS PASSED!")
        print("✅ "*30 + "\n")
    
    except Exception as e:
        print(f"\n❌ TEST FAILED: {str(e)}\n")
        raise