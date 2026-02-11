from fhir.resources.patient import Patient
from fhir.resources.humanname import HumanName
from fhir.resources.identifier import Identifier
from fhir.resources.address import Address
from fhir.resources.contactpoint import ContactPoint
import json

def convert_to_fhir_patient(admission_info):
    """
    Convert admission info to FHIR Patient resource.
    
    Args:
        admission_info (dict): Output from adt_logic.process_admit_logic()
    
    Returns:
        dict: FHIR Patient resource as JSON-serializable dict
    """
    
    print("[FHIR_CONVERTER] Converting to FHIR Patient resource...")
    
    # Create FHIR Patient object
    patient = Patient.construct()
    
    # 1. Identifier (MRN)
    mrn = Identifier()
    mrn.system = "http://hospital.org/mrn"
    mrn.value = admission_info.get("patient_id", "UNKNOWN")
    patient.identifier = [mrn]
    
    # 2. Name
    name = HumanName()
    name.family = admission_info.get("family_name", "Unknown")
    name.given = [admission_info.get("given_name", "Unknown")]
    name.use = "official"
    patient.name = [name]
    
    # 3. Gender
    gender_code = admission_info.get("gender", "").upper()
    if gender_code == "M":
        patient.gender = "male"
    elif gender_code == "F":
        patient.gender = "female"
    else:
        patient.gender = "unknown"
    
    # 4. Birth Date
    dob = admission_info.get("dob", "")
    if dob:
        # Format: YYYYMMDD -> YYYY-MM-DD
        if len(dob) >= 8:
            patient.birthDate = f"{dob[0:4]}-{dob[4:6]}-{dob[6:8]}"
        else:
            patient.birthDate = dob
    
    # 5. Address
    if admission_info.get("address"):
        address = Address()
        address.text = admission_info.get("address")
        address.use = "home"
        patient.address = [address]
    
    # 6. Telecom (Phone)
    if admission_info.get("phone"):
        telecom = ContactPoint()
        telecom.system = "phone"
        telecom.value = admission_info.get("phone")
        telecom.use = "home"
        patient.telecom = [telecom]
    
    # 7. Active status
    patient.active = True
    
    # 8. Custom Extensions for Hospice Admission
    patient.extension = [
        {
            "url": "http://hospice.example.org/StructureDefinition/episode-status",
            "valueString": admission_info.get("episode_status", "CURRENT")
        },
        {
            "url": "http://hospice.example.org/StructureDefinition/soc-roc-completed",
            "valueBoolean": admission_info.get("soc_roc_completed", False)
        },
        {
            "url": "http://hospice.example.org/StructureDefinition/eob-workflow-closed",
            "valueBoolean": admission_info.get("eob_closed_workflow", False)
        },
        {
            "url": "http://hospice.example.org/StructureDefinition/admit-timestamp",
            "valueDateTime": admission_info.get("admit_event_timestamp")
        }
    ]
    
    # Convert to dict (JSON-serializable)
    fhir_dict = json.loads(patient.json())
    
    print(f"[FHIR_CONVERTER] ✓ FHIR Patient created for MRN: {fhir_dict['identifier'][0]['value']}")
    
    return fhir_dict