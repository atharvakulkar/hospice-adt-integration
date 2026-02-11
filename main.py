from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import traceback

from hl7_parser import parse_adt_a01
from adt_logic import process_admit_logic
from fhir_converter import convert_to_fhir_patient
from airbyte_integration import airbyte_send_fhir_patient

app = FastAPI(
    title="Hospice ADT A01 Integration Service",
    description="Ingest HL7 ADT A01 (Admit) messages and convert to FHIR Patient resources",
    version="1.0.0"
)

# Enable CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Hospice ADT Integration"}


@app.post("/ingest/patient")
async def ingest_patient(request: Request):
    """
    POST endpoint to ingest HL7 ADT A01 (Admit) message.
    
    Expected Body: Raw HL7 ADT A01 message as plain text
    
    Returns: FHIR Patient resource as JSON
    """
    try:
        # Step 1: Read raw HL7 message
        raw_hl7 = await request.body()
        raw_hl7 = raw_hl7.decode('utf-8').strip()
        
        if not raw_hl7:
            raise HTTPException(status_code=400, detail="Empty HL7 message")
        
        print(f"[INFO] Received HL7 message:\n{raw_hl7}\n")
        
        # Step 2: Parse HL7 A01 message
        print("[INFO] Parsing HL7 ADT A01 message...")
        parsed_hl7 = parse_adt_a01(raw_hl7)
        print(f"[INFO] Parsed patient: {parsed_hl7['family_name']}, {parsed_hl7['given_name']}")
        
        # Step 3: Apply Admission Business Logic
        print("[INFO] Processing admission business logic...")
        admission_info = process_admit_logic(parsed_hl7)
        print(f"[INFO] Episode status: {admission_info['episode_status']}")
        print(f"[INFO] SOC/ROC completed: {admission_info['soc_roc_completed']}")
        print(f"[INFO] EOB workflow closed: {admission_info['eob_closed_workflow']}")
        
        # Step 4: Convert to FHIR Patient resource
        print("[INFO] Converting to FHIR Patient resource...")
        fhir_patient = convert_to_fhir_patient(admission_info)
        print(f"[INFO] FHIR Patient created with MRN: {fhir_patient['identifier'][0]['value']}")
        
        # Step 5: Send via PyAirbyte (demo)
        print("[INFO] Sending to Airbyte destination...")
        airbyte_send_fhir_patient(fhir_patient)
        print("[INFO] Successfully sent to Airbyte")
        
        # Step 6: Return FHIR Patient JSON
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "HL7 ADT A01 processed successfully",
                "fhir_patient": fhir_patient,
                "admission_details": {
                    "episode_status": admission_info['episode_status'],
                    "hospice_eob_event": admission_info['hospice_eob_event'],
                    "hospice_eob_stage": admission_info['hospice_eob_stage'],
                    "soc_roc_completed": admission_info['soc_roc_completed'],
                    "eob_closed_workflow": admission_info['eob_closed_workflow']
                }
            }
        )
    
    except HTTPException as e:
        print(f"[ERROR] HTTP Exception: {e.detail}")
        raise e
    
    except Exception as e:
        print(f"[ERROR] Exception: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error processing HL7 message: {str(e)}"
        )


@app.post("/ingest/patient/json")
async def ingest_patient_json(request: Request):
    """
    Alternative endpoint that accepts JSON body instead of raw HL7.
    
    Expected JSON body:
    {
        "hl7_message": "MSH|^~\\&|..."
    }
    """
    try:
        body = await request.json()
        raw_hl7 = body.get("hl7_message", "").strip()
        
        if not raw_hl7:
            raise HTTPException(status_code=400, detail="Missing 'hl7_message' field in JSON body")
        
        # Re-use the same logic as /ingest/patient
        print(f"[INFO] Received JSON-wrapped HL7 message")
        
        parsed_hl7 = parse_adt_a01(raw_hl7)
        admission_info = process_admit_logic(parsed_hl7)
        fhir_patient = convert_to_fhir_patient(admission_info)
        airbyte_send_fhir_patient(fhir_patient)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "HL7 ADT A01 processed successfully",
                "fhir_patient": fhir_patient,
                "admission_details": {
                    "episode_status": admission_info['episode_status'],
                    "hospice_eob_event": admission_info['hospice_eob_event'],
                    "hospice_eob_stage": admission_info['hospice_eob_stage'],
                    "soc_roc_completed": admission_info['soc_roc_completed'],
                    "eob_closed_workflow": admission_info['eob_closed_workflow']
                }
            }
        )
    
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"[ERROR] Exception: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error processing HL7 message: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)