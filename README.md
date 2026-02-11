# Hospice ADT A01 Integration Service

A minimal, production-ready Python service for ingesting HL7 ADT A01 (Admit) messages, processing hospice-specific business logic, and converting to FHIR Patient resources.

## Features

✅ Parse HL7 ADT A01 messages  
✅ Apply admission business logic (episode status, EOB workflow, SOC/ROC)  
✅ Convert to FHIR Patient resources  
✅ REST API with FastAPI  
✅ PyAirbyte integration (stub for production enhancement)  

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt