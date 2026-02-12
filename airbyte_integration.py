"""
Airbyte integration (simulated).

This module is intentionally integration-ready but does NOT call real Airbyte libraries yet.
It provides production-grade structure: logging, validation, env-based config, and retries.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid int in env var %s=%r; using default=%s", name, raw, default)
        return default


def _validate_fhir_patient_resource(fhir_patient: Any) -> Dict[str, Any]:
    if not isinstance(fhir_patient, dict):
        raise ValueError("FHIR Patient resource must be a dict")
    if fhir_patient.get("resourceType") != "Patient":
        raise ValueError("FHIR Patient resourceType must be 'Patient'")

    identifiers = fhir_patient.get("identifier")
    if not isinstance(identifiers, list) or not identifiers:
        raise ValueError("FHIR Patient.identifier must exist and not be empty")

    first = identifiers[0]
    if not isinstance(first, dict) or not first.get("value"):
        raise ValueError("FHIR Patient.identifier[0].value must exist and not be empty")

    return fhir_patient


def _extract_mrn(fhir_patient: Dict[str, Any]) -> str:
    identifiers = fhir_patient.get("identifier") or []
    for ident in identifiers:
        if isinstance(ident, dict) and ident.get("value"):
            return str(ident["value"])
    # Should not happen if validated, but keep defensive.
    return "UNKNOWN"


def airbyte_send_fhir_patient(fhir_patient):
    """
    Send FHIR Patient resource to an Airbyte destination (SIMULATED).

    Environment configuration:
    - AIRBYTE_ENABLED: default "true"
    - AIRBYTE_STREAM_NAME: default "fhir_patients"
    - AIRBYTE_MAX_RETRIES: default 3
    - AIRBYTE_TIMEOUT: default 5 (seconds)

    Returns:
        dict: {
          "status": "success" | "failed" | "skipped",
          "mrn": "...",
          "stream": "...",
          "attempts": number,
          "error": optional
        }
    """

    enabled = _env_bool("AIRBYTE_ENABLED", True)
    stream = os.getenv("AIRBYTE_STREAM_NAME") or "fhir_patients"
    max_retries = max(1, _env_int("AIRBYTE_MAX_RETRIES", 3))
    timeout_s = max(1, _env_int("AIRBYTE_TIMEOUT", 5))

    patient = _validate_fhir_patient_resource(fhir_patient)
    mrn = _extract_mrn(patient)

    if not enabled:
        logger.info("Airbyte integration disabled; skipping send (mrn=%s, stream=%s)", mrn, stream)
        return {"status": "skipped", "mrn": mrn, "stream": stream, "attempts": 0}

    last_error: Optional[str] = None
    attempts = 0

    for attempt in range(1, max_retries + 1):
        attempts = attempt
        logger.info("Sending FHIR Patient to Airbyte (simulated) (mrn=%s, stream=%s, attempt=%s/%s)", mrn, stream, attempt, max_retries)
        try:
            # Simulated send: validate and "wait" for a bounded amount of time.
            # In real production integration, replace the following block with actual Airbyte CDK/Destination write.
            #
            # Example placeholder (DO NOT IMPLEMENT YET):
            # - build AirbyteRecordMessage with stream name + patient data
            # - write to destination
            time.sleep(min(0.01, float(timeout_s)))  # tiny sleep to emulate work without slowing tests

            logger.info("Airbyte send successful (simulated) (mrn=%s, stream=%s)", mrn, stream)
            return {"status": "success", "mrn": mrn, "stream": stream, "attempts": attempts}
        except Exception as e:
            last_error = str(e)
            logger.warning(
                "Airbyte send attempt failed (simulated) (mrn=%s, stream=%s, attempt=%s/%s, error=%s)",
                mrn,
                stream,
                attempt,
                max_retries,
                last_error,
            )
            if attempt < max_retries:
                # Simple retry backoff (bounded, no overengineering).
                time.sleep(0.2)

    logger.error("Airbyte send failed after retries (mrn=%s, stream=%s, attempts=%s, error=%s)", mrn, stream, attempts, last_error)
    return {"status": "failed", "mrn": mrn, "stream": stream, "attempts": attempts, "error": last_error or "Unknown error"}