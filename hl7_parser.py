"""
Production-ready HL7 ADT A01 parser using the python-hl7 (hl7) library.

This module focuses on extracting key patient demographic and admission
information from ADT^A01 messages in a robust, testable way.
"""
#updated parser file
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import hl7

logger = logging.getLogger(__name__)


def parse_adt_a01(raw_hl7_message: str) -> Dict[str, Any]:
    """
    Parse an HL7 ADT A01 message using the hl7 library.

    Args:
        raw_hl7_message: Raw HL7 message as string.

    Returns:
        dict: Parsed patient and admission data with keys:
            - patient_id
            - family_name
            - given_name
            - dob (YYYY-MM-DD where possible)
            - gender
            - address
            - phone
            - admit_datetime (ISO 8601 where possible)
            - primary_diagnosis
            - message_type ("A01")
            - raw_hl7 (original message)

    Raises:
        ValueError: If parsing fails or required segments are missing.
    """
    try:
        logger.info("Starting HL7 ADT A01 message parsing")

        if not raw_hl7_message or not raw_hl7_message.strip():
            raise ValueError("Empty HL7 message")

        # Normalize line endings so each segment is on its own line for the parser.
        # Many clients (like Postman) send HL7 with '\n' only; python-hl7 expects '\r'.
        normalized_message = (
            raw_hl7_message.replace("\r\n", "\r").replace("\n", "\r").strip()
        )

        # Let the library handle segment and field parsing
        message = hl7.parse(normalized_message)

        if not message:
            raise ValueError("Parsed HL7 message is empty")

        # Required segments
        msh = _find_segment(message, "MSH")
        pid = _find_segment(message, "PID")

        if msh is None:
            raise ValueError("MSH segment not found")
        if pid is None:
            raise ValueError("PID segment not found")

        # Validate message type is ADT^A01 (MSH-9)
        message_type_field = _get_field(msh, 9)
        if not message_type_field or not str(message_type_field).startswith("ADT^A01"):
            logger.warning("Expected ADT^A01 message type, got '%s'", message_type_field)

        # Patient demographics from PID
        patient_id = _extract_patient_id(pid)
        family_name, given_name = _extract_patient_name(pid)
        # Keep DOB in original HL7 date format (YYYYMMDD...) so downstream
        # converters (e.g., fhir_converter) can handle formatting consistently.
        dob = _get_field(pid, 7)
        gender = (_get_field(pid, 8) or "").upper()
        address = _extract_address(pid)
        phone = _get_field(pid, 13)

        # Admission info from PV1
        pv1 = _find_segment(message, "PV1")
        admit_datetime_raw = ""
        if pv1 is not None:
            # Prefer PV1-44 (Admit Date/Time) if present, else fall back to PV1-2
            admit_datetime_raw = _get_field(pv1, 44) or _get_field(pv1, 2)
        admit_datetime = _format_hl7_datetime(admit_datetime_raw)

        # Diagnosis from DG1
        dg1 = _find_segment(message, "DG1")
        primary_diagnosis = _get_field(dg1, 3) if dg1 is not None else ""

        result = {
            "patient_id": patient_id or "UNKNOWN",
            "family_name": family_name or "Unknown",
            "given_name": given_name or "Unknown",
            "dob": dob or "",
            "gender": gender or "",
            "address": address or "",
            "phone": phone or "",
            "admit_datetime": admit_datetime or "",
            "primary_diagnosis": primary_diagnosis or "",
            "message_type": "A01",
            "raw_hl7": raw_hl7_message,
        }

        logger.info(
            "Successfully parsed ADT A01 for patient: %s, %s",
            result["family_name"],
            result["given_name"],
        )
        return result

    except Exception as e:
        logger.exception("Failed to parse HL7 message: %s", e)
        raise ValueError(f"Failed to parse HL7 message: {str(e)}") from e


def _find_segment(message: Any, segment_id: str) -> Optional[Any]:
    """Find the first segment in the message with the given ID (e.g., 'PID')."""
    try:
        for segment in message:
            if segment and len(segment) > 0 and str(segment[0]) == segment_id:
                return segment
    except Exception:
        logger.exception("Error while searching for segment %s", segment_id)
    return None


def _get_field(segment: Optional[Any], index: int) -> str:
    """
    Safely get a field from a segment by index.

    segment[index] returns an hl7.Container; converting to str gives its ER7 representation.
    """
    if segment is None:
        return ""
    try:
        if len(segment) > index:
            field = segment[index]
            return str(field).strip() if field is not None else ""
    except Exception:
        logger.debug("Error extracting field %s from segment", index, exc_info=True)
    return ""


def _extract_patient_id(pid: Any) -> str:
    """
    Extract patient identifier from PID-3, taking the first component if composite.
    """
    raw = _get_field(pid, 3)
    if not raw:
        return ""
    parts = raw.split("^")
    return parts[0].strip() if parts else raw.strip()


def _extract_patient_name(pid: Any) -> tuple[str, str]:
    """
    Extract family and given name from PID-5 (XPN).
    Format typically: FAMILY^GIVEN^MIDDLE^...
    """
    raw = _get_field(pid, 5)
    if not raw:
        return "", ""
    parts = raw.split("^")
    family = parts[0].strip() if len(parts) > 0 else ""
    given = parts[1].strip() if len(parts) > 1 else ""
    return family, given


def _extract_address(pid: Any) -> str:
    """
    Extract a formatted address string from PID-11 (XAD).
    Format typically: street^other^city^state^zip^...
    """
    raw = _get_field(pid, 11)
    if not raw:
        return ""
    parts = raw.split("^")
    street = parts[0].strip() if len(parts) > 0 else ""
    city = parts[2].strip() if len(parts) > 2 else ""
    state = parts[3].strip() if len(parts) > 3 else ""
    zip_code = parts[4].strip() if len(parts) > 4 else ""
    components = [c for c in (street, city, state, zip_code) if c]
    return ", ".join(components)


def _format_hl7_date(hl7_date: str) -> str:
    """
    Format an HL7 date (YYYYMMDD...) into YYYY-MM-DD, if possible.
    """
    if not hl7_date or len(hl7_date) < 8:
        return hl7_date
    try:
        dt = datetime.strptime(hl7_date[:8], "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.debug("Could not parse HL7 date: %s", hl7_date, exc_info=True)
        return hl7_date


def _format_hl7_datetime(hl7_datetime: str) -> str:
    """
    Format an HL7 datetime (YYYYMMDDHHMMSS...) into ISO 8601 where possible.
    """
    if not hl7_datetime or len(hl7_datetime) < 8:
        return hl7_datetime
    try:
        if len(hl7_datetime) >= 14:
            dt = datetime.strptime(hl7_datetime[:14], "%Y%m%d%H%M%S")
            return dt.isoformat()
        # Fallback to date-only
        dt = datetime.strptime(hl7_datetime[:8], "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.debug("Could not parse HL7 datetime: %s", hl7_datetime, exc_info=True)
        return hl7_datetime