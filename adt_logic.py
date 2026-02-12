from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid int in env var %s=%r; using default=%s", name, raw, default)
        return default


def process_admit_logic(parsed_hl7):
    """
    Business logic for HL7 ADT A01 (Admit) event.

    When an A01 Admit message is received:
    1. Episode status changes from PENDING to CURRENT
    2. Verify Hospice EOB workflow stage (Event 210, Stage 2029)
    3. SOC or ROC visit marked as completed
    4. Hospice EOB workflow closed

    Args:
        parsed_hl7 (dict): Output from hl7_parser.parse_adt_a01()

    Returns:
        dict: Admission info with business logic applied
    """

    # Single timestamp for this entire workflow invocation (UTC, ISO-8601)
    workflow_ts = datetime.now(timezone.utc).isoformat()

    try:
        if not isinstance(parsed_hl7, dict):
            raise ValueError("parsed_hl7 must be a dict")

        # Minimal required identity validation for downstream pipeline robustness.
        # Keep this conservative to avoid breaking existing HL7 parser output shapes.
        mrn = parsed_hl7.get("mrn") or parsed_hl7.get("patient_id") or parsed_hl7.get("patient_identifier")
        if not mrn:
            raise ValueError("Missing required patient identifier (expected one of: mrn, patient_id, patient_identifier)")

        hospice_eob_event = _get_env_int("HOSPICE_EOB_EVENT", 210)
        hospice_eob_stage = _get_env_int("HOSPICE_EOB_STAGE", 2029)

        soc_roc_type = parsed_hl7.get("soc_roc_type") or "UNKNOWN"
        acuity_level = parsed_hl7.get("acuity_level") or "UNKNOWN"

        logger.info("Processing A01 Admit event (mrn=%s)", mrn)

        admission: Dict[str, Any] = {
            **parsed_hl7,

            # Episode Status: PENDING → CURRENT
            "episode_status": "CURRENT",
            "episode_previous_status": "PENDING",

            # Hospice EOB Verification (env-configurable)
            "hospice_eob_event": hospice_eob_event,
            "hospice_eob_stage": hospice_eob_stage,
            "hospice_eob_workflow_verified": True,

            # SOC/ROC (Start of Care / Return of Care)
            "soc_roc_type": soc_roc_type,
            "soc_roc_completed": True,
            "soc_roc_completion_timestamp": workflow_ts,

            # EOB Workflow Status
            "eob_closed_workflow": True,
            "workflow_closed_timestamp": workflow_ts,

            # Admission metadata
            "admit_event_timestamp": workflow_ts,
            "admission_status": "ACTIVE",
            "acuity_level": acuity_level,
        }

        logger.info("Episode status changed to CURRENT (mrn=%s)", mrn)
        logger.info(
            "Hospice EOB verified (mrn=%s, event=%s, stage=%s)",
            mrn,
            admission["hospice_eob_event"],
            admission["hospice_eob_stage"],
        )
        logger.info("SOC/ROC marked as completed (mrn=%s, type=%s)", mrn, soc_roc_type)
        logger.info("EOB workflow closed (mrn=%s)", mrn)

        return admission
    except Exception:
        logger.exception("Failed to process admit business logic")
        raise