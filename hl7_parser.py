def parse_adt_a01(raw_hl7_message):
    """
    Parse HL7 ADT A01 message.
    Extracts key patient demographic and admission information.
    
    Args:
        raw_hl7_message (str): Raw HL7 message as string
    
    Returns:
        dict: Parsed patient and admission data
    """
    try:
        print(f"[HL7_PARSER] Starting to parse HL7 message...")
        
        # Step 1: Normalize newlines
        clean_hl7 = raw_hl7_message.replace('\r\n', '\r').replace('\n', '\r').strip()
        
        print(f"[HL7_PARSER] Message cleaned and normalized")
        
        # Step 2: Split into segments using \r delimiter
        segments = clean_hl7.split('\r')
        
        print(f"[HL7_PARSER] Total segments found: {len(segments)}")
        print(f"[HL7_PARSER] Segment types: {[seg.split('|')[0] if seg else 'EMPTY' for seg in segments]}")
        
        # Step 3: Parse segments manually (avoid hl7 library issues)
        segment_dict = {}
        for segment_line in segments:
            if not segment_line.strip():
                continue
            
            fields = segment_line.split('|')
            seg_type = fields[0]
            
            if seg_type not in segment_dict:
                segment_dict[seg_type] = []
            segment_dict[seg_type].append(fields)
        
        print(f"[HL7_PARSER] Parsed segment types: {list(segment_dict.keys())}")
        
        # Verify required segments
        if 'MSH' not in segment_dict:
            raise ValueError("MSH segment not found")
        if 'PID' not in segment_dict:
            raise ValueError("PID segment not found")
        
        msh_fields = segment_dict['MSH'][0]
        pid_fields = segment_dict['PID'][0]
        pv1_fields = segment_dict.get('PV1', [[]])[0] if 'PV1' in segment_dict else []
        dg1_fields = segment_dict.get('DG1', [[]])[0] if 'DG1' in segment_dict else []
        
        print(f"[HL7_PARSER] MSH fields count: {len(msh_fields)}")
        print(f"[HL7_PARSER] PID fields count: {len(pid_fields)}")
        
        # Step 4: Extract PID fields
        # PID-3: Patient ID (MRN) - at index 3
        patient_id = "UNKNOWN"
        if len(pid_fields) > 3:
            pid_3 = pid_fields[3]
            # Handle format like: 10006579^^^1^MRN^1
            if pid_3:
                patient_id = pid_3.split('^')[0] if '^' in pid_3 else pid_3
        
        print(f"[HL7_PARSER] Extracted Patient ID: {patient_id}")
        
        # PID-5: Name (at index 5) - format: DUCK^DONALD^D
        family_name = "Unknown"
        given_name = "Unknown"
        if len(pid_fields) > 5:
            pid_5 = pid_fields[5]
            if pid_5:
                name_parts = pid_5.split('^')
                family_name = name_parts[0] if len(name_parts) > 0 else "Unknown"
                given_name = name_parts[1] if len(name_parts) > 1 else "Unknown"
        
        print(f"[HL7_PARSER] Extracted Name: {family_name}, {given_name}")
        
        # PID-7: DOB (at index 7) - format: 19241010
        dob = ""
        if len(pid_fields) > 7:
            dob = pid_fields[7]
        
        print(f"[HL7_PARSER] Extracted DOB: {dob}")
        
        # PID-8: Gender (at index 8) - format: M or F
        gender = ""
        if len(pid_fields) > 8:
            gender = pid_fields[8].upper() if pid_fields[8] else ""
        
        print(f"[HL7_PARSER] Extracted Gender: {gender}")
        
        # PID-11: Address (at index 11) - format: 111 DUCK ST^^FOWL^CA^999990000^^M
        address = ""
        if len(pid_fields) > 11:
            pid_11 = pid_fields[11]
            if pid_11:
                addr_parts = pid_11.split('^')
                street = addr_parts[0] if len(addr_parts) > 0 else ""
                city = addr_parts[2] if len(addr_parts) > 2 else ""
                state = addr_parts[3] if len(addr_parts) > 3 else ""
                zip_code = addr_parts[4] if len(addr_parts) > 4 else ""
                
                address_components = [s for s in [street, city, state, zip_code] if s]
                address = ", ".join(address_components)
        
        print(f"[HL7_PARSER] Extracted Address: {address}")
        
        # PID-13: Phone (at index 13)
        phone = ""
        if len(pid_fields) > 13:
            phone = pid_fields[13]
        
        print(f"[HL7_PARSER] Extracted Phone: {phone}")
        
        # PV1-2: Admission DateTime (at index 2)
        admit_datetime = ""
        if len(pv1_fields) > 2:
            admit_datetime = pv1_fields[2]
        
        print(f"[HL7_PARSER] Extracted Admit DateTime: {admit_datetime}")
        
        # DG1-3: Diagnosis (at index 3)
        primary_diagnosis = ""
        if len(dg1_fields) > 3:
            primary_diagnosis = dg1_fields[3]
        
        print(f"[HL7_PARSER] Extracted Diagnosis: {primary_diagnosis}")
        
        result = {
            "patient_id": patient_id,
            "family_name": family_name,
            "given_name": given_name,
            "dob": dob,
            "gender": gender,
            "address": address,
            "phone": phone,
            "admit_datetime": admit_datetime,
            "primary_diagnosis": primary_diagnosis,
            "message_type": "A01",
            "raw_hl7": raw_hl7_message
        }
        
        print(f"[HL7_PARSER] ✓ Successfully parsed A01 message for: {family_name}, {given_name}")
        return result
    
    except Exception as e:
        print(f"[HL7_PARSER] ✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise ValueError(f"Failed to parse HL7 message: {str(e)}")