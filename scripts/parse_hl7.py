import pandas as pd
import os

def parse_hl7(filepath):
    patients = []

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by MSH — each MSH starts a new message
    messages = content.split('MSH')
    messages = [m for m in messages if m.strip()]

    for msg in messages:
        # Add MSH back
        lines = ('MSH' + msg).strip().split('\n')
        patient = {}

        for line in lines:
            line = line.strip()
            if not line:
                continue

            segments = line.split('|')
            seg_name = segments[0]

            # PID segment — patient details
            if seg_name == 'PID':
                try:
                    # Patient ID
                    pid_field = segments[3] if len(segments) > 3 else ''
                    patient['patient_id'] = pid_field.split('^')[0]

                    # Name
                    name_field = segments[5].split('^') if len(segments) > 5 else []
                    last  = name_field[0] if len(name_field) > 0 else ''
                    first = name_field[1] if len(name_field) > 1 else ''
                    patient['full_name'] = f"{first} {last}".strip()

                    # DOB
                    dob = segments[7] if len(segments) > 7 else ''
                    patient['dob'] = f"{dob[:4]}-{dob[4:6]}-{dob[6:8]}" if len(dob) >= 8 else dob

                    # Gender
                    patient['gender'] = segments[8] if len(segments) > 8 else ''

                    # City
                    addr = segments[11].split('^') if len(segments) > 11 else []
                    patient['city'] = addr[1] if len(addr) > 1 else ''

                except Exception as e:
                    print(f"PID error: {e}")

            # PV1 segment — visit details
            elif seg_name == 'PV1':
                try:
                    dept = segments[3].split('^') if len(segments) > 3 else []
                    patient['department'] = dept[0] if dept else ''

                    doc = segments[7].split('^') if len(segments) > 7 else []
                    patient['doctor_id']   = doc[0] if doc else ''
                    patient['doctor_name'] = f"{doc[2]} {doc[1]}" if len(doc) > 2 else ''

                    admit = segments[44] if len(segments) > 44 else ''
                    patient['admit_date'] = f"{admit[:4]}-{admit[4:6]}-{admit[6:8]}" if len(admit) >= 8 else admit

                    if patient.get('patient_id'):
                        patients.append(patient.copy())

                except Exception as e:
                    print(f"PV1 error: {e}")

    df = pd.DataFrame(patients)
    if not df.empty:
        df = df.drop_duplicates(subset=['patient_id'])
    print(f"✅ HL7 Parsed — {len(df)} unique patients found")
    return df


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filepath = os.path.join(base, 'raw_data', 'adt_patients.hl7')
    df = parse_hl7(filepath)
    print(df.to_string())