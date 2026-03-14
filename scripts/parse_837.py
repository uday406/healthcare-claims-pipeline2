import pandas as pd
import os


def parse_837(filepath):
    claims = []

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    transactions = content.split('ST*837')
    transactions = [t for t in transactions if t.strip()]

    for txn in transactions:
        lines = txn.strip().split('\n')
        claim = {}

        for line in lines:
            line = line.strip().replace('~', '')
            if not line:
                continue

            segments = line.split('*')
            seg_name = segments[0]

            if seg_name == 'NM1' and len(segments) > 8:
                if segments[1] == 'IL':
                    claim['patient_id'] = segments[9].replace('~', '').strip() if len(segments) > 9 else ''
                    last = segments[4] if len(segments) > 4 else ''
                    first = segments[3] if len(segments) > 3 else ''
                    claim['patient_name'] = f"{first} {last}".strip()
                elif segments[1] == '82':
                    claim['doctor_id'] = segments[9].replace('~', '').strip() if len(segments) > 9 else ''
                    last = segments[4] if len(segments) > 4 else ''
                    first = segments[3] if len(segments) > 3 else ''
                    claim['doctor_name'] = f"{first} {last}".strip()

            elif seg_name == 'CLM' and len(segments) > 2:
                claim['claim_id'] = segments[1].replace('~', '').strip()
                claim['billed_amount'] = float(segments[2].replace('~', '')) if segments[2] else 0.0

            elif seg_name == 'DTP' and len(segments) > 3:
                if segments[1] == '435':
                    date = segments[3].replace('~', '').strip()
                    claim['claim_date'] = f"{date[:4]}-{date[4:6]}-{date[6:8]}" if len(date) >= 8 else date

            elif seg_name == 'HI' and len(segments) > 1:
                diag = segments[1].replace('~', '').split(':')
                claim['diagnosis_code'] = diag[1] if len(diag) > 1 else ''

            elif seg_name == 'SV1' and len(segments) > 1:
                proc = segments[1].replace('~', '').split(':')
                claim['procedure_code'] = proc[1] if len(proc) > 1 else ''

        if claim.get('claim_id'):
            claims.append(claim.copy())

    df = pd.DataFrame(claims)
    print(f"✅ EDI 837 Parsed — {len(df)} claims found")
    return df


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filepath = os.path.join(base, 'raw_data', 'claims_837.edi')
    df = parse_837(filepath)
    print(df[['claim_id', 'patient_id']].to_string())