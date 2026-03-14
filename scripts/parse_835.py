import pandas as pd
import os

def clean(value):
    # Remove ~ and whitespace from EDI segment endings
    return value.strip().replace('~', '')

def parse_835(filepath):
    payments = []

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    transactions = content.split('ST*835')
    transactions = [t for t in transactions if t.strip()]

    for txn in transactions:
        lines = txn.strip().split('\n')
        payment = {}
        payer_name = ''

        for line in lines:
            line = clean(line)
            if not line:
                continue

            segments = line.split('*')
            seg_name = segments[0]

            # N1 — payer details
            if seg_name == 'N1' and len(segments) > 2:
                if segments[1] == 'PR':
                    payer_name = clean(segments[2])

            # CLP — claim payment details
            elif seg_name == 'CLP' and len(segments) > 6:
                if payment.get('claim_id'):
                    payments.append(payment.copy())

                payment = {}
                payment['claim_id']      = clean(segments[1])
                payment['claim_status']  = clean(segments[2])
                payment['billed_amount'] = float(clean(segments[3])) if clean(segments[3]) else 0.0
                payment['paid_amount']   = float(clean(segments[4])) if clean(segments[4]) else 0.0
                payment['denied_amount'] = float(clean(segments[5])) if clean(segments[5]) else 0.0
                payment['payer_name']    = payer_name

                status_map = {
                    '1':  'Approved',
                    '2':  'Approved — Adjusted',
                    '3':  'Approved — Partial',
                    '4':  'Denied',
                    '19': 'Pending',
                    '22': 'Reversed',
                }
                payment['status'] = status_map.get(clean(segments[2]), 'Unknown')

            # NM1 — patient details
            elif seg_name == 'NM1' and len(segments) > 8:
                if segments[1] == 'QC':
                    last  = clean(segments[4])
                    first = clean(segments[3])
                    payment['patient_name'] = f"{first} {last}".strip()
                    payment['patient_id']   = clean(segments[9]) if len(segments) > 9 else ''

            # DTM — dates
            elif seg_name == 'DTM' and len(segments) > 2:
                date = clean(segments[2])
                formatted = f"{date[:4]}-{date[4:6]}-{date[6:8]}" if len(date) >= 8 else date
                if segments[1] == '150':
                    payment['service_date'] = formatted
                elif segments[1] == '151':
                    payment['payment_date'] = formatted

            # CAS — adjustment
            elif seg_name == 'CAS' and len(segments) > 3:
                payment['adjustment_reason'] = clean(segments[2])
                amt = clean(segments[3])
                payment['adjustment_amount'] = float(amt) if amt else 0.0

        if payment.get('claim_id'):
            payments.append(payment.copy())

    df = pd.DataFrame(payments)
    print(f"✅ EDI 835 Parsed — {len(df)} payment records found")
    return df


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filepath = os.path.join(base, 'raw_data', 'payments_835.edi')
    df = parse_835(filepath)
    print(df.to_string())