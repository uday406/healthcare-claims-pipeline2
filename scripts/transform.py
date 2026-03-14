import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parse_hl7 import parse_hl7
from parse_837 import parse_837
from parse_835 import parse_835

def transform(hl7_path, edi837_path, edi835_path):

    print("\n📂 STEP 1 — Extracting Raw Data...")
    df_patients = parse_hl7(hl7_path)
    df_claims   = parse_837(edi837_path)
    df_payments = parse_835(edi835_path)

    print("\n🧹 STEP 2 — Cleaning Patients Data...")
    # Strip spaces and proper case names
    df_patients['full_name']   = df_patients['full_name'].str.strip().str.title()
    df_patients['city']        = df_patients['city'].str.strip().str.title()
    df_patients['department']  = df_patients['department'].str.strip().str.title()
    df_patients['patient_id']  = df_patients['patient_id'].str.strip().str.upper()
    df_patients['gender']      = df_patients['gender'].str.strip().str.upper()

    # Fill missing values
    df_patients.fillna('Unknown', inplace=True)
    print(f"   ✅ Patients cleaned — {len(df_patients)} records")

    print("\n🧹 STEP 3 — Cleaning Claims Data...")
    df_claims['claim_id']       = df_claims['claim_id'].str.strip().str.upper()
    df_claims['patient_id']     = df_claims['patient_id'].str.strip().str.upper()
    df_claims['patient_name']   = df_claims['patient_name'].str.strip().str.title()
    df_claims['diagnosis_code'] = df_claims['diagnosis_code'].str.strip().str.upper()
    df_claims['procedure_code'] = df_claims['procedure_code'].str.strip().str.upper()

    # Validate claim ID format — should start with CLM
    invalid_claims = df_claims[~df_claims['claim_id'].str.startswith('CLM')]
    if len(invalid_claims) > 0:
        print(f"   ⚠️  {len(invalid_claims)} invalid claim IDs found!")
    else:
        print(f"   ✅ All claim IDs valid")

    # Validate billed amount
    df_claims = df_claims[df_claims['billed_amount'] > 0]
    df_claims.fillna('Unknown', inplace=True)
    print(f"   ✅ Claims cleaned — {len(df_claims)} records")

    print("\n🧹 STEP 4 — Cleaning Payments Data...")
    df_payments['claim_id']     = df_payments['claim_id'].str.strip().str.upper()
    df_payments['patient_id']   = df_payments['patient_id'].str.strip().str.upper()
    df_payments['patient_name'] = df_payments['patient_name'].str.strip().str.title()
    df_payments['payer_name']   = df_payments['payer_name'].str.strip().str.title()
    df_payments['status']       = df_payments['status'].str.strip()

    df_payments.fillna(0, inplace=True)
    print(f"   ✅ Payments cleaned — {len(df_payments)} records")

    print("\n🔗 STEP 5 — Merging Claims with Payments...")
    df_merged = pd.merge(
        df_claims,
        df_payments[['claim_id', 'paid_amount', 'denied_amount',
                     'status', 'payer_name', 'payment_date', 'adjustment_amount']],
        on='claim_id',
        how='left'
    )

    # Fill claims with no payment yet as Pending
    df_merged['status']      = df_merged['status'].fillna('Pending')
    df_merged['paid_amount'] = df_merged['paid_amount'].fillna(0)
    df_merged['payer_name']  = df_merged['payer_name'].fillna('Unknown')
    print(f"   ✅ Merged — {len(df_merged)} records")

    print("\n✅ TRANSFORM COMPLETE!")
    print(f"   Patients  → {len(df_patients)} records")
    print(f"   Claims    → {len(df_claims)} records")
    print(f"   Payments  → {len(df_payments)} records")
    print(f"   Merged    → {len(df_merged)} records")

    return df_patients, df_claims, df_payments, df_merged


if __name__ == "__main__":
    base     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hl7_path = os.path.join(base, 'raw_data', 'adt_patients.hl7')
    e37_path = os.path.join(base, 'raw_data', 'claims_837.edi')
    e35_path = os.path.join(base, 'raw_data', 'payments_835.edi')

    df_patients, df_claims, df_payments, df_merged = transform(hl7_path, e37_path, e35_path)

    print("\n📊 MERGED DATA PREVIEW:")
    print(df_merged[['claim_id','patient_name','billed_amount','paid_amount','status','payer_name']].to_string())