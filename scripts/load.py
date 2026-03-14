import pyodbc
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transform import transform

def get_connection():
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=.\\SQLEXPRESS;'
        'DATABASE=HealthcareDB;'
        'Trusted_Connection=yes;'
    )
    return conn
def load_patients(cursor, df_patients):
    print("\n📥 Loading Patients...")
    count = 0
    for _, row in df_patients.iterrows():
        try:
            cursor.execute("""
                INSERT INTO patients
                (patient_id, full_name, dob, gender, city,
                 department, doctor_id, doctor_name, admit_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                row.get('patient_id', ''),
                row.get('full_name', ''),
                row.get('dob', ''),
                row.get('gender', ''),
                row.get('city', ''),
                row.get('department', ''),
                row.get('doctor_id', ''),
                row.get('doctor_name', ''),
                row.get('admit_date', '')
            )
            count += 1
        except Exception as e:
            print(f"   ⚠️  Skipped patient {row.get('patient_id')} — {e}")
    print(f"   ✅ {count} patients loaded!")

def load_claims(cursor, df_claims):
    print("\n📥 Loading Claims...")
    count = 0
    for _, row in df_claims.iterrows():
        try:
            cursor.execute("""
                INSERT INTO claims
                (claim_id, patient_id, patient_name, billed_amount,
                 claim_date, diagnosis_code, procedure_code,
                 doctor_id, doctor_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                row.get('claim_id', ''),
                row.get('patient_id', ''),
                row.get('patient_name', ''),
                float(row.get('billed_amount', 0)),
                row.get('claim_date', ''),
                row.get('diagnosis_code', ''),
                row.get('procedure_code', ''),
                row.get('doctor_id', ''),
                row.get('doctor_name', '')
            )
            count += 1
        except Exception as e:
            print(f"   ⚠️  Skipped claim {row.get('claim_id')} — {e}")
    print(f"   ✅ {count} claims loaded!")

def load_payments(cursor, df_payments):
    print("\n📥 Loading Payments...")
    count = 0
    for _, row in df_payments.iterrows():
        try:
            cursor.execute("""
                INSERT INTO payments
                (claim_id, patient_id, patient_name, billed_amount,
                 paid_amount, denied_amount, adjustment_amount,
                 status, payer_name, service_date, payment_date,
                 adjustment_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                row.get('claim_id', ''),
                row.get('patient_id', ''),
                row.get('patient_name', ''),
                float(row.get('billed_amount', 0)),
                float(row.get('paid_amount', 0)),
                float(row.get('denied_amount', 0)),
                float(row.get('adjustment_amount', 0)),
                row.get('status', ''),
                row.get('payer_name', ''),
                row.get('service_date', ''),
                row.get('payment_date', ''),
                row.get('adjustment_reason', '')
            )
            count += 1
        except Exception as e:
            print(f"   ⚠️  Skipped payment {row.get('claim_id')} — {e}")
    print(f"   ✅ {count} payments loaded!")

def load_all():
    print("🚀 STARTING ETL LOAD PROCESS...")
    print("=" * 50)

    # Get file paths
    base     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hl7_path = os.path.join(base, 'raw_data', 'adt_patients.hl7')
    e37_path = os.path.join(base, 'raw_data', 'claims_837.edi')
    e35_path = os.path.join(base, 'raw_data', 'payments_835.edi')

    # Transform data
    df_patients, df_claims, df_payments, df_merged = transform(
        hl7_path, e37_path, e35_path
    )

    # Connect to SQL Server
    print("\n🔌 Connecting to SQL Server...")
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        print("   ✅ Connected to HealthcareDB!")
    except Exception as e:
        print(f"   ❌ Connection failed — {e}")
        return

    # Load data
    load_patients(cursor, df_patients)
    load_claims(cursor, df_claims)
    load_payments(cursor, df_payments)

    # Commit all changes
    conn.commit()
    print("\n✅ ALL DATA COMMITTED TO SQL SERVER!")
    print("=" * 50)

    # Verify counts
    print("\n📊 VERIFICATION — Record Counts in Database:")
    cursor.execute("SELECT COUNT(*) FROM patients")
    print(f"   Patients  → {cursor.fetchone()[0]} records")
    cursor.execute("SELECT COUNT(*) FROM claims")
    print(f"   Claims    → {cursor.fetchone()[0]} records")
    cursor.execute("SELECT COUNT(*) FROM payments")
    print(f"   Payments  → {cursor.fetchone()[0]} records")

    cursor.close()
    conn.close()
    print("\n🔌 Connection closed!")
    print("\n🎉 ETL PIPELINE COMPLETE!")

if __name__ == "__main__":
    load_all()