from datetime import datetime, timedelta
import json
from google.cloud.firestore_v1 import FieldFilter
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.cloud import firestore
import psycopg2
from psycopg2.extras import execute_values
import warnings
import os

test = "secrets/gcp_keys/nosqldbtops.json"

# To ignore all warnings:
warnings.filterwarnings("ignore")
"""
# ECS injects the secret path into this env var
if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    creds_json = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    
    #write json to temporary file
    creds_path = "/tmp/google_creds.json"
    with open(creds_path, 'w') as creds_file:
        creds_file.write(creds_json)
        
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
"""
# Initialize Firestore client using the temp file
db = firestore.Client.from_service_account_json(test)

def extract_from_firestore(**context):
    # Get last successful run timestamp for incremental loading
    last_run = context.get('prev_data_interval_end') or datetime(2024, 1, 1)
    #last_run_str = last_run.isoformat()

    db = firestore.Client()

    query = db.collection("peoples").where(
        filter=FieldFilter('modified_at', '>', last_run)
    ).stream()  # Adjust batch size as needed
    
    documents = []
    doc_count = 0
    
    try:
        # Execute query and collect documents
        for doc in query:
            doc_data = doc.to_dict()
            doc_data['firestore_id'] = doc.id  # Include document ID
            documents.append(doc_data)
            doc_count += 1
        
        print(f"Extracted {doc_count} documents from Firestore")
        return documents
        
    except Exception as e:
        print(f"Error extracting from Firestore: {str(e)}")
        raise
    

def transform_firestore_data(from_firestore_json):
    """Transform Firestore data to be compatible with RDS"""
    
    transformed_records = []
    
    for doc in from_firestore_json:
        try:
            transformed_record = {
                'firestore_document_id': doc.get('firestore_document_id'),
                'firstName': doc.get('firstName'),
                'lastName': doc.get('lastName'),
                'email': doc.get('email'),
                'address': doc.get('address'),
                'phone': doc.get('phone'),
                'created_at': doc.get('created_at'),
                'modified_at': doc.get('modified_at'),
                'sync_timestamp': datetime.utcnow().isoformat()
            }
            
            # Data validation
            if not transformed_record['firestore_document_id']:
                print(f"Skipping document - missing firestore_document_id")
                continue
                
            transformed_records.append(transformed_record)
            
        except Exception as e:
            print(f"Error transforming document {doc.get('firestore_id', 'unknown')}: {str(e)}")
            continue
    
    print(f"Transformed {len(transformed_records)} records for RDS")
    return transformed_records


def load_to_rds(transformed_firestore_data):
    if not transformed_firestore_data:
        print("No data to load to RDS")
        return 0
    
    # Connection config
    conn = psycopg2.connect(
        host="database-1.cfao22kk0umo.us-east-2.rds.amazonaws.com",
        port=5432,
        user="postgres",
        password="!Lionsheart7!"
    )
    cur = conn.cursor()
    
    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS firestore_users (
            firestore_document_id VARCHAR(255) PRIMARY KEY,
            firstName VARCHAR(255),
            lastName VARCHAR(255),
            email VARCHAR(500),
            address VARCHAR(320),
            phone VARCHAR(320),
            created_at TIMESTAMP,
            modified_at TIMESTAMP,
            sync_timestamp TIMESTAMP 
            );
        """)
    
    # Prepare data for batch insert
    records_to_insert = [
        (
            record['firestore_document_id'],
            record['firstName'],
            record['lastName'],
            record['email'],
            record['address'],
            record['phone'],
            record['created_at'],
            record['modified_at'],
            record['sync_timestamp']
        )
        for record in transformed_firestore_data
    ]
    
    # Batch upsert
    insert_query = """
        INSERT INTO firestore_users (
            firestore_document_id, firstName, lastName, email,
            address, phone, created_at, modified_at, sync_timestamp
        ) VALUES %s
        ON CONFLICT (firestore_document_id) 
        DO UPDATE SET
            firstName = EXCLUDED.firstName,
            lastName = EXCLUDED.lastName,
            email = EXCLUDED.email,
            address = EXCLUDED.address,
            phone = EXCLUDED.phone,
            created_at = EXCLUDED.created_at,
            modified_at = EXCLUDED.modified_at,
            sync_timestamp = EXCLUDED.sync_timestamp
    """
    
    execute_values(cur, insert_query, records_to_insert)
    conn.commit()
    
    print(f"Upserted {len(records_to_insert)} records into RDS")
    cur.close()
    conn.close()
    
    return len(records_to_insert)


print(load_to_rds(transform_firestore_data(extract_from_firestore())))