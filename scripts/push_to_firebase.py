try:
    from firebase_admin import credentials, firestore, initialize_app
    from google.cloud.firestore import WriteBatch
    from google.api_core.exceptions import DeadlineExceeded, RetryError
except ModuleNotFoundError as e:
    print("Missing required package. Please install dependencies with:")
    print("    pip install firebase-admin google-cloud-firestore")
    exit(1)

import json
import logging
import time
import random

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)

from config.firebaseConfig import *
db = firestore.client()

def batch_push_with_retry(collection_name, data_list, batch_size=50, max_retries=3, base_sleep_time=2):
    """
    Push data to Firestore in batches with retry logic and exponential backoff.
    
    :param collection_name: Firestore collection name
    :param data_list: List of dicts to push
    :param batch_size: Number of docs per batch (reduced from 100 to 50)
    :param max_retries: Maximum number of retry attempts per batch
    :param base_sleep_time: Base seconds to sleep between batches
    """
    total = len(data_list)
    logging.info(f"Starting batch push of {total} documents to '{collection_name}'")
    
    failed_batches = []
    
    for i in range(0, total, batch_size):
        batch_num = i // batch_size + 1
        batch_data = data_list[i:i+batch_size]
        
        success = False
        for attempt in range(max_retries + 1):
            try:
                batch = db.batch()
                for doc in batch_data:
                    # Generate document ID if not present
                    doc_id = doc.get('id') or f"doc_{i}_{hash(str(doc))}"
                    doc_ref = db.collection(collection_name).document(doc_id)
                    batch.set(doc_ref, doc)
                
                # Commit with timeout
                batch.commit()
                logging.info(f"✓ Pushed batch {batch_num}: {len(batch_data)} documents")
                success = True
                break
                
            except (DeadlineExceeded, RetryError) as e:
                if attempt < max_retries:
                    wait_time = base_sleep_time * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"⚠ Batch {batch_num} failed (attempt {attempt + 1}/{max_retries + 1}). "
                                  f"Retrying in {wait_time:.1f}s... Error: {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"✗ Batch {batch_num} failed after {max_retries + 1} attempts: {e}")
                    failed_batches.append((i, batch_data))
                    
            except Exception as e:
                logging.error(f"✗ Unexpected error in batch {batch_num}: {e}")
                failed_batches.append((i, batch_data))
                break
        
        if success:
            # Add jitter to avoid thundering herd
            sleep_time = base_sleep_time + random.uniform(0, 1)
            time.sleep(sleep_time)
    
    logging.info("Batch push completed.")
    
    if failed_batches:
        logging.warning(f"❌ {len(failed_batches)} batches failed. Consider retrying them.")
        return failed_batches
    else:
        logging.info("✅ All batches completed successfully!")
        return []

def individual_push_fallback(collection_name, failed_data, sleep_time=0.5):
    """
    Fallback method: push documents individually for failed batches.
    
    :param collection_name: Firestore collection name
    :param failed_data: List of tuples (index, batch_data) that failed
    :param sleep_time: Seconds to sleep between individual pushes
    """
    logging.info("Starting individual document push for failed batches...")
    
    for batch_index, batch_data in failed_data:
        for doc_index, doc in enumerate(batch_data):
            try:
                doc_id = doc.get('id') or f"doc_{batch_index}_{doc_index}_{hash(str(doc))}"
                doc_ref = db.collection(collection_name).document(doc_id)
                doc_ref.set(doc)
                logging.info(f"✓ Individual push successful: {doc_id}")
                time.sleep(sleep_time)
                
            except Exception as e:
                logging.error(f"✗ Individual push failed for doc {doc_id}: {e}")

if __name__ == "__main__":
    # Load data from providers_transformed.json
    try:
        with open("../data/providers_transformed.json", "r", encoding="utf-8") as f:
            your_data_list = json.load(f)
    except FileNotFoundError:
        logging.error("providers_transformed.json not found. Please ensure the file exists.")
        exit(1)
    except json.JSONDecodeError:
        logging.error("Invalid JSON format in providers_transformed.json")
        exit(1)

    if not your_data_list:
        logging.warning("No data to push. Exiting.")
    else:
        # Try batch push with smaller batch size and retry logic
        failed_batches = batch_push_with_retry(
            collection_name='universities', 
            data_list=your_data_list, 
            batch_size=12,  # Further reduced batch size
            max_retries=3,
            base_sleep_time=3
        )
        
        # If some batches failed, try individual push
        if failed_batches:
            logging.info("🔄 Attempting individual document push for failed batches...")
            individual_push_fallback('universities', failed_batches)