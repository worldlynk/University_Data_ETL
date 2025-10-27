import firebase_admin
from firebase_admin import credentials
from pathlib import Path

# Use absolute path to credentials file in the config directory
config_dir = Path(__file__).parent
cred_path = config_dir / "worldlynk-97994-firebase-adminsdk-89lpc-52e4181dd4.json"

cred = credentials.Certificate(str(cred_path))
firebase_admin.initialize_app(cred)
