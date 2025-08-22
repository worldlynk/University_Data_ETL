import firebase_admin
from firebase_admin import credentials

cred = credentials.Certificate("worldlynk-97994-firebase-adminsdk-89lpc-52e4181dd4.json")
firebase_admin.initialize_app(cred)
