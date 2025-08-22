# University Data ETL

This project extracts, transforms, and loads university data, including provider and course information, and pushes it to Firebase.

## Setup

1. **Clone the repository**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Add your Firebase config file:**
   - Place your Firebase service account JSON file in the `config/` directory.
   - The file should be named as required by your scripts (e.g., `worldlynk-97994-firebase-adminsdk-89lpc-52e4181dd4.json`).
   - **Do not commit this file to git.**

## Usage

- See scripts in the `scripts/` folder for ETL operations.
- See `analytics/` for data analysis scripts.
- See `reports/` for generated reports.

## Notes
- The Firebase config file is required for pushing data to Firebase.
- The file is ignored by git for security reasons.
