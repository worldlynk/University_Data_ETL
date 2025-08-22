import requests
import json

# API endpoint
url = "https://services.ucas.com/search/api/v2/providers/search?fields=providers(id,name,aliasName,aliases,providerSort,logoUrl,websiteUrl,institutionCode,address(line4,country(mappedCaption)),courses(id,academicYearId,applicationCode,courseTitle,routingData(destination(caption),scheme(caption)),options(id,outcomeQualification(caption),duration,durationRange(min,max),studyMode,startDate,location(name)))),information(postcodeLooku,paging)"

# Payload
payload = {
    "searchTerm": "",
    "filters": {
        "academicYearId": "2025",
        "destinations": ["Undergraduate", "Postgraduate"],
        "providers": [],
        "schemes": [],
        "ucasTeacherTrainingProvider": False,
        "degreeApprenticeship": False,
        "studyTypes": [],
        "subjects": [],
        "qualifications": [],
        "attendanceTypes": [],
        "acceleratedDegrees": False,
        "entryPoint": None,
        "regions": [],
        "vacancy": "",
        "startDates": [],
        "higherTechnicalQualifications": False
    },
    "options": {
        "sort": [
            {
                "direction": "a-z",
                "name": "provider"
            }
        ],
        "paging": {
            "pageNumber": 1,
            "pageSize": 600
        },
        "viewType": "provider"
    },
    "inClearing": True
}

headers = {"Content-Type": "application/json"}

# Send POST request
response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    data = response.json()

    # Extract provider IDs
    providers = data.get("providers", [])
    ids = [p["id"] for p in providers if "id" in p]

    # Wrap in { "ids": [...] }
    output = {"ids": ids}

    # Save to file
    with open("../data/provider_ids.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4)

    print(f"✅ Extracted {len(ids)} provider IDs and saved to provider_ids.json")
else:
    print(f"❌ Failed to fetch data. Status: {response.status_code}, Response: {response.text}")
