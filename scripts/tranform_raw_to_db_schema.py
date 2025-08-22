import json
from datetime import datetime

FIXED_TIMESTAMP = "23 April 2025 at 02:54:35 UTC+5:30"
USER_ID = "DxOuOVfbLoSTX2EttB49UFDcC113"


def transform_provider(pid, provider, user_id=USER_ID):
    """
    Transform a single provider object from scraped structure to DB schema.
    Keeps all original fields as well (under 'raw_data').
    """
    transformed = {}

    # Map simple fields
    transformed["universityId"] = pid
    transformed["about_us"] = provider.get("aboutUs")
    transformed["background_image"] = provider.get("backgroundUrl")
    transformed["logo_url"] = provider.get("logoUrl")
    transformed["official_website"] = provider.get("websiteUrl")
    transformed["university_name"] = provider.get("name")
    transformed["location"] = provider.get("address", {}).get("line4")
    transformed["userId"] = user_id

    # Add placeholders (since not in scraped data)
    transformed["contact_email"] = None
    transformed["contact_phone"] = None

    # Timestamps (fixed as per request)
    transformed["createdAt"] = FIXED_TIMESTAMP
    transformed["updatedAt"] = FIXED_TIMESTAMP

    # Course locations
    course_locations = []
    for loc in provider.get("courseLocations", []):
        course_locations.append({
            "location_name": loc.get("title"),
            "location_address": loc.get("address")
        })
    transformed["course_locations"] = course_locations

    # Courses grouped by Undergraduate / Postgraduate -> Full-time / Part-time
    courses_grouped = {
        "Undergraduate": {"Full-time": [], "Part-time": []},
        "Postgraduate": {"Full-time": [], "Part-time": []}
    }

    for course in provider.get("courses", []):
        destination = course.get("routingData", {}).get("destination", {}).get("caption", "Unknown")
        course_name = course.get("courseTitle")
        application_code = course.get("applicationCode")

        for opt in course.get("options", []):
            study_mode = opt.get("studyMode", {}).get("caption", "Unknown")
            duration_obj = opt.get("duration")
            duration = None
            if duration_obj:
                qty = duration_obj.get("quantity")
                unit = duration_obj.get("durationType", {}).get("caption")
                if qty and unit:
                    duration = f"{int(qty)} {unit}"

            entry = {
                "course_name": course_name,
                "duration": duration,
                "location": opt.get("location").get("name") if opt.get("location") else None,
                "qualification": opt.get("outcomeQualification", {}).get("caption"),
                "study_mode": study_mode,
                "study_period": study_mode,
                "type": destination,
                "ucas_points": None,
                "university": provider.get("name")
            }

            # Place into grouped dict
            if destination in courses_grouped and study_mode in courses_grouped[destination]:
                courses_grouped[destination][study_mode].append(entry)
            else:
                # Fallback bucket
                courses_grouped.setdefault(destination, {}).setdefault(study_mode, []).append(entry)

    transformed["courses"] = courses_grouped

    # Keep the raw scraped data (so nothing is lost)
    transformed["raw_data"] = provider

    return transformed


def main():
    # Load scraped data
    with open("../data/providers_with_courses.json", "r", encoding="utf-8") as f:
        scraped = json.load(f)

    transformed_all = []

    for pid, provider in scraped.items():
        transformed_all.append(transform_provider(pid, provider))

    # Save transformed
    with open("../data/providers_transformed.json", "w", encoding="utf-8") as f:
        json.dump(transformed_all, f, indent=4, ensure_ascii=False)

    print(f"✅ Transformed {len(transformed_all)} providers → providers_transformed.json")


if __name__ == "__main__":
    main()
