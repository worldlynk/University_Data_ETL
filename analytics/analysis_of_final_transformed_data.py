import json

def count_courses(courses):
    result = {
        "Undergraduate": {"Full-time": 0, "Part-time": 0},
        "Postgraduate": {"Full-time": 0, "Part-time": 0}
    }
    for level in result:
        for mode in result[level]:
            result[level][mode] = len(courses.get(level, {}).get(mode, []))
    return result

def count_null_fields(provider, fields):
    null_counts = {}
    for field in fields:
        value = provider.get(field)
        if value is None:
            null_counts[field] = 1
        elif isinstance(value, list) and not value:
            null_counts[field] = 1
        elif isinstance(value, dict) and not value:
            null_counts[field] = 1
        else:
            null_counts[field] = 0
    return null_counts

def main():
    with open("../data/providers_transformed.json", "r", encoding="utf-8") as f:
        providers = json.load(f)

    fields_to_check = [
        "about_us", "background_image", "contact_email", "contact_phone",
        "course_locations", "courses", "createdAt", "location", "logo_url",
        "official_website", "university_name", "updatedAt", "userId"
    ]

    with open("../reports/analysis_report.md", "w", encoding="utf-8") as f:
        f.write("# University Data Analysis Report\n\n")
        for provider in providers:
            uni_name = provider.get("university_name", "Unknown University")
            f.write(f"## {uni_name}\n")
            # Course counts
            course_counts = count_courses(provider.get("courses", {}))
            f.write("### Course Counts\n")
            for level in course_counts:
                for mode in course_counts[level]:
                    f.write(f"- {level} {mode}: {course_counts[level][mode]}\n")
            # Null field counts
            nulls = count_null_fields(provider, fields_to_check)
            f.write("\n### Null Field Counts\n")
            for field, count in nulls.items():
                f.write(f"- {field}: {count}\n")
            f.write("\n---\n")

    print("✅ Per-university analysis complete. See analysis_report.md")

if __name__ == "__main__":
    main()
     