from __future__ import annotations


def validate_categories(category_groupings: dict, category_labels: list) -> dict:
    grouping_labels = category_groupings.values()
    combined_list_label = [item for sublist in grouping_labels for item in sublist]

    required_labels = []
    extra_labels = []

    for label in category_labels:
        if label not in combined_list_label:
            required_labels.append(label)

    for label in combined_list_label:
        if label not in category_labels:
            extra_labels.append(label)

    return {"required_labels": required_labels, "extra_labels": extra_labels}
