from dataclasses import dataclass
from types import NoneType
from typing import List, TypedDict, Dict, Union, Iterable

import click

import illumio_pylo as pylo


class LabelToBeCreatedRecord(TypedDict):
    name: str
    type: str


def generate_list_of_labels_to_create(csv_data: Iterable[Dict[str, Union[bool, str, int, NoneType]]],
                                      org: pylo.Organization, header_label_prefix: str) -> List[LabelToBeCreatedRecord]:
    """
    This function will check for label case collisions and missing ones to be created

    :param csv_data: list of csv data
    :param org:
    :param header_label_prefix: prefix for the label headers
    :return:
    """
    print(" * Checking for Labels case collisions and missing ones to be created:")

    @dataclass
    class LabelExistsRecord:
        csv: bool
        real_case: str
        type: str

    name_cache: Dict[str,LabelExistsRecord] = {}

    for label in org.LabelStore.get_labels():
        if label.name is not None:
            lower_name = label.name.lower()
            if lower_name not in name_cache:
                name_cache[lower_name] = LabelExistsRecord(csv=False, real_case=label.name, type=label.type)
            else:
                if name_cache[lower_name].type != label.type:
                    raise pylo.PyloEx("Found duplicate label with name '{}' but different type in the PCE. This must be fixed before this tool is run again.".format(label.name))
                else:
                    print("  - Warning duplicate found 2 Labels in the PCE with same name but different case: '{}' vs '{}'. One will be picked".format(
                        label.name, label.type))

    for csv_object in csv_data:
        if '**not_created_reason**' in csv_object:
            continue

        # each label type/dimension we must check which ones are requested and if they exist
        for label_type in org.LabelStore.label_types:
            requested_label_name = csv_object[f"{header_label_prefix}{label_type}"]
            if requested_label_name is None or requested_label_name == "":
                pass
            else:
                requested_label_name = str(requested_label_name)
                requested_label_name_lower = requested_label_name.lower()
                if requested_label_name_lower not in name_cache:
                    name_cache[requested_label_name_lower] = LabelExistsRecord(csv=True,
                                                                               real_case=requested_label_name,
                                                                               type=label_type)
                # type collision, this is not recoverable
                elif name_cache[requested_label_name_lower].type != label_type:
                    if 'csv' in name_cache[requested_label_name_lower]:
                        raise pylo.PyloEx(
                            "Found duplicate label with name '{}' but different type within the CSV".format(
                                requested_label_name))
                    else:
                        raise pylo.PyloEx(
                            "Found duplicate label with name '{}' but different type between CSV and PCE".format(
                                requested_label_name))
                # case collision
                elif name_cache[requested_label_name_lower].real_case != requested_label_name:
                    if 'csv' in name_cache[requested_label_name_lower]:
                        raise pylo.PyloEx(
                            "Found duplicate label with name '{}' but different case within the CSV".format(
                                requested_label_name))
                    else:
                        raise pylo.PyloEx(
                            "Found duplicate label with name '{}' but different case between CSV and PCE".format(
                                requested_label_name))

    labels_to_be_created: List[LabelToBeCreatedRecord] = []
    for label_entry in name_cache.values():
        if label_entry.csv is True:
            labels_to_be_created.append(LabelToBeCreatedRecord(name=label_entry.real_case, type=label_entry.type))
    print("  * DONE")
    return labels_to_be_created


def create_labels(labels_to_be_created: List[LabelToBeCreatedRecord], org: pylo.Organization):
    for label_to_create in labels_to_be_created:
        print("   - {} type {}".format(label_to_create['name'], label_to_create['type']))
    click.confirm("Do you want to create these labels now?", abort=True)
    for label_to_create in labels_to_be_created:
        print(
            "   - Pushing '{}' with type '{}' to the PCE... ".format(label_to_create['name'], label_to_create['type']),
            end='', flush=True)
        org.LabelStore.api_create_label(label_to_create['name'], label_to_create['type'])
        print("OK")