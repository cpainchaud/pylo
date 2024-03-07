import sys
from typing import List

from illumio_pylo import log
import logging
#log.setLevel(logging.DEBUG)

sys.path.append('..')

import argparse
import illumio_pylo as pylo


originHostname='10.107.3.2'
targetHostname='10.256.3.2'


print("Loading Origin PCE configuration from " + originHostname + " or cached file... ", end="", flush=True)
origin = pylo.get_organization_using_credential_file(originHostname)
print("OK!")

print("Loading Target PCE configuration from " + targetHostname + " or cached file ... ", end="", flush=True)
target = pylo.get_organization_using_credential_file(targetHostname)
print("OK!")


print("Statistics for Origin PCE %s:\n%s" % (originHostname, origin.stats_to_str()))
print("Statistics for Target PCE %s:\n%s" % (targetHostname, target.stats_to_str()))


labelsToImport: List[pylo.Label] = []
labelsInConflict: List[pylo.Label] = []

for label in origin.LabelStore.get_both_labels_and_groups():
    labelName = label.name

    targetLabelFind = target.LabelStore.find_label_by_name_and_type(labelName, label.type)
    if targetLabelFind is not None:
        labelsInConflict.append(label)
    else:
        labelsToImport.append(label)


importCount = len(labelsToImport)
conflictCount = len(labelsInConflict)

print("*** Found " + str(importCount) + " Labels to import and " + str(conflictCount) + " which already exist")

#print("** List of Labels which already exist in the Target PCE: ")
#for label in labelsToImport:
#    print("- " + label.name + " (" + label.type_string() + ")")


print("\n** Now looking for Labels with uppercase/lowercase mismatch")
labelsWithCaseMismatch = []
for label in labelsToImport.copy():
    ''':type label pylo.Label'''
    msg = label.name
    labelLowerCap = label.name.lower()

    lowerCapConflicts = target.LabelStore.find_label_by_name(labelLowerCap, label_tyê=label.type(), case_sensitive=False)

    if len(lowerCapConflicts) > 0:
        for lowerCapLabel in lowerCapConflicts:
            lowerCapConflicts.append(lowerCapLabel.name)

        labelsToImport.remove(label)
        labelsWithCaseMismatch.append(label)
        print(" - " + label.name + " has " + str(len(lowerCapConflicts)) + " case mismatches: " +
              pylo.Helpers.string_list_to_text(lowerCapConflicts))

print("*** Found %i Labels to import and %i which have case mismatches.\n" % (len(labelsToImport), len(labelsWithCaseMismatch)) )

if len(labelsWithCaseMismatch) > 0:
    sys.exit("You must fix the lowercase Label before you can continue")


print("** Now importing {} labels into PCE '{}'".format(len(labelsToImport), targetHostname))

for label in labelsToImport:
    print(" - Processing label '%s' with type %s" % (label.name, label.type_string()) )
    target.connector.objects_label_create(label.name, label.type_string())
    print("    + CREATED!")

print("\n*****  IMPORT DONE!  *****\n")


