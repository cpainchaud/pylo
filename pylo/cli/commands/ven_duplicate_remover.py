import pylo
import argparse
from typing import Dict, List, Any
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'ven-duplicate-remover'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='')
    parser.add_argument('--confirm', '-c', action='store_true',
                        help='actually operate deletions')


def __main(args, org: pylo.Organization, **kwargs):
    verbose = args['verbose']
    argument_confirm = args['confirm']

    output_file_prefix = make_filename_with_timestamp('ven-duplicate-removal_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    csv_report_headers = ['name', 'hostname', 'role', 'app', 'env', 'loc', 'online', 'href', 'action']
    csv_report = pylo.ArrayToExport(csv_report_headers)

    # <editor-fold desc="PCE Configuration Download and Parsing">
    all_workloads = org.WorkloadStore.itemsByHRef.copy()
    used_filters = {}

    def add_workload_to_report(wkl: pylo.Workload, action: str):
        labels = workload.get_labels_str_list()
        new_row = {
                'hostname': wkl.hostname,
                'role': labels[0],
                'app': labels[1],
                'env': labels[2],
                'loc': labels[3],
                'online': wkl.online,
                'href': wkl.href,
                'action': action
        }

        csv_report.add_line_from_object(new_row)

    class DuplicatedRecord:
        def __init__(self):
            self.offline = []
            self.online = []
            self.unmanaged= []

        def add_workload(self, wkl: pylo.Workload):
            if wkl.unmanaged:
                self.unmanaged.append(wkl)
                return
            if wkl.online:
                self.online.append(wkl)
                return
            self.offline.append(wkl)

        def count_workloads(self):
            return len(self.unmanaged) + len(self.online) + len(self.offline)

        def count_online(self):
            return len(self.online)

        def count_offline(self):
            return len(self.offline)

        def count_unmanaged(self):
            return len(self.unmanaged)

        def has_duplicates(self):
            if len(self.offline) + len(self.online) + len(self.unmanaged) > 1:
                return True
            return False

    class DuplicateRecordManager:
        def __init__(self):
            self._records: Dict[str, DuplicatedRecord] = {}

        def count_record(self):
            return len(self._records)

        def count_workloads(self):
            total = 0
            for record in self._records.values():
                total += record.count_workloads()

        def records(self):
            return list(self._records.values())

        def count_duplicates(self):
            count = 0
            for record in self._records.values():
                if record.has_duplicates():
                    count += 1
            return count

        def add_workload(self, wkl: pylo.Workload):
            lower_hostname = wkl.get_name_stripped_fqdn().lower()

            if lower_hostname not in self._records:
                self._records[lower_hostname] = DuplicatedRecord()
            record = self._records[lower_hostname]
            record.add_workload(wkl)

    duplicated_hostnames = DuplicateRecordManager()

    print(" * Looking for VEN with duplicated hostname(s)")

    for workload in all_workloads.values():
        if workload.deleted:
            continue
        duplicated_hostnames.add_workload(workload)

    print(" * Found {} duplicated hostnames".format(duplicated_hostnames.count_duplicates()))

    deleteTracker = org.connector.new_tracker_workload_multi_delete()

    for dup_hostname, dup_record in duplicated_hostnames._records.items():
        if not dup_record.has_duplicates():
            continue

        print("  - hostname '{}' has duplicates. ({} online, {} offline, {} unmanaged)".format(dup_hostname,
                                                                                         len(dup_record.online),
                                                                                         len(dup_record.offline),
                                                                                         len(dup_record.unmanaged)))

        if dup_record.count_online() == 0:
            print("     - IGNORED: there is no VEN online")
            continue

        if dup_record.count_online() > 1:
            print("     - IGNORED: there are more than 1 VEN online")
            continue

        for wkl in dup_record.offline:
            deleteTracker.add_workload(wkl)
            print("    - added offline wkl {}/{} to the delete list".format(wkl.get_name_stripped_fqdn(), wkl.href))

        for wkl in dup_record.unmanaged:
            deleteTracker.add_workload(wkl)
            # deleteTracker.add_href('nope')
            print("    - added unmanaged wkl {}/{} to the delete list".format(wkl.get_name_stripped_fqdn(), wkl.href))

    print()

    if deleteTracker.count_entries() < 1:
        print(" * No duplicate found!")

    elif argument_confirm:
        print(" * Found {} workloads to be deleted".format(deleteTracker.count_entries()))
        print(" * Executing deletion requests ... ".format(output_file_csv), end='', flush=True)
        deleteTracker.execute(unpair_agents=True)
        print("DONE")

        for wkl in deleteTracker._wkls.values():
            error_msg = deleteTracker.get_error_by_href(wkl.href)
            if error_msg is None:
                add_workload_to_report(wkl, "deleted")
            else:
                print("    - an error occured when deleting workload {}/{} : {}".format(wkl.get_name_stripped_fqdn(), wkl.href, error_msg))
                add_workload_to_report(wkl, "API error: " + error_msg)

        print()
        print(" * {} workloads deleted / {} with errors".format(deleteTracker.count_entries()-deleteTracker.count_errors(), deleteTracker.count_errors()))
        print()
    else:
        print(" * Found {} workloads to be deleted BUT NO 'CONFIRM' OPTION WAS USED".format(deleteTracker.count_entries()))
        for wkl in deleteTracker._wkls.values():
            add_workload_to_report(wkl, "DELETE (no confirm option used)")

    if csv_report.lines_count() < 1:
        print()
        print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
        csv_report.write_to_csv(output_file_csv)
        print("DONE")
        print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
        csv_report.write_to_excel(output_file_excel)
        print("DONE")

    if csv_report.lines_count() < 1:
        print("\n** WARNING: no entry matched your filters so reports were not generated !\n")


command_object = Command(command_name, __main, fill_parser, objects_load_filter)
