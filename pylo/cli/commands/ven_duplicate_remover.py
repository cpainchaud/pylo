import pylo
import argparse
from typing import Dict, List, Any
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'ven-duplicate-remover'
objects_load_filter = ['labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='')
    parser.add_argument('--confirm', '-c', action='store_true',
                        help='actually operate deletions')
    parser.add_argument('--filter-label', '-fl', action='append',
                        help='Only look at workloads matching specified labels')
    parser.add_argument('--ignore-unmanaged-workloads', '-iuw', action='store_true',
                        help='Do not touch unmanaged workloads nor include them to detect duplicates')

class DuplicateRecordManager:
    class DuplicatedRecord:
        def __init__(self):
            self.offline = []
            self.online = []
            self.unmanaged= []

        def add_workload(self, workload: 'pylo.Workload'):
            if workload.unmanaged:
                self.unmanaged.append(workload)
                return
            if workload.online:
                self.online.append(workload)
                return
            self.offline.append(workload)

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

    def __init__(self):
        self._records: Dict[str, DuplicateRecordManager.DuplicatedRecord] = {}

    def count_record(self):
        return len(self._records)

    def count_workloads(self):
        total = 0
        for record in self._records.values():
            total += record.count_workloads()

    def records(self) -> List['DuplicateRecordManager.DuplicatedRecord']:
        return list(self._records.values())

    def count_duplicates(self):
        count = 0
        for record in self._records.values():
            if record.has_duplicates():
                count += 1
        return count

    def add_workload(self, workload: pylo.Workload):
        lower_hostname = workload.get_name_stripped_fqdn().lower()

        if lower_hostname not in self._records:
            self._records[lower_hostname] = self.DuplicatedRecord()
        record = self._records[lower_hostname]
        record.add_workload(workload)


def __main(args, org: pylo.Organization, pce_cache_was_used: bool, **kwargs):
    verbose = args['verbose']
    argument_confirm = args['confirm']
    arg_ignore_unmanaged_workloads = args['ignore_unmanaged_workloads'] is True

    output_file_prefix = make_filename_with_timestamp('ven-duplicate-removal_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    csv_report_headers = ['name', 'hostname', 'role', 'app', 'env', 'loc', 'online', 'href', 'action']
    csv_report = pylo.ArrayToExport(csv_report_headers)

    # <editor-fold desc="Download workloads from PCE">
    filter_labels: Dict[str, List[pylo.Label]] = {}
    if args['filter_label'] is not None:
        for label_name in args['filter_label']:
            label = org.LabelStore.find_label_by_name(label_name)
            if label is None:
                raise pylo.PyloEx("Cannot find label '{}' in the PCE".format(label_name))
            if label.type_string() in filter_labels:
                filter_labels[label.type_string()].append(label)
            else:
                filter_labels[label.type_string()] = [label]
    if pce_cache_was_used:
        print("* Skipping Workloads download as it was loaded from cache file")
    else:
        print("* Downloading Workloads data from the PCE... ", flush=True)
        if args['filter_label'] is None:
            workloads_json = org.connector.objects_workload_get(async_mode=True, max_results=1000000)
        else:
            filter_labels_list_of_list: List[List[pylo.Label]] = []
            # convert filter_labels dict to an array of arrays
            for label_type, label_list in filter_labels.items():
                filter_labels_list_of_list.append(label_list)

            # convert filter_labels_list_of_list to a matrix of all possibilities
            # example: [[a,b],[c,d]] becomes [[a,c],[a,d],[b,c],[b,d]]
            filter_labels_matrix = [[]]
            for label_list in filter_labels_list_of_list:
                new_matrix = []
                for label in label_list:
                    for row in filter_labels_matrix:
                        new_row = row.copy()
                        new_row.append(label.href)
                        new_matrix.append(new_row)
                filter_labels_matrix = new_matrix
            print(filter_labels_matrix)

            workloads_json = org.connector.objects_workload_get(async_mode=False, max_results=1000000, filter_by_label=filter_labels_matrix)

        org.WorkloadStore.load_workloads_from_json(workloads_json)

    print("OK!")
    # </editor-fold>

    print(org.stats_to_str())

    all_workloads = org.WorkloadStore.itemsByHRef.copy()

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

    duplicated_hostnames = DuplicateRecordManager()

    print(" * Looking for VEN with duplicated hostname(s)")

    for workload in all_workloads.values():
        if workload.deleted:
            continue
        if workload.unmanaged and arg_ignore_unmanaged_workloads:
            continue

        duplicated_hostnames.add_workload(workload)

    print(" * Found {} duplicated hostnames".format(duplicated_hostnames.count_duplicates()))

    delete_tracker = org.connector.new_tracker_workload_multi_delete()

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
            print("     - WARNING: there are more than 1 VEN online")

        for wkl in dup_record.offline:
            delete_tracker.add_workload(wkl)
            print("    - added offline wkl {}/{} to the delete list".format(wkl.get_name_stripped_fqdn(), wkl.href))

        for wkl in dup_record.unmanaged:
            delete_tracker.add_workload(wkl)
            # deleteTracker.add_href('nope')
            print("    - added unmanaged wkl {}/{} to the delete list".format(wkl.get_name_stripped_fqdn(), wkl.href))

    print()

    if delete_tracker.count_entries() < 1:
        print(" * No duplicate found!")

    elif argument_confirm:
        print(" * Found {} workloads to be deleted".format(delete_tracker.count_entries()))
        print(" * Executing deletion requests ... ".format(output_file_csv), end='', flush=True)
        delete_tracker.execute(unpair_agents=True)
        print("DONE")

        for wkl in delete_tracker.workloads:
            error_msg = delete_tracker.get_error_by_href(wkl.href)
            if error_msg is None:
                add_workload_to_report(wkl, "deleted")
            else:
                print("    - an error occurred when deleting workload {}/{} : {}".format(wkl.get_name_stripped_fqdn(), wkl.href, error_msg))
                add_workload_to_report(wkl, "API error: " + error_msg)

        print()
        print(" * {} workloads deleted / {} with errors".format(delete_tracker.count_entries()-delete_tracker.count_errors(), delete_tracker.count_errors()))
        print()
    else:
        print(" * Found {} workloads to be deleted BUT NO 'CONFIRM' OPTION WAS USED".format(delete_tracker.count_entries()))
        for wkl in delete_tracker.workloads:
            add_workload_to_report(wkl, "DELETE (no confirm option used)")

    if csv_report.lines_count() >= 1:
        print()
        print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
        csv_report.write_to_csv(output_file_csv)
        print("DONE")
        print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
        csv_report.write_to_excel(output_file_excel)
        print("DONE")

    else:
        print("\n** WARNING: no entry matched your filters so reports were not generated !\n")


command_object = Command(command_name, __main, fill_parser, objects_load_filter)
