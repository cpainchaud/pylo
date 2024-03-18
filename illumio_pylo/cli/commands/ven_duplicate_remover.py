from typing import Dict, List, Literal, Optional
import datetime
import click
import argparse

import illumio_pylo as pylo
from illumio_pylo import ArraysToExcel, ExcelHeader, ExcelHeaderSet

from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'ven-duplicate-remover'
objects_load_filter = ['labels']

def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='')
    parser.add_argument('--proceed-with-deletion', action='store_true',
                        help='Actually operate deletions. Considered as a dry-run if not specified.')
    parser.add_argument('--do-not-require-deletion-confirmation', action='store_true',
                        help='Ask for confirmation for each deletion')
    parser.add_argument('--filter-label', '-fl', action='append',
                        help='Only look at workloads matching specified labels')
    parser.add_argument('--ignore-unmanaged-workloads', '-iuw', action='store_true',
                        help='Do not touch unmanaged workloads nor include them to detect duplicates')
    parser.add_argument('--report-format', '-rf', action='append', type=str, choices=['csv', 'xlsx'], default=None,
                        help='Which report formats you want to produce (repeat option to have several)')
    parser.add_argument('--do-not-delete-the-most-recent-workload', '-nrc', action='store_true',
                        help='Workload which was created the last will not be deleted')
    parser.add_argument('--do-not-delete-the-most-recently-heartbeating-workload', '-nrh', action='store_true',
                        help='Workload which was heartbeating the last will not be deleted')
    parser.add_argument('--do-not-delete-if-last-heartbeat-is-more-recent-than', type=int, default=None,
                        help='Workload which was heartbeating the last will not be deleted if the last heartbeat is more recent than the specified number of days')
    parser.add_argument('--override-pce-offline-timer-to', type=int, default=None,
                        help='Override the PCE offline timer to the specified number of days')
    parser.add_argument('--limit-number-of-deleted-workloads', '-l', type=int, default=None,
                        help='Limit the number of workloads to be deleted, for a limited test run for example.')

    parser.add_argument('--output-dir', '-o', type=str, required=False, default="output",
                        help='Directory where to write the report file(s)')



def __main(args, org: pylo.Organization, pce_cache_was_used: bool, **kwargs):

    report_wanted_format: List[Literal['csv','xlsx']] = args['report_format']
    if report_wanted_format is None:
        report_wanted_format = ['xlsx']


    arg_verbose = args['verbose']
    arg_proceed_with_deletion = args['proceed_with_deletion'] is True
    arg_do_not_require_deletion_confirmation = args['do_not_require_deletion_confirmation'] is True
    arg_ignore_unmanaged_workloads = args['ignore_unmanaged_workloads'] is True
    arg_do_not_delete_the_most_recent_workload = args['do_not_delete_the_most_recent_workload'] is True
    arg_do_not_delete_the_most_recently_heartbeating_workload = args['do_not_delete_the_most_recently_heartbeating_workload'] is True
    arg_do_not_delete_if_last_heartbeat_is_more_recent_than = args['do_not_delete_if_last_heartbeat_is_more_recent_than']
    arg_override_pce_offline_timer_to = args['override_pce_offline_timer_to']
    arg_limit_number_of_deleted_workloads = args['limit_number_of_deleted_workloads']
    arg_report_output_dir: str = args['output_dir']


    output_file_prefix = make_filename_with_timestamp('ven-duplicate-removal_', arg_report_output_dir)
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'


    csv_report_headers = pylo.ExcelHeaderSet([
        ExcelHeader(name = 'name', max_width = 40),
        ExcelHeader(name = 'hostname', max_width = 40)
    ])
    # insert all label dimensions
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(ExcelHeader(name= f'label_{label_type}', wrap_text= False))

    csv_report_headers.extend([
        'online',
        ExcelHeader(name = 'last_heartbeat', max_width = 15, wrap_text = False),
        ExcelHeader(name = 'created_at', max_width = 15, wrap_text = False),
        'action',
        ExcelHeader(name = 'link_to_pce', max_width = 15, wrap_text = False, url_text = 'See in PCE', is_url = True),
        ExcelHeader(name = 'href', max_width = 15, wrap_text = False)
    ])
    csv_report = pylo.ArraysToExcel()
    sheet: pylo.ArraysToExcel.Sheet = csv_report.create_sheet('duplicates', csv_report_headers, force_all_wrap_text=True, multivalues_cell_delimiter=',')


    filter_labels: List[pylo.Label] = []  # the list of labels to filter the workloads against
    if args['filter_label'] is not None:
        for label_name in args['filter_label']:
            label = org.LabelStore.find_label_by_name(label_name)
            if label is None:
                raise pylo.PyloEx("Cannot find label '{}' in the PCE".format(label_name))
            filter_labels.append(label)

    # <editor-fold desc="Download workloads from PCE">
    if not pce_cache_was_used:
        print("* Downloading Workloads data from the PCE (it may take moment for large amounts of workloads) ... ", flush=True, end='')
        if args['filter_label'] is None:
            workloads_json = org.connector.objects_workload_get(async_mode=True, max_results=1000000)
        else:
            filter_labels_list_of_list: List[List[pylo.Label]] = []
            # convert filter_labels dict to an array of arrays
            for label_type, label_list in org.LabelStore.Utils.list_to_dict_by_type(filter_labels).items():
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

            workloads_json = org.connector.objects_workload_get(async_mode=False, max_results=1000000, filter_by_label=filter_labels_matrix)

        org.WorkloadStore.load_workloads_from_json(workloads_json)

    print("OK! {} workloads loaded".format(org.WorkloadStore.count_workloads()))
    # </editor-fold>

    all_workloads: List[pylo.Workload]  # the list of all workloads to be processed

    if pce_cache_was_used:
        # if some filters were used, let's apply them now
        print("* Filtering workloads loaded from cache based on their labels... ", end='', flush=True)
        # if some label filters were used, we will apply them at later stage
        all_workloads: List[pylo.Workload] = list((org.WorkloadStore.find_workloads_matching_all_labels(filter_labels)).values())
        print("OK! {} workloads left after filtering".format(len(all_workloads)))
    else:
        # filter was already applied during the download from the PCE
        all_workloads = org.WorkloadStore.workloads

    def add_workload_to_report(workload: pylo.Workload, action: str):
        url_link_to_pce = workload.get_pce_ui_url()
        new_row = {
                'hostname': workload.hostname,
                'online': workload.online,
                'last_heartbeat': workload.ven_agent.get_last_heartbeat_date().strftime('%Y-%m-%d %H:%M'),
                'created_at': workload.created_at_datetime().strftime('%Y-%m-%d %H:%M'),
                'href': workload.href,
                'link_to_pce': url_link_to_pce,
                'action': action
        }

        for label_type in org.LabelStore.label_types:
            new_row['label_'+label_type] = workload.get_label_name(label_type, '')

        sheet.add_line_from_object(new_row)

    duplicated_hostnames = DuplicateRecordManager(arg_override_pce_offline_timer_to)

    print(" * Looking for VEN with duplicated hostname(s)")

    for workload in all_workloads:
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

        latest_created_workload = dup_record.find_latest_created_at()
        latest_heartbeat_workload = dup_record.find_latest_heartbeat()
        print("     - Latest created at {} and latest heartbeat at {}".format(latest_created_workload.created_at, latest_heartbeat_workload.ven_agent.get_last_heartbeat_date()))

        if dup_record.count_online() == 0:
            print("     - IGNORED: there is no VEN online")
            for wkl in dup_record.offline:
                add_workload_to_report(wkl, "ignored (no VEN online)")
            continue

        if dup_record.count_online() > 1:
            print("     - WARNING: there are more than 1 VEN online")

        # Don't delete online workloads but still show them in the report
        for wkl in dup_record.online:
            add_workload_to_report(wkl, "ignored (VEN is online)")

        for wkl in dup_record.offline:
            if arg_do_not_delete_the_most_recent_workload and  wkl is latest_created_workload:
                print("    - IGNORED: wkl {}/{} is the most recent".format(wkl.get_name_stripped_fqdn(), wkl.href))
                add_workload_to_report(wkl, "ignored (it is the most recently created)")
            elif arg_do_not_delete_the_most_recently_heartbeating_workload and wkl is latest_heartbeat_workload:
                print("    - IGNORED: wkl {}/{} is the most recently heartbeating".format(wkl.get_name_stripped_fqdn(), wkl.href))
                add_workload_to_report(wkl, "ignored (it is the most recently heartbeating)")
            elif arg_do_not_delete_if_last_heartbeat_is_more_recent_than is not None and wkl.ven_agent.get_last_heartbeat_date() > datetime.datetime.now() - datetime.timedelta(days=arg_do_not_delete_if_last_heartbeat_is_more_recent_than):
                print("    - IGNORED: wkl {}/{} has a last heartbeat more recent than {} days".format(wkl.get_name_stripped_fqdn(), wkl.href, arg_do_not_delete_if_last_heartbeat_is_more_recent_than))
                add_workload_to_report(wkl, "ignored (last heartbeat is more recent than {} days)".format(arg_do_not_delete_if_last_heartbeat_is_more_recent_than))
            else:
                if arg_limit_number_of_deleted_workloads is not None and delete_tracker.count_entries() >= arg_limit_number_of_deleted_workloads:
                    print("    - IGNORED: wkl {}/{} because the limit of {} workloads to be deleted was reached".format(wkl.get_name_stripped_fqdn(), wkl.href, arg_limit_number_of_deleted_workloads))
                    add_workload_to_report(wkl, "ignored (limit of {} workloads to be deleted was reached)".format(arg_limit_number_of_deleted_workloads))
                else:
                    delete_tracker.add_workload(wkl)
                    print("    - added offline wkl {}/{} to the delete list".format(wkl.get_name_stripped_fqdn(), wkl.href))


        for wkl in dup_record.unmanaged:
            if arg_limit_number_of_deleted_workloads is not None and delete_tracker.count_entries() >= arg_limit_number_of_deleted_workloads:
                print("    - IGNORED: wkl {}/{} because the limit of {} workloads to be deleted was reached".format(wkl.get_name_stripped_fqdn(), wkl.href, arg_limit_number_of_deleted_workloads))
                add_workload_to_report(wkl, "ignored (limit of {} workloads to be deleted was reached)".format(arg_limit_number_of_deleted_workloads))
            else:
                delete_tracker.add_workload(wkl)
                print("    - added unmanaged wkl {}/{} to the delete list".format(wkl.get_name_stripped_fqdn(), wkl.href))

    print()

    if delete_tracker.count_entries() < 1:
        print(" * No workloads to be deleted")

    elif arg_proceed_with_deletion:
        print(" * Found {} workloads to be deleted. Listing:".format(delete_tracker.count_entries()))
        for wkl in delete_tracker.workloads:
            print("    - {} (href: {} url: {})".format(wkl.get_name_stripped_fqdn(), wkl.href, wkl.get_pce_ui_url()))

        print()

        deletion_confirmed = False

        if arg_do_not_require_deletion_confirmation:
            print(" * '--do-not-require-deletion-confirmation' option was used, no confirmation will be asked")
        else:
            deletion_confirmed = click.confirm(" * Are you sure you want to proceed with the deletion of {} workloads?".format(delete_tracker.count_entries()))

        if not deletion_confirmed:
            print(" * Aborted by user")
            for wkl in delete_tracker.workloads:
                add_workload_to_report(wkl, "TO BE DELETED (aborted by user)")
        else:
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
        print(" * Found {} workloads to be deleted BUT NO '--proceed-with-deletion' OPTION WAS USED".format(delete_tracker.count_entries()))
        for wkl in delete_tracker.workloads:
            add_workload_to_report(wkl, "TO BE DELETED (no confirm option used)")

    if sheet.lines_count() >= 1:
        if len(report_wanted_format) < 1:
            print(" * No report format was specified, no report will be generated")
        else:
            sheet.reorder_lines(['hostname']) # sort by hostname for better readability
            for report_format in report_wanted_format:
                output_filename = output_file_prefix + '.' + report_format
                print(" * Writing report file '{}' ... ".format(output_filename), end='', flush=True)
                if report_format == 'csv':
                    sheet.write_to_csv(output_filename)
                elif report_format == 'xlsx':
                    csv_report.write_to_excel(output_filename)
                else:
                    raise pylo.PyloEx("Unknown format for report: '{}'".format(report_format))
                print("DONE")

    else:
        print("\n** WARNING: no entry matched your filters so reports were not generated !\n")


# make this command available to the CLI system
command_object = Command(command_name, __main, fill_parser, objects_load_filter)

class DuplicateRecordManager:
    class DuplicatedRecord:
        def __init__(self, pce_offline_timer_override: Optional[int] = None):
            self.offline = []
            self.online = []
            self.unmanaged= []
            self.all: List[pylo.Workload] = []
            self._pce_offline_timer_override: Optional[int] = pce_offline_timer_override

        def add_workload(self, workload: 'pylo.Workload'):
            self.all.append(workload)
            if workload.unmanaged:
                self.unmanaged.append(workload)
            elif self._pce_offline_timer_override is None:
                if workload.online:
                    self.online.append(workload)
                else:
                    self.offline.append(workload)
            else:
                if workload.ven_agent.get_last_heartbeat_date() > datetime.datetime.now() - datetime.timedelta(days=self._pce_offline_timer_override):
                    self.online.append(workload)
                else:
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

        def find_latest_created_at(self)-> 'pylo.Workload':
            latest: Optional[pylo.Workload] = None
            for wkl in self.all:
                if wkl.unmanaged:
                    continue
                if latest is None or wkl.created_at > latest.created_at:
                    latest = wkl
            return latest

        def find_latest_heartbeat(self)-> 'pylo.Workload':
            latest: Optional[pylo.Workload] = None
            for wkl in self.all:
                if wkl.unmanaged:
                    continue
                if latest is None or wkl.ven_agent.get_last_heartbeat_date() > latest.ven_agent.get_last_heartbeat_date():
                    latest = wkl
            return latest

    def __init__(self, pce_offline_timer_override: Optional[int] = None):
        self._records: Dict[str, DuplicateRecordManager.DuplicatedRecord] = {}
        self._pce_offline_timer_override: Optional[int] = pce_offline_timer_override

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
            self._records[lower_hostname] = self.DuplicatedRecord(self._pce_offline_timer_override)
        record = self._records[lower_hostname]
        record.add_workload(workload)
