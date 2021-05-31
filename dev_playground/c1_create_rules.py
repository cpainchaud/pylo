import os
import sys
import io
import argparse
import shlex
from datetime import datetime
import socket
from typing import Union, Optional, Dict, List, Any
import c1_shared

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo

# makes unbuffered output
sys.stdout = io.TextIOWrapper(open(sys.stdout.fileno(), 'wb', 0), write_through=True)
sys.stderr = io.TextIOWrapper(open(sys.stderr.fileno(), 'wb', 0), write_through=True)

# <editor-fold desc="Handling of arguments provided on stdin">
parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--args-from-input', type=bool, required=False, default=False, const=True, nargs='?')
args, unknown = parser.parse_known_args()

if args.args_from_input:
    input_str = input("Please enter arguments now: ")
    input_args = shlex.split(input_str)
# </editor-fold>

# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', '--host', type=str, required=True,
                    help='hostname of the PCE')

parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')

parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')

parser.add_argument('--input-file', type=str, required=True,
                    help='input for rules creation')



if args.args_from_input:
    args = vars(parser.parse_args(input_args))
else:
    args = vars(parser.parse_args())

# </editor-fold>

debug = args['debug']

if debug:
    pylo.log_set_debug()
    print(args)

hostname = args['pce']
org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()
use_cached_config = args['dev_use_cache']

# <editor-fold desc="FINGERPRINT data extraction">
csv_expected_fields = [
    {'name': 'app', 'optional': False},
    {'name': 'env', 'optional': False},
    {'name': 'loc', 'optional': True}
]
print(" * Loading Excel file FINGERPRINT infos '{}'...".format(args['input_file']), flush=True, end='')
FingerPrintData = pylo.CsvExcelToObject(args['input_file'], expected_headers=csv_expected_fields, excel_sheet_name=c1_shared.excel_doc_sheet_fingerprint_title)
if FingerPrintData.count_lines() != 1:
    raise pylo.PyloEx("Excel file has wrong line count in the FINGERPRINT section: {}".format(FingerPrintData.count_lines()))
app_label_href = FingerPrintData.objects()[0]['app']
env_label_href = FingerPrintData.objects()[0]['env']
loc_label_href = FingerPrintData.objects()[0]['loc']
if app_label_href is None or type(app_label_href) is not str or len(app_label_href) < 1 or \
        env_label_href is None or type(env_label_href) is not str or len(env_label_href) < 1 or \
        loc_label_href is None or type(loc_label_href) is not str or len(loc_label_href) < 1:
    raise pylo.PyloEx("Failed to find proper labels i FINGERPRINT sheet")
print('OK')
# </editor-fold>

# <editor-fold desc="Rules Extraction From Excel">
csv_expected_fields = [
    {'name': 'src_role', 'optional': False},
    {'name': 'src_application', 'optional': False},
    {'name': 'src_environment', 'optional': False},
    {'name': 'src_location', 'optional': False},
    {'name': 'dst_role', 'optional': False},
    {'name': 'dst_port', 'optional': False},
    {'name': 'to_be_implemented', 'optional': False}
]
print(" * Loading Inbound Traffic rules from Excel file '{}'...".format(args['input_file']), flush=True, end='')
InboundRuleData = pylo.CsvExcelToObject(args['input_file'],
                                        expected_headers=csv_expected_fields,
                                        excel_sheet_name=c1_shared.excel_doc_sheet_inbound_identified_title)

print("OK! ({} lines, {} empty)".format(InboundRuleData.count_lines(), InboundRuleData.count_empty_lines()))

csv_expected_fields = [
    {'name': 'dst_role', 'optional': False},
    {'name': 'dst_application', 'optional': False},
    {'name': 'dst_environment', 'optional': False},
    {'name': 'dst_location', 'optional': False},
    {'name': 'src_role', 'optional': False},
    {'name': 'dst_port', 'optional': False},
    {'name': 'to_be_implemented', 'optional': False}
]
print(" * Loading Outbound Traffic rules from Excel file '{}'...".format(args['input_file']), flush=True, end='')
OutboundRuleData = pylo.CsvExcelToObject(args['input_file'],
                                        expected_headers=csv_expected_fields,
                                        excel_sheet_name=c1_shared.excel_doc_sheet_outbound_identified_title)

print("OK! ({} lines, {} empty)".format(OutboundRuleData.count_lines(), OutboundRuleData.count_empty_lines()))
# </editor-fold>

if OutboundRuleData.count_lines() < 1 and InboundRuleData.count_lines() < 1:
    print("**** Excel file contains no Inbound/Outbound traffic to allow, EXIT NOW ****")
    exit(0)


# <editor-fold desc="PCE DB extraction">
if use_cached_config:
    print(" * Loading PCE Database from cached file or API if not available... ", end='', flush=True)
    org.load_from_cache_or_saved_credentials(hostname, prompt_for_api_key_if_missing=False)
    connector = org.connector
    print("OK!")
else:
    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading and Parsing PCE Data... ", end="", flush=True)
    org.load_from_api(connector)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))
# </editor-fold>


print("")
print(" - Looking for APP label with HREF '{}' in PCE database...".format(app_label_href), end='')
app_label = org.LabelStore.find_by_href_or_die(app_label_href)
if app_label is None:
    pylo.log.error("NOT FOUND!")
    exit(1)
print(" OK! (label name is '{}')".format(app_label.name))

print(" - Looking for ENV label with HREF '{}' in PCE database...".format(env_label_href), end='')
env_label = org.LabelStore.find_by_href_or_die(env_label_href)
if env_label is None:
    pylo.log.error("NOT FOUND!")
    exit(1)
print(" OK! (label name is '{}')".format(env_label.name))

print(" - Looking for LOC label with HREF '{}' in PCE database...".format(loc_label_href), end='')
loc_label = org.LabelStore.find_by_href_or_die(loc_label_href)
if loc_label is None:
    pylo.log.error("NOT FOUND!")
    exit(1)
print(" OK! (label name is '{}')".format(loc_label.name))


class InboundRuleToCreate:
    _services: List[Union['pylo.DirectServiceInRule','pylo.Service']]
    _consumer_labels: List[pylo.Label]
    _provider_labels: List[pylo.Label]

    def __init__(self, data, context: pylo.Organization,
                 provider_app: pylo.Label, provider_env: pylo.Label,
                 provider_loc: pylo.Label
                 ):
        self._intra = False
        self._consumer_labels = []
        self._provider_labels = []
        self._services = []

        self.src_role_label = org.LabelStore.find_label_by_name_and_type(data['src_role'], pylo.label_type_role)
        if self.src_role_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['src_role'], data))
        self.src_app_label = org.LabelStore.find_label_by_name_and_type(data['src_application'], pylo.label_type_app)
        if self.src_app_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['src_application'], data))
        self.src_env_label = org.LabelStore.find_label_by_name_and_type(data['src_environment'], pylo.label_type_env)
        if self. src_env_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['src_environment'], data))
        self.src_loc_label = org.LabelStore.find_label_by_name_and_type(data['src_location'], pylo.label_type_loc)
        if self.src_loc_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['src_location'], data))

        self.dst_role_label = org.LabelStore.find_label_by_name_and_type(data['dst_role'], pylo.label_type_role)
        if self.dst_role_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['dst_role'], data))

        self.dst_app_label = provider_app
        self.dst_env_label = provider_env
        self.dst_loc_label = provider_loc

        service = data.get('dst_port')
        if service is None or type(service) is not str:
            raise pylo.PyloEx("Cannot find Service/port in the following data:", data)

        service_split = service.split('/')
        if len(service_split) != 2:
            raise pylo.PyloEx("invalid service format from Excel input:", service)

        if not service_split[0].isdigit():
            raise pylo.PyloEx("port number is not numeric '{}':".format(service_split[0]), service)

        protocol_to_int = 6
        if service_split[1].lower() == 'udp':
            protocol_to_int = 17
        elif service_split[1].lower() == 'tcp':
            pass
        else:
            raise pylo.PyloEx("invalid protocol given: '{}'".format(service_split[1]))

        self._services.append(pylo.DirectServiceInRule(proto=protocol_to_int, port=int(service_split[0])))

    def create_in_pce(self, ruleset: 'pylo.Ruleset'):
        ruleset.create_rule(
            intra_scope=False,
            consumers=[self.src_role_label, self.src_app_label, self.src_env_label, self.src_loc_label],
            providers=[self.dst_role_label],
            services=self._services
        )


    def quick_str(self):
        return "FROM {} TO '{}  - {} - {} - {}' on port {}".format(self.src_role_label.name,
                                                                   self.dst_role_label.name,
                                                                   self.dst_app_label.name,
                                                                   self.dst_env_label.name,
                                                                   self.dst_loc_label.name,
                                                                   self._services[0].to_string_standard())

    def generate_expected_ruleset_name(self) -> str:
            return '{}_{}_{}'.format(self.dst_app_label.name, self.dst_env_label.name, self.dst_loc_label.name)

    def generate_api_payload(self):
        result = {}
        pass


class OutboundRuleToCreate:
    _services: List[Union['pylo.DirectServiceInRule', 'pylo.Service']]
    _consumer_labels: List[pylo.Label]
    _provider_labels: List[pylo.Label]

    def __init__(self, data, context: pylo.Organization,
                 consumer_app: pylo.Label, consumer_env: pylo.Label,
                 consumer_loc: pylo.Label
                 ):
        self._intra = False
        self._consumer_labels = []
        self._provider_labels = []
        self._services = []

        self.dst_role_label = org.LabelStore.find_label_by_name_and_type(data['dst_role'], pylo.label_type_role)
        if self.dst_role_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['dst_role'], data))
        self.dst_app_label = org.LabelStore.find_label_by_name_and_type(data['dst_application'], pylo.label_type_app)
        if self.dst_app_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['dst_application'], data))
        self.dst_env_label = org.LabelStore.find_label_by_name_and_type(data['dst_environment'], pylo.label_type_env)
        if self. dst_env_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['dst_environment'], data))
        self.dst_loc_label = org.LabelStore.find_label_by_name_and_type(data['dst_location'], pylo.label_type_loc)
        if self.dst_loc_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['dst_location'], data))

        self.src_role_label = org.LabelStore.find_label_by_name_and_type(data['src_role'], pylo.label_type_role)
        if self.src_role_label is None:
            raise pylo.PyloEx("Cannot find label named '{}' for the following rule record: {}".format(data['src_role'], data))

        self.src_app_label = consumer_app
        self.src_env_label = consumer_env
        self.src_loc_label = consumer_loc

        service = data.get('dst_port')
        if service is None or type(service) is not str:
            raise pylo.PyloEx("Cannot find Service/port in the following data:", data)

        service_split = service.split('/')
        if len(service_split) != 2:
            raise pylo.PyloEx("invalid service format from Excel input:", service)

        if not service_split[0].isdigit():
            raise pylo.PyloEx("port number is not numeric '{}':".format(service_split[0]), service)

        port = int(service_split[0])
        protocol_to_int = 6

        if service_split[1].lower() == 'udp':
            protocol_to_int = 17
        elif service_split[1].lower() == 'tcp':
            pass
        elif service_split[1].lower() == 'proto':
            protocol_to_int = service_split[0]
            port = 0
        else:
            raise pylo.PyloEx("invalid protocol given: '{}'".format(service_split[1]))

        self._services.append(pylo.DirectServiceInRule(proto=protocol_to_int, port=int(service_split[0])))


    def generate_expected_ruleset_name(self) -> str:
        return '{}_{}_{}'.format(self.dst_app_label.name, self.dst_env_label.name, self.dst_loc_label.name)

    def generate_api_payload(self):
        result = {}
        pass

    def create_in_pce(self, ruleset: 'pylo.Ruleset'):
        ruleset.create_rule(
            intra_scope=False,
            consumers=[self.src_role_label, self.src_app_label, self.src_env_label, self.src_loc_label],
            providers=[self.dst_role_label],
            services=self._services
        )


    def quick_str(self):
        return "FROM {} TO '{}  - {} - {} - {}' on port {}".format(self.src_role_label.name,
                                                self.dst_role_label.name,
                                                self.dst_app_label.name,
                                                self.dst_env_label.name,
                                                self.dst_loc_label.name,
                                                self._services[0].to_string_standard())


# <editor-fold desc="Inbound Rules Processing">
print()
print(" * Parsing Excel data for Inbound rules to be created:")
inbound_rules : List[InboundRuleToCreate] = []
for inbound_raw_line in InboundRuleData.objects():
    if type(inbound_raw_line['to_be_implemented']) is bool and inbound_raw_line['to_be_implemented']:
        rule_to_create = InboundRuleToCreate(inbound_raw_line, org, app_label, env_label, loc_label)
        inbound_rules.append(rule_to_create)

        print("  - {}".format(rule_to_create.quick_str()))

print(" ** Inbound rules parsing done (found {})".format(len(inbound_rules)))

print()
if len(inbound_rules) < 1:
    print(" * No inbound rule to create")
else:
    inbound_ruleset_name = inbound_rules[0].generate_expected_ruleset_name()
    print(" * Creating Inbound Rules in the PCE:")
    print("   - inbound ruleset name: {}".format(inbound_ruleset_name))
    inbound_ruleset = org.RulesetStore.find_ruleset_by_name(inbound_ruleset_name)
    if inbound_ruleset is None:
        print("    - ruleset not found, let's create it...", end='')
        org.RulesetStore.create_ruleset(inbound_ruleset_name,
                         inbound_rules[0].dst_app_label,
                         inbound_rules[0].dst_env_label,
                         inbound_rules[0].dst_loc_label)
        print("OK")

    for inbound_rule in inbound_rules:
        print("   - Creating rule: {} ....... ".format(inbound_rule.quick_str()), end='')
        inbound_rule.create_in_pce(inbound_ruleset)
        print("OK")

# </editor-fold>


# <editor-fold desc="Outbound Rules Processing">
print()
print(" * Parsing Excel data for Outbound rules to be created:")
outbound_rules : List[OutboundRuleToCreate] = []
for outbound_raw_line in OutboundRuleData.objects():
    if type(outbound_raw_line['to_be_implemented']) is bool and outbound_raw_line['to_be_implemented']:
        rule_to_create = OutboundRuleToCreate(outbound_raw_line, org, app_label, env_label, loc_label)
        outbound_rules.append(rule_to_create)

        print("  - {}".format(rule_to_create.quick_str()))

print(" ** Outbound rules parsing done (found {})".format(len(outbound_rules)))

print()
if len(outbound_rules) < 1:
    print(" * No outbound rule to create")
else:
    print(" * Creating Outbound Rules in the PCE:")

    for outbound_rule in outbound_rules:
        print("   - Dealing with rule: {} :".format(outbound_rule.quick_str()))

        outbound_ruleset_name = outbound_rules[0].generate_expected_ruleset_name()
        print("      - outbound ruleset name: {}".format(outbound_ruleset_name))
        outbound_ruleset = org.RulesetStore.find_ruleset_by_name(outbound_ruleset_name)
        if outbound_ruleset is None:
            print("        - ruleset not found, let's create it...", end='')
            outbound_ruleset = org.RulesetStore.create_ruleset(outbound_ruleset_name,
                                            outbound_rules[0].dst_app_label,
                                            outbound_rules[0].dst_env_label,
                                            outbound_rules[0].dst_loc_label)
            print("OK")

        print("      - pushing rule to PCE... ", end='')
        outbound_rule.create_in_pce(outbound_ruleset)
        print("OK")

# </editor-fold>

