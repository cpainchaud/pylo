import pylo
from pylo import log


class RulesetScope:
    """
    :type owner: pylo.Ruleset
    """

    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner = owner
        self.scope_entries = {}  # type: dict[pylo.RulesetScopeEntry, pylo.RulesetScopeEntry]

    def load_from_json(self, data):
        for scope_json in data:
            scope_entry = pylo.RulesetScopeEntry(self)
            scope_entry.load_from_json(scope_json)
            self.scope_entries[scope_entry] = scope_entry


class RulesetScopeEntry:
    """
    :type owner: pylo.RulesetScope
    """

    def __init__(self, owner: 'pylo.RulesetScope'):
        self.owner = owner
        self.loc_label = None  # type: pylo.Label
        self.env_label = None  # type: pylo.Label
        self.app_label = None  # type: pylo.Label

    def load_from_json(self, data):
        self.loc_label = None
        #log.error(pylo.nice_json(data))
        l_store = self.owner.owner.owner.owner.LabelStore
        for label_json in data:
            label_entry = label_json.get('label')
            if label_entry is None:
                label_entry = label_json.get('label_group')
                if label_entry is None:
                    raise pylo.PyloEx("Cannot find 'label' or 'label_group' entry in scope: {}".format(pylo.nice_json(label_json)))
            href_entry = label_entry.get('href')
            if href_entry is None:
                raise pylo.PyloEx("Cannot find 'href' entry in scope: {}".format(pylo.nice_json(data)))

            label = l_store.find_by_href_or_die(href_entry)
            if label.type_is_location():
                self.loc_label = label
            elif label.type_is_environment():
                self.env_label = label
            elif label.type_is_application():
                self.app_label = label
            else:
                raise pylo.PyloEx("Unsupported label type '{}' in scope".format(label.type_string()))

    def to_string(self):
        string = 'All/'
        if self.app_label is not None:
            string = self.app_label.name + '/'

        if self.env_label is None:
            string += 'All/'
        else:
            string += self.env_label.name + '/'

        if self.loc_label is None:
            string += 'All'
        else:
            string += self.loc_label.name

        return string

    def is_all_all_all(self):
        if self.app_label is None and self.env_label is None and self.loc_label is None:
            return True
        return False



class Ruleset:

    def __init__(self, owner: 'pylo.RulesetStore'):
        self.owner = owner
        self.href = None  # type: str
        self.name = None  # type: str
        self.description = None  # type: str
        self.scopes = pylo.RulesetScope(self)
        self.rules_byHref = {}  # type: dict[str,pylo.Rule]

    def load_from_json(self, data):
        if 'name' not in data:
            raise pylo.PyloEx("Cannot find Ruleset name in JSON data: \n" + pylo.Helpers.nice_json(data))
        self.name = data['name']

        if 'href' not in data:
            raise pylo.PyloEx("Cannot find Ruleset href in JSON data: \n" + pylo.Helpers.nice_json(data))
        self.href = data['href']

        if 'scopes' not in data:
            raise pylo.PyloEx("Cannot find Ruleset scope in JSON data: \n" + pylo.Helpers.nice_json(data))

        self.description = data.get('description')

        self.scopes.load_from_json(data['scopes'])


        if 'rules' in data:
            for rule_data in data['rules']:
                new_rule = pylo.Rule(self)
                new_rule.load_from_json(rule_data)
                self.rules_byHref[new_rule.href] = new_rule

    def count_rules(self):
        return len(self.rules_byHref)


class Rule:
    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner = owner  # type: pylo.Ruleset
        self.description = None  # type: str
        self.services = pylo.RuleServiceContainer(self)
        self.providers = pylo.RuleHostContainer(self, 'providers')
        self.consumers = pylo.RuleHostContainer(self, 'consumers')
        self.consuming_principals = pylo.RuleSecurityPrincipalContainer(self)
        self.href = None
        self.enabled = True
        self.secure_connect = False
        self.unscoped_consumers = False

    def load_from_json(self, data):
        self.href = data['href']

        self.description = data.get('description')

        services = data.get('ingress_services')
        if services is not None:
            self.services.load_from_json(services)

        enabled = data.get('enabled')
        if enabled is not None:
            self.enabled = enabled

        secure_connect = data.get('sec_connect')
        if secure_connect is not None:
            self.secure_connect = secure_connect

        unscoped_consumers = data.get('unscoped_consumers')
        if unscoped_consumers is not None:
            self.unscoped_consumers = unscoped_consumers

        self.providers.load_from_json(data['providers'])
        self.consumers.load_from_json(data['consumers'])
        self.consuming_principals.load_from_json(data['consuming_security_principals'])


class RuleSecurityPrincipalContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule'):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: dict[pylo.SecurityPrincipal, pylo.SecurityPrincipal]

    def load_from_json(self, data):
        ssStore = self.owner.owner.owner.owner.SecurityPrincipalStore
        for item_data in data:
            wanted_href = item_data['href']
            found_object = ssStore.find_by_href_or_die(wanted_href)
            found_object.add_reference(self)
            self._items[found_object] = found_object


class DirectServiceInRule:
    def __init__(self, proto: int, port: int, toport: int):
        self.protocol = proto
        self.port = port
        self.to_port = toport

    def to_string_standard(self):
        if self.protocol == 17:
            if self.to_port is None:
                return 'udp/' + str(self.port)
            return 'udp/' + str(self.port) + '-' + str(self.to_port)
        elif self.protocol == 6:
            if self.to_port is None:
                return 'tcp/' + str(self.port)
            return 'tcp/' + str(self.port) + '-' + str(self.to_port)

        return 'proto/' + str(self.protocol)


class RuleServiceContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule'):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: dict[pylo.Service, pylo.Service]
        self._direct_services = []

    def load_from_json_legacy_single(self, data):
        href = data.get('href')
        if href is None:
            raise Exception('Cannot find service HREF')

        find_service = self.owner.owner.owner.owner.ServiceStore.itemsByHRef.get(href)
        if find_service is None:
            raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

        self._items[find_service] = find_service
        find_service.add_reference(self)

    def load_from_json(self, data_list):
        for data in data_list:
            # print(data)
            href = data.get('href')
            if href is None:
                port = data.get('port')
                if port is None:
                     raise pylo.PyloEx("unsupported service type in rule: {}".format(pylo.nice_json(data)))
                protocol = data.get('proto')
                if protocol is None:
                    raise pylo.PyloEx("Protocol not found in direct service use: {}".format(pylo.nice_json(data)))

                to_port = data.get('to_port')
                direct_port = DirectServiceInRule(protocol, port, to_port)
                self._direct_services.append(direct_port)

                continue

            find_service = self.owner.owner.owner.owner.ServiceStore.itemsByHRef.get(href)
            if find_service is None:
                raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

            self._items[find_service] = find_service
            find_service.add_reference(self)


class RuleHostContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule', name: str):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}
        self.name = name
        self._hasAllWorkloads = False

    def load_from_json(self, data):
        for host_data in data:
            find_object = None
            if 'label' in host_data:
                href = host_data['label'].get('href')
                if href is None:
                    raise Exception('Cannot find object HREF ')
                find_object = self.owner.owner.owner.owner.LabelStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find Label with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'label_group' in host_data:
                href = host_data['label_group'].get('href')
                if href is None:
                    raise Exception('Cannot find object HREF ')
                find_object = self.owner.owner.owner.owner.LabelStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find LabelGroup with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'ip_list' in host_data:
                href = host_data['ip_list'].get('href')
                if href is None:
                    raise Exception('Cannot find object HREF ')
                find_object = self.owner.owner.owner.owner.IPListStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find IPList with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'workload' in host_data:
                href = host_data['workload'].get('href')
                if href is None:
                    raise Exception('Cannot find object HREF ')
                # @TODO : better handling of temporary objects
                find_object = self.owner.owner.owner.owner.WorkloadStore.itemsByHRef.get(href)
                if find_object is None:
                    # raise Exception("Cannot find Workload with HREF {} in Rule {}. JSON:\n {}".format(href, self.owner.href, pylo.nice_json(host_data)))
                    find_object = self.owner.owner.owner.owner.WorkloadStore.find_by_href_or_create_tmp(href, 'tmp-deleted-wkl-'+href)
            elif 'actors' in host_data:
                actor_value = host_data['actors']
                if actor_value is not None and actor_value == 'ams':
                    self._hasAllWorkloads = True
                    continue
                # TODO implement actors
                raise pylo.PyloEx("An actor that is not 'ams' was detected but this library doesn't support it yet")
            else:
                raise Exception("Unsupported reference type: {}\n".format(pylo.nice_json(host_data)))

            if find_object is not None:
                self._items[find_object] = find_object
                find_object.add_reference(self)


class RulesetStore:

    """
    :type owner: pylo.Organization
    :type itemsByHRef: dict[str,Ruleset]
    :type itemsByName: dict[str,Ruleset]
    """

    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.itemsByHRef = {}
        self.itemsByName = {}

    def count_rulesets(self):
        return len(self.itemsByHRef)

    def count_rules(self):
        count = 0
        for ruleset in self.itemsByHRef.values():
            count += ruleset.count_rules()

        return count

    def load_rulesets_from_json(self, data):
        for json_item in data:

            new_item = pylo.Ruleset(self)
            new_item.load_from_json(json_item)

            if new_item.href in self.itemsByHRef:
                raise Exception("A Ruleset with href '%s' already exists in the table, please check your JSON data for consistency. JSON:\n%s"
                                % (new_item.href, pylo.nice_json(json_item)) )

            if new_item.name in self.itemsByName:
                print("The following Ruleset is conflicting (name already exists): '%s' Href: '%s'" % (self.itemsByName[new_item.name].name, self.itemsByName[new_item.name].href), flush=True)
                raise Exception("A Ruleset with name '%s' already exists in the table, please check your JSON data for consistency. JSON:\n%s"
                                % (new_item.name, pylo.nice_json(json_item)))

            self.itemsByHRef[new_item.href] = new_item
            self.itemsByName[new_item.name] = new_item

            log.debug("Found Ruleset '%s' with href '%s'" % (new_item.name, new_item.href) )
