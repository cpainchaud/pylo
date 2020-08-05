import pylo
import ipaddress
import copy
from typing import Optional, List, Dict


def sort_first(val):
    return val[0]


start = 0
end = 1


class IP4Map:
    def __init__(self):
        self._entries = []

    @staticmethod
    def ip_entry_from_text(entry: str, ignore_ipv6=False) -> Optional[List[int]]:
        new_entry = None

        dash_find = entry.find('-')

        if dash_find > 0:
            # this is a range entry
            start_txt = entry[0:dash_find]
            if ignore_ipv6 and pylo.is_valid_ipv6(start_txt):
                return None
            end_txt = entry[dash_find+1:]
            if ignore_ipv6 and pylo.is_valid_ipv6(end_txt):
                return None
            start_ip_object = ipaddress.IPv4Address(start_txt)
            end_ip_object = ipaddress.IPv4Address(end_txt)
            new_entry = [int(start_ip_object), int(end_ip_object)]
            if new_entry[start] > new_entry[end]:
                raise pylo.PyloEx("Invalid IP Ranged entered with start address > end address: {}".format(entry))
        elif entry.find('/') > 0:
            # This is a network entry
            network_str = entry[0:(entry.find('/')-1)]
            if ignore_ipv6 and (pylo.is_valid_ipv6(network_str) or network_str == '::'):
                return None
            ip_object = ipaddress.IPv4Network(entry)
            new_entry = [int(ip_object.network_address), int(ip_object.broadcast_address)]
            if ignore_ipv6 and pylo.is_valid_ipv6(ip_object.network_address.__str__()):
                return None
        else:
            if ignore_ipv6 and pylo.is_valid_ipv6(entry):
                return None
            ip_object = ipaddress.IPv4Address(entry)
            new_entry = [int(ip_object), int(ip_object)]

        return new_entry

    def add_from_text(self, entry: str, skip_recalculation=False, ignore_ipv6=False):

        new_entry = self.ip_entry_from_text(entry, ignore_ipv6=ignore_ipv6)

        if ignore_ipv6 and new_entry is None:
            return

        if not skip_recalculation:
            self._entries.append(new_entry)
            self.sort_and_recalculate()

    def intersection(self, another_map: 'IP4Map'):

        inverted_map = IP4Map()
        inverted_map.add_from_text('0.0.0.0-255.255.255.255')

        inverted_map.substract(another_map)

        result = copy.deepcopy(self)
        result.substract(inverted_map)

        return result

    def substract(self, another_map: 'IP4Map'):
        affected_rows = 0
        for entry in another_map._entries:
            affected_rows += self.substract_single_entry(entry)
        return affected_rows

    def substract_single_entry(self, sub_entry: []) -> int:
        affected_rows = 0
        updated_entries = []

        for entry in self._entries:
            if sub_entry[end] < entry[start] or sub_entry[start] > entry[end]:
                # no overlap at all, entry is left untouched
                updated_entries.append(entry)
                continue

            affected_rows += 1
            if sub_entry[start] <= entry[start]:
                if sub_entry[end] >= entry[end]:
                    # complete overlap, remove entry
                    continue
                else:
                    entry[start] = sub_entry[end] + 1
                    updated_entries.append(entry)
            else:
                updated_entries.append([entry[start], sub_entry[start] - 1])
                if sub_entry[end] < entry[end]:
                    updated_entries.append([sub_entry[end] + 1, entry[end]])

        self._entries = updated_entries

        return affected_rows



    def substract_from_text(self, entry: str, ignore_ipv6=False):

        new_entry = self.ip_entry_from_text(entry, ignore_ipv6=ignore_ipv6)

        if new_entry is None:
            return 0

        return self.substract_single_entry(new_entry)


    def sort_and_recalculate(self):
        new_entries = []

        self._entries.sort(key=sort_first)

        cursor = None
        for entry in self._entries:
            if cursor is None:
                cursor = entry
                continue


            if entry[start] > cursor[end]:
                new_entries.append(cursor)
                cursor = entry
                continue

            if entry[end] > cursor[end]:
                cursor[end] = entry[end]
                continue

            if entry[end] < cursor[end]:
                continue

            raise pylo.PyloEx("Error while sorting IP4Map, unexpected value found: entry({}-{}) cursor({}-{})".format(
                ipaddress.IPv4Address(entry[start]),
                ipaddress.IPv4Address(entry[end]),
                ipaddress.IPv4Address(cursor[start]),
                ipaddress.IPv4Address(cursor[end])
            ))


        if cursor is not None:
            new_entries.append(cursor)

        self._entries = new_entries


    def to_string_list(self):
        ranges = []

        for entry in self._entries:
            ranges.append('{}-{}'.format(ipaddress.IPv4Address(entry[start]), ipaddress.IPv4Address(entry[end])))

        return ranges

    def print_to_std(self, header=None, padding='', list_marker=' - '):
        if header is not None:
            print('{}{}({} entries)'.format(
                padding,
                header,
                len(self._entries)))

        for text in self.to_string_list():
            print('{}{}{}'.format(padding, list_marker, text))


# test = IP4Map()
# test.add_from_text('10.0.0.0/16')
# test.add_from_text('10.0.0.0-10.2.50.50')
# test.add_from_text('192.168.1.2')
# test.add_from_text('1.0.0.0/8')
# test.add_from_text('192.168.1.0-192.168.2.0')
# test.substract_from_text('192.168.0.0-192.168.1.255')
#
# test.add_from_text('200.0.0.0-200.1.255.255')
# test.substract_from_text('199.255.255.255-200.1.0.0')  # should produce 200.1.0.1-200.1.255.255
#
# test.add_from_text('200.10.0.0-200.11.255.255')
# test.substract_from_text('200.10.10.10-200.11.0.0')  # should produce 200.10.0.0-200.10.10.9 and 200.11.0.1-200.11.255.255
#
# test.add_from_text('200.20.0.0-200.21.255.255')
# test.substract_from_text('200.20.10.10-200.22.0.0')  # should produce 200.20.0.0-200.20.10.9
#
# test.print_to_std(header="Show IP4Map test:", padding='')
