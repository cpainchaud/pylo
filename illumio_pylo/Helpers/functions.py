import json
import os
import re
import time


def nice_json(json_obj):
    return json.dumps(json_obj, indent=2, sort_keys=True)


def string_list_to_text(string_list, separator=',') -> str:
    """

    :type string_list: List[str]
    :type separator: str
    """
    msg = ""
    first = True
    for stringItem in string_list:
        if type(stringItem) is str:
            str_to_print = stringItem
        else:
            str_to_print = stringItem.name

        if not first:
            msg += separator
        first = False
        msg += str_to_print
    # print("list length {} and msg ='{}'".format(len(string_list), msg))
    return msg


def obj_with_href_list_to_text(string_list, separator=',') -> str:
    """

    :type string_list: List[str]
    :type separator: str
    """
    msg = ""
    first = True
    for stringItem in string_list:
        str_to_print = stringItem.href

        if not first:
            msg += separator
        first = False
        msg += str_to_print
    # print("list length {} and msg ='{}'".format(len(string_list), msg))
    return msg


def file_clean(path, no_print=False):
    if not no_print:
        print(" * Cleaning file '{}' from previous runs... ".format(path), end='', flush=True)
    if os.path.exists(path):
        if not os.path.isfile(path):
            raise Exception("Provided path '{}' is not a file!".format(path))
        else:
            os.remove(path)
    print("OK!")


___ipv4_pattern = re.compile(r"""
        ^
        (?:
          # Dotted variants:
          (?:
            # Decimal 1-255 (no leading 0's)
            [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
          |
            0x0*[0-9a-f]{1,2}  # Hexadecimal 0x0 - 0xFF (possible leading 0's)
          |
            0+[1-3]?[0-7]{0,2} # Octal 0 - 0377 (possible leading 0's)
          )
          (?:                  # Repeat 0-3 times, separated by a dot
            \.
            (?:
              [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
            |
              0x0*[0-9a-f]{1,2}
            |
              0+[1-3]?[0-7]{0,2}
            )
          ){0,3}
        |
          0x0*[0-9a-f]{1,8}    # Hexadecimal notation, 0x0 - 0xffffffff
        |
          0+[0-3]?[0-7]{0,10}  # Octal notation, 0 - 037777777777
        |
          # Decimal notation, 1-4294967295:
          429496729[0-5]|42949672[0-8]\d|4294967[01]\d\d|429496[0-6]\d{3}|
          42949[0-5]\d{4}|4294[0-8]\d{5}|429[0-3]\d{6}|42[0-8]\d{7}|
          4[01]\d{8}|[1-3]\d{0,9}|[4-9]\d{0,8}
        )
        $
    """, re.VERBOSE | re.IGNORECASE)


def is_valid_ipv4(ip):
    """Validates IPv4 addresses.
    """
    return ___ipv4_pattern.match(ip) is not None


___ipv6_pattern = re.compile(r"""
        ^
        \s*                         # Leading whitespace
        (?!.*::.*::)                # Only a single whildcard allowed
        (?:(?!:)|:(?=:))            # Colon iff it would be part of a wildcard
        (?:                         # Repeat 6 times:
            [0-9a-f]{0,4}           #   A group of at most four hexadecimal digits
            (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
        ){6}                        #
        (?:                         # Either
            [0-9a-f]{0,4}           #   Another group
            (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
            [0-9a-f]{0,4}           #   Last group
            (?: (?<=::)             #   Colon iff preceeded by exacly one colon
             |  (?<!:)              #
             |  (?<=:) (?<!::) :    #
             )                      # OR
         |                          #   A v4 address with NO leading zeros 
            (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
            (?: \.
                (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
            ){3}
        )
        \s*                         # Trailing whitespace
        $
    """, re.VERBOSE | re.IGNORECASE | re.DOTALL)


def is_valid_ipv6(ip):
    """Validates IPv6 addresses.
    """
    #print("testing ({})".format(ip))
    return ___ipv6_pattern.match(ip) is not None


def hostname_from_fqdn(fqdn: str):
    return fqdn.split('.')[0]


__clocks_start = {}
__clocks_end = {}


def clock_start(name:str = 'default'):
    __clocks_start[name] = time.time()


def clock_stop(name:str = 'default'):
    __clocks_end[name] = time.time()


import functools


def clock_elapsed_str(name:str = 'default'):
    t = time.time()-__clocks_start[name]
    return "%d:%02d:%02d.%03d" % \
           functools.reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
                  [(t*1000,),1000,60,60])
    return "{}".format(time.time()-__clocks_start[name])


