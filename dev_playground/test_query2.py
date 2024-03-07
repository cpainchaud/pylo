import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import illumio_pylo as pylo

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')

args = vars(parser.parse_args())

hostname = args['pce']

org = pylo.Organization(1)

print("Loading Origin PCE configuration from " + hostname + " or cached file... ", end="", flush=True)
org.load_from_cache_or_saved_credentials(hostname)
print("OK!\n")

# pylo.log_set_debug()

q = pylo.Query()

failed_queries = {}


queries = [ "name matches test",  # most simple example
            "(name matches test)"  # like previous example but with parenthesis,
            "description contains tic and name matches toc",  # multiple filters
            'name matches test and (name matches hello or name matches toc) or name matches "hello there"',
            "name matches '(i am a regex)'",  # some filters argument can be quoted because they contains spaces
            "name matches '(i am a regex and need to escape this quote \'here\')'",  # sometimes you need to escape forbidden chars
            "(description contains this and (name matches that or name matches 'something else') ) or name matches 1",  # nested-queries
            ]

for query in queries:
    print("\n\nNow parsing query: ''' {} ''' ...".format(query), end='', flush=True)
    q = pylo.Query()

    try:
        q.parse(query)
        print("OK!", flush=True)
    except pylo.PyloEx as e:
        failed_queries[query] = e
        print("FAILED! {}".format(e))
    print()


print("\nTOTAL NUMBER OF FAILED QUERIES: {}".format(len(failed_queries)))

if len(failed_queries) > 0:
    raise Exception("Failed one or more tests")


current_query = 'name matches test'
print()
print("* Now testing query '{}' against PCEs workload:".format(current_query))
query = pylo.Query()
q.parse(current_query)

# for wkl in org.WorkloadStore.itemsByName.values():
#     print(" - wkl '{}': ".format(wkl.get_name()), end='', flush=True)
#     print(query.execute_on_single_object(wkl))
#
# print("** DONE")



print("\nEND OF SCRIPT\n")

