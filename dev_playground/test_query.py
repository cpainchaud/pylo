
import illumio_pylo as pylo
import sys
import argparse

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


queries = [ 'A or B and D', 'D and (A or B)', ' D and(A orB) ',
            'D and (A and (E or R) ) ',
            'D and (A and (E or R))',
            'D or E (not G and H) and (A and (E or R))',
            '(D or O and (NOT H)) or E (not G and H) and (A and (E or R))',

            ]

for query in queries:
    print("Now parsing query: {}".format(query))
    q = pylo.Query()
    q.parse(query)
    print()



print("\nEND OF SCRIPT\n")

