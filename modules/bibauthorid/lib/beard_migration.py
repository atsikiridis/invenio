import numpy as np

from scipy.sparse import lil_matrix
from itertools import groupby
from invenio.dbquery import run_sql

"""
 TODO 1) Extract all signatures
             - agree on the feature extraction dict.
                 fields of dict = (name of author (as it appears on the paper
                 IDEALLY but last name, first name for now),
                                   affiliation(s),
                                   field number (order it appears),
                                   bibrec)


      2) Build an affinity matrix.
        for each pid
            for all pairs (i,j) of signatures, where both i,j are claimed
                similarity(i,j) = 1

            for all pairs (i,j) of signatures, where i is claimed and j is disclaimed
                similarity(i,j) = -1

            all the rest 0


      3) Export records to files and define getters ( bibrec -> string)
"""


def extract_data_from_signatures(signatures):
    data = dict()
    for sig in signatures:
        data['name'] = sig[-2]
        # affiliation
        # field number
        data['bibrec'] = sig[3]



def authors_affinity_matrix(signatures):
    affinity = lil_matrix((len(signatures), len(signatures)), dtype=np.int8)
    counter = 0
    for _, sigs in groupby(signatures, lambda x: x[0]):
        sigs = list(sigs)
        for i, s1 in enumerate(sigs):
            for j, s2 in enumerate(sigs[i+1:]):
                if s1[-1] == 2 and s2[-1] == 2:
                    affinity[counter+i, counter+j] = 1
                    affinity[counter+j, counter+i] = 1
                elif (s1[-1] == 2 and s2[-1] == -2) or\
                        (s1[-1] == -2 and s2[-1] == 2):
                    affinity[counter+i, counter+j] = -1
                    affinity[counter+j, counter+i] = -1

        for d in range(counter, counter + len(sigs)):
            affinity[d, d] = 1
        counter += len(sigs)
    return affinity.tocsr()

signatures = set(run_sql("""select personid, bibref_table, bibref_value,
                            bibrec, name, flag from aidPERSONIDPAPERS  """))
no_of_sigs = len(signatures)
signatures = sorted(signatures)
matrix = authors_affinity_matrix(signatures)

print matrix.shape
print matrix.nnz