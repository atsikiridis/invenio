import numpy as np

from scipy.sparse import lil_matrix, csr_matrix
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
    data = list()
    for sig in signatures:
        data.append({'name': sig[-2],
                     'bibrec': sig[3],
                     'affiliation': None,
                     'field_number': None})



def signature_similarity(all_signatures):
    n_signatures = len(all_signatures)
    similarity = lil_matrix((n_signatures, n_signatures), dtype=np.int8)
    counter = 0

    for n, (personid, signatures) in enumerate(groupby(all_signatures, lambda x: x[0])):
        if n % 1000 == 0: print n

        signatures = list(signatures)
        vector = np.zeros((len(signatures, )), dtype=np.complex)

        for i, s in enumerate(signatures):
            if s[-1] == 2:
                vector[i] = 1
            elif s[-1] == -2:
                vector[i] = 1j

        vector = csr_matrix(vector)

        block = vector.T * vector
        block[block == -1.] = 0
        block[block == 1j] = -1
        block = block.astype(np.int8)

        similarity[counter:counter + len(signatures), counter:counter + len(signatures)] = block
        counter += len(signatures)

    similarity.setdiag(1)

    return similarity

signatures = run_sql("""select personid, flag from aidPERSONIDPAPERS """)
no_of_sigs = len(signatures)
signatures = sorted(signatures)
matrix = signature_similarity(signatures)

print matrix.shape
print matrix.nnz