import numpy as np
from joblib import Parallel, delayed


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
       signatures
       records
       matrix


      2) Build an affinity matrix.
        for each pid
            for all pairs (i,j) of signatures, where both i,j are claimed
                similarity(i,j) = 1

            for all pairs (i,j) of signatures, where i is claimed and j is disclaimed
                similarity(i,j) = -1

            all the rest 0


      3) Export records to files and define getters ( bibrec -> string)
"""


def get_affiliation(personid, bibrec, table):
    """Returns  insutitution name and field number. """
    table_name = str(table)[0:2] + 'x'
    q = run_sql("""SELECT f2.value, r.field_number
                   FROM aidPERSONIDPAPERS AS a
                   INNER JOIN bibrec AS b ON (a.bibrec = b.id)
                   INNER JOIN bibrec_bib%s AS r ON (b.id = r.id_bibrec)
                   INNER JOIN bib%s AS f ON (r.id_bibxxx = f.id)
                   INNER JOIN bibrec_bib%s AS r2 ON (b.id = r2.id_bibrec) AND (r.field_number = r2.field_number)
                   INNER JOIN bib%s AS f2 ON (r2.id_bibxxx = f2.id)
                   WHERE a.personid = %d AND
                         r.id_bibrec = %d AND
                         f.tag = '%s__a' AND
                         f.value = a.name AND
                         f2.tag = '%s__u'

                 """ % (table_name, table_name, table_name, table_name,
                        personid, bibrec, table, table))

    if len(q) > 0:
        return q
    else:
        return None

def get_position_in_marc(personid, bibrec, table):
    """Returns  field number. """
    table_name = str(table)[0:2] + 'x'
    q = run_sql("""SELECT r.field_number
                   FROM aidPERSONIDPAPERS AS a
                   INNER JOIN bibrec AS b ON (a.bibrec = b.id)
                   INNER JOIN bibrec_bib%s AS r ON (b.id = r.id_bibrec)
                   INNER JOIN bib%s AS f ON (r.id_bibxxx = f.id)
                   INNER JOIN bibrec_bib%s AS r2 ON (b.id = r2.id_bibrec) AND (r.field_number = r2.field_number)
                   INNER JOIN bib%s AS f2 ON (r2.id_bibxxx = f2.id)
                   WHERE a.personid = %d AND
                         r.id_bibrec = %d AND
                         f.tag = '%s__a' AND
                         f.value = a.name
                 """ % (table_name, table_name, table_name, table_name,
                        personid, bibrec, table))

    if len(q) > 0:
        return q
    else:
        return None


def parallel_signature_getter(i, signature):
    try:
        affiliation, position = get_affiliation(signature[0], signature[1],
                                                signature[2])[0]
    except TypeError:
        affiliation = None
        position = get_position_in_marc(signature[0], signature[1],
                                        signature[2])[0][0]
    return {'signature_id': i,
            'author_name': signature[3],
            'publication_id': signature[1],
            'author_affiliation': affiliation,
            'signature_position': position}


def extract_data_from_signatures(signatures):
    data = Parallel(n_jobs=-1, verbose=3)(
        delayed(parallel_signature_getter)(
            i, signature
        ) for i, signature in enumerate(signatures)
    )

    return data


def signature_similarity(all_signatures):
    n_signatures = len(all_signatures)
    similarity = lil_matrix((n_signatures, n_signatures), dtype=np.int8)
    counter = 0

    for n, (personid, signatures) in enumerate(groupby(all_signatures,
                                                       lambda x: x[0])):
        if n % 1000 == 0:
            print n

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

signatures = run_sql("""select personid, bibrec, bibref_table, name, flag from aidPERSONIDPAPERS""")
no_of_sigs = len(signatures)
signatures = sorted(signatures)
data = extract_data_from_signatures(signatures)
#matrix = signature_similarity(signatures)

#print matrix.shape
#print matrix.nnz