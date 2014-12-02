import numpy as np

import argparse
import cPickle

from joblib import Parallel, delayed
from scipy.sparse import lil_matrix, csr_matrix
from itertools import groupby

from invenio.dbquery import run_sql
from invenio.bibauthorid_dbinterface import get_title_of_paper
from invenio.bibrank_citation_searcher import get_refers_to

"""
 TODO 1) Extract all signatures


             Add a prefix to the script

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
         fields -- title, list of authors, institutions, references
"""


def get_record_metadata(record_id):
    authors = run_sql(""" select  f.value, f2.value FROM bibrec_bib10x as r

                          INNER JOIN bib10x as f ON r.id_bibxxx = f.id
                                 and f.tag = '100__a'
                          INNER JOIN bibrec_bib10x AS r2 ON (r.id_bibrec = r2.id_bibrec)
                          LEFT JOIN bib10x AS f2 ON (r2.id_bibxxx = f2.id)
                              AND f2.tag = '100__u'
                          WHERE r.id_bibrec =%s

                          UNION

                          select f.value, f2.value from bibrec_bib70x as r
                          INNER JOIN bib70x as f on r.id_bibxxx = f.id
                               and f.tag = '700__a'
                          INNER JOIN bibrec_bib70x AS r2 ON (r.id_bibrec = r2.id_bibrec)
                          LEFT JOIN bib70x AS f2 ON (r2.id_bibxxx = f2.id)
                              AND f2.tag = '700__u'
                          WHERE r.id_bibrec =%s


                          """ % (record_id, record_id))

    return {'title': get_title_of_paper(record_id),
            'authors': authors,
            'references': get_refers_to(record_id)}


def get_affiliation(personid, table):
    """Returns  insutitution name and field number. """
    table_name = str(table)[0:2] + 'x'
    q = run_sql("""SELECT f2.value, r2.field_number
                   FROM aidPERSONIDPAPERS as a
                   INNER JOIN bibrec AS b ON (a.bibrec = b.id)
                   INNER JOIN bibrec_bib%s AS r ON (b.id = r.id_bibrec)
                   INNER JOIN bib%s AS f ON (r.id_bibxxx = f.id)
                   INNER JOIN bibrec_bib%s AS r2 ON (b.id = r2.id_bibrec)
                       AND (r.field_number = r2.field_number)
                   INNER JOIN bib%s AS f2 ON (r2.id_bibxxx = f2.id)
                   WHERE a.personid = %d AND
                         f.tag = '%s__a' AND
                         f.value = a.name AND
                         f2.tag = '%s__u'
                 """ % (table_name, table_name, table_name, table_name,
                        personid, table, table))
    if len(q) > 0:
        return q
    else:
        return None


def get_position_in_marc(table, bibref, bibrec ):
    """Returns field number. """
    table_name = str(table)[0:2] + 'x'
    q = run_sql("""SELECT field_number
                   FROM bib%s, bibrec_bib%s
                   WHERE bib%s.id = bibrec_bib%s.id_bibxxx AND
                   bib%s.id = %s AND bibrec_bib%s.id_bibrec = %s
                 """ % (table_name, table_name, table_name, table_name,
                        table_name, bibref, table_name, bibrec))

    if len(q) > 0:
        return q
    else:
        return None


def parallel_signature_getter(i, signature):
    try:
        affiliation, position = get_affiliation(signature[0], signature[1])[0]
    except TypeError:
        affiliation = None
        position = get_position_in_marc(signature[1], signature[2],
                                        signature[3])[0][0]
    return {'signature_id': i,
            'author_name': signature[4],
            'publication_id': signature[3],
            'author_affiliation': affiliation,
            'signature_position': position}


def extract_data_from_signatures(signatures):
    data = Parallel(n_jobs=-1, verbose=3)(
        delayed(parallel_signature_getter)(
            i, signature
        ) for i, signature in enumerate(signatures)
    )

    return data


def populate_signature_similarity(positive_signatures):
    n_signatures = len(positive_signatures)
    similarity = lil_matrix((n_signatures, n_signatures), dtype=np.int8)
    counter = 0

    for n, (personid, signatures) in enumerate(groupby(positive_signatures,
                                                       lambda x: x[0])):
        if n % 1000 == 0:
            print n

        signatures = list(signatures)
        vector = np.zeros((len(signatures, )), dtype=np.int8)

        for i, s in enumerate(signatures):
            if s[-1] == 2:
                vector[i] = 1

        vector = csr_matrix(vector)

        block = vector.T * vector
        block = block.astype(np.int8)

        similarity[counter:counter + len(signatures),
            counter:counter + len(signatures)] = block

        counter += len(signatures)

    similarity.setdiag(1)

    return similarity


def add_rejection_claims(matrix, rejected_signatures, signature_id_mapping,
                         personid_signature_mapping):
    for rejected_sig in rejected_signatures:
        if rejected_sig[1:4] not in signature_id_mapping:
            continue
        id = signature_id_mapping[rejected_sig[1:4]]
        if rejected_sig[0] not in personid_signature_mapping:
            continue
        claimed_sigs_of_person = personid_signature_mapping[rejected_sig[0]]
        for sig in claimed_sigs_of_person:
            matrix[id, signature_id_mapping[sig[1:4]]] = -1
            matrix[signature_id_mapping[sig[1:4]], id] = -1

    return matrix


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute similarity matrix.')
    parser.add_argument('prefix', type=str,
                        help='gets all names starting with a prefix')
    args = parser.parse_args()

    all_signatures = set(run_sql("""select personid, bibref_table,
                                    bibref_value, bibrec, name, flag
                                    from aidPERSONIDPAPERS where name
                                    like %s limit 100 """, (args.prefix + '%', )))

    positive_signatures = filter(lambda x: x[-1] >= 0, all_signatures)
    positive_signatures = sorted(positive_signatures)
    step = 100000
    filename = "signatures-%010d"

    print 'Step 1: Extracting data from signatures...'
    for start in range(0, len(positive_signatures), step):
        data = extract_data_from_signatures(
            positive_signatures[start:min(start+step,
                                          len(positive_signatures))])
        cPickle.dump(data, open(filename % start, "w"))
    print 'Done!'

    print 'Step 2: Populating similarity matrix...'
    matrix = populate_signature_similarity(positive_signatures)
    signature_id_mapping = {sig[1:4]: i
                            for i, sig in enumerate(positive_signatures)}
    personid_signature_mapping = {pid: list(sigs) for pid, sigs in
                                  groupby(filter(lambda x: x[-1] == 2,
                                                 positive_signatures),
                                          lambda x: x[0])}
    matrix = add_rejection_claims(matrix,
                                  all_signatures - set(positive_signatures),
                                  signature_id_mapping,
                                  personid_signature_mapping)

    print 'Shape of matrix is %s.' % str(matrix.shape)
    print 'The number of non zero values is %s' % matrix.nnz
    print 'Done!'

    print 'Step 3: Extracting record metadata.'
    metadata = dict()
    for i, rec_id in enumerate(set(sig[3] for sig in positive_signatures)):
        if i % 1000 == 0:
            print i
        metadata[rec_id] = get_record_metadata(rec_id)
    cPickle.dump(metadata, open('record_metadata', 'w'))