# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2011, 2012, 2014 CERN.
##
## Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""
Tests for Authors Models
"""
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound


class ConfiguredModelsSetup(object):

    @classmethod
    def prepare(cls):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.authors.models import Author, Publication
        cls._publications = [Publication() for _ in range(0, 3)]
        cls._authors = [Author() for _ in range(0, 3)]

        map(cls._publications[0].authors.append, cls._authors[0:2])
        map(cls._publications[1].authors.append, cls._authors[1:3])
        cls._publications[2].authors.append(cls._authors[0])
        cls._publications[0].references.append(cls._publications[1])

        map(db.session.add, cls._authors)
        map(db.session.add, cls._publications)
        db.session.commit()

    @classmethod
    def clean(cls):
        for author in cls._authors:
            author.delete()

        for publication in cls._publications:
            publication.delete()


class TestAuthorModels(InvenioTestCase, ConfiguredModelsSetup):
    """Base class for author model tests"""

    @classmethod
    def setUpClass(cls):
        cls.prepare()

    def test_publication_has_author(self):
        """Publications should have authors assigned automatically."""
        pub_authors = self._publications[0].authors

        self.assertEqual(set(pub_authors), {self._authors[0],
                                            self._authors[1]})

    def test_author_has_publication(self):
        """Authors should have publications assigned automatically."""
        author_pubs = self._authors[0].publications

        self.assertEqual(set(author_pubs),
                         {self._publications[0], self._publications[2]})

        author_pubs = self._authors[1].publications

        self.assertEqual(set(author_pubs),
                         {self._publications[0], self._publications[1]})

    def test_references_and_citations(self):

        self.assertEqual(self._publications[0].references,
                         [self._publications[1]])

        self.assertEqual(self._publications[1].citations,
                         [self._publications[0]])

    def test_coauthors(self):
        self.assertEqual(self._authors[0].coauthors,
                         (self._authors[1],))

    def test_author_citations(self):
        self.assertEqual(self._authors[2].citations,
                         (self._publications[0],))

    @classmethod
    def tearDownClass(cls):
        cls.clean()


class TestAuthorsModelsCleanup(InvenioTestCase):

    def setUp(self):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.authors.models import Author, Publication
        self._publications = [Publication() for _ in range(0, 3)]
        self._author = Author()

        map(self._author.publications.append, self._publications)

        db.session.add(self._author)
        map(db.session.add, self._publications)
        db.session.commit()

    def test_author_deletion(self):
        from invenio.modules.authors.models import Signature
        """When an author is deleted, the respective signatures should
           have NULL as author. The publications should be there"""
        sigs = Signature.query.filter(
            Signature.author == self._author).all()
        self._author.delete()

        self.assertEqual([None] * 3, [sig.author for sig in sigs])

    def test_publication_deletion(self):
        from invenio.modules.authors.models import Signature
        """When a publication is deleted the respective signatures should be
           deleted"""
        self._publications[0].delete()
        deleted_pub = self._publications[0]

        self.assertFalse(Signature.query.filter(
            Signature.publication == deleted_pub).all())

    def test_signature_deletion(self):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.authors.models import Signature
        pub1 = self._publications[0]
        sign1 = Signature.query.filter(Signature.publication == pub1).all()[0]
        db.session.delete(sign1)
        db.session.commit()

        author = self._author
        publication = self._publications[0]

        self.assertTrue(author)
        self.assertTrue(publication)
        self.assertEqual(set(author.publications),
                         set(self._publications[1:]))

    def tearDown(self):
        try:
            self._author.delete()
        except AttributeError:
            pass
        for publication in self._publications:
            try:
                publication.delete()
            except AttributeError:
                pass


class TestAuthorsModelsClaims(InvenioTestCase, ConfiguredModelsSetup):

    def setUp(self):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.accounts.models import User
        self.prepare()
        pub_2 = self._publications[1]
        self._user = User(password='test')
        self._authors[0].publications.append(pub_2)
        db.session.add(self._authors[0])
        db.session.add(self._user)
        db.session.commit()

    def _get_signature(self, author, publication):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.authors.models import Signature
        sig_query = Signature.query.filter(
            db.and_(Signature.author == author,
                    Signature.publication == publication))
        try:
            sig = sig_query.one()
        except NoResultFound:
            self.fail('No signature found for author: %s and publication %s' %
                      (author, publication))
        except MultipleResultsFound:
            self.fail("""Multiple signatures found for author: %s and
            publication %s. In the context of the test this is wrong.""" %
                      (author, publication))
        return sig

    def test_signature_claim(self):
        from invenio.modules.authors.models import Signature
        sig = self._get_signature(self._authors[0],
                                  self._publications[1])
        sig_id = sig.id
        sig.claim(self._user)
        self.assertEqual(sig.author, Signature.query.get(sig_id).author)
        self.assertEqual(self._user,
                         Signature.query.get(sig_id).curator)
        self.assertEqual(Signature.query.get(sig_id).attribution,
                         'verified')

    def test_signature_disclaim(self):
        from invenio.modules.authors.models import Signature
        sig = self._get_signature(self._authors[0],
                                  self._publications[1])
        sig_id = sig.id
        pubs_before = len(sig.author.publications)
        sig.disclaim(self._user)
        self.assertEqual(pubs_before, len(sig.author.publications) + 1)
        self.assertEqual(sig.author, Signature.query.get(sig_id).author)
        self.assertEqual(self._user,
                         Signature.query.get(sig_id).curator)
        self.assertEqual(Signature.query.get(sig_id).attribution,
                         'rejected')
        self.assertFalse(self._publications[1] 
                         in self._authors[0].publications)
        self.assertFalse(self._authors[0] in self._publications[1].authors)

    def test_signature_move(self):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.authors.models import Signature
        sig = self._get_signature(self._authors[0],
                                  self._publications[1])
        sig_len_before = db.session.query(Signature).count()
        sig.move(self._authors[1])
        sig_len_after = db.session.query(Signature).count()

        self.assertFalse(self._publications[1]
                         in self._authors[0].publications)

        self.assertTrue(self._publications[1]
                        in self._authors[1].publications)
            
        self.assertEqual(sig_len_before, sig_len_after)

    def test_signature_reassignment(self):
        from invenio.ext.sqlalchemy import db
        from invenio.modules.authors.models import Signature
        sig = self._get_signature(self._authors[0], self._publications[2])
        sig_len_before = db.session.query(Signature).count()
        Signature.reassign(self._authors[2], self._user, sig)
        sig_len_after = db.session.query(Signature).count()
        self.assertEqual(sig_len_before + 1, sig_len_after)
        self.assertEqual(sig.attribution, 'rejected')
        one_sig = Signature.query.filter_by(publication=self._publications[2],
                                            author=self._authors[2]).one()
        self.assertTrue(one_sig)

    def test_signature_reassignment_already_assigned(self):
        from invenio.modules.authors.models import Signature
        from invenio.modules.authors.errors import SignatureExistsError
        sig = self._get_signature(self._authors[0], self._publications[1])
        try:
            with self.assertRaises(SignatureExistsError):
                Signature.reassign(self._authors[0], self._user, sig)
        except AssertionError:
            self.fail("""SignatureExistsError was not raised, 
                when trying to associate a second signature to 
                the same pair of Author and Publications""")                  

    def tearDown(self):
        from invenio.ext.sqlalchemy import db
        db.session.delete(self._user)
        db.session.commit()
        self.clean()

TEST_SUITE = make_test_suite(TestAuthorModels, TestAuthorsModelsCleanup,
                             TestAuthorsModelsClaims)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
