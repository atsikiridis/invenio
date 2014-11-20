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

"""Authors database models."""

from invenio.ext.sqlalchemy import db, utils
from invenio.modules.accounts.models import User
from invenio.modules.records.models import Record
from invenio.modules.records.api import get_record
from invenio.modules.authors.errors import SignatureExistsError

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound


Citation = db.Table('authors_citation', db.metadata,
                    db.Column('citer', db.Integer(15, unsigned=True),
                              db.ForeignKey('authors_publication.id')),
                    db.Column('cited', db.Integer(15, unsigned=True),
                              db.ForeignKey('authors_publication.id')))


class Publication(db.Model):

    """Represents a publication entity."""

    __tablename__ = 'authors_publication'
    __mapper_args__ = {'confirm_deleted_rows': False}

    _id_bibrec = db.Column(db.MediumInteger(8, unsigned=True),
                           db.ForeignKey(Record.id))

    id = db.Column(db.Integer(15, unsigned=True), primary_key=True,
                   autoincrement=True, nullable=False)
    authors = db.relationship('Author', secondary='authors_signature',
                              secondaryjoin="""and_(Signature.author == Author,
                                                    Signature.attribution.in_(
                                                        ('unknown',
                                                         'verified')))""")
    references = db.relationship('Publication', secondary='Citation',
                                 primaryjoin=Citation.c.cited == id,
                                 secondaryjoin=Citation.c.citer == id,
                                 backref='citations')

    def __repr__(self):
        return 'Publication(id=%d)' % self.id

    @utils.session_manager
    def delete(self):
        """TODO."""
        sigs = Signature.query.filter(Signature.publication == self).all()
        for s in sigs:
            db.session.delete(s)

        db.session.delete(self)

    @property
    def record(self):
        """TODO."""
        return get_record(self._id_bibrec)


class Author(db.Model):

    """Represents an author entity."""

    __tablename__ = 'authors_author'
    __mapper_args__ = {'confirm_deleted_rows': False}

    _id_user = db.Column(db.Integer(15, unsigned=True), db.ForeignKey(User.id))
    _id_bibrec = db.Column(db.MediumInteger(8, unsigned=True),
                           db.ForeignKey(Record.id))

    id = db.Column(db.Integer(15, unsigned=True), primary_key=True,
                   autoincrement=True, nullable=False)
    user = db.relationship('User')
    publications = db.relationship('Publication',
                                   secondary='authors_signature',
                                   secondaryjoin="""and_(
                                       Signature.publication == Publication,
                                       Signature.attribution.in_(
                                           ('unknown', 'verified')))""")

    def __repr__(self):
        return 'Author(id=%d)' % self.id

    @property
    def record(self):
        """TODO."""
        return get_record(self._id_bibrec)

    @property
    def coauthors(self):
        """TODO."""
        coauthors = set()
        for publication in self.publications:
            for author in publication.authors:
                if author != self:
                    coauthors.add(author)

        return tuple(coauthors)

    @property
    def citations(self):
        """TODO."""
        citations = set()
        map(citations.update, [pub.citations for pub in self.publications])
        return tuple(citations)

    @utils.session_manager
    def delete(self):
        """TODO."""
        for s in Signature.query.filter(Signature.author == self).all():
            s.author = None
            db.session.add(s)

        db.session.delete(self)


class Signature(db.Model):

    """Represents a signature on a publication."""

    __mapper_args__ = {'confirm_deleted_rows': False}
    __tablename__ = 'authors_signature'

    _publication_id = db.Column(db.Integer(15, unsigned=True),
                                db.ForeignKey(Publication.id), nullable=False)
    _id_author = db.Column(db.Integer(15, unsigned=True),
                           db.ForeignKey(Author.id, ondelete="SET NULL"))
    _id_curator = db.Column(db.Integer(15, unsigned=True),
                            db.ForeignKey(User.id))

    id = db.Column(db.Integer(15, unsigned=True), primary_key=True,
                   autoincrement=True, nullable=False)
    json = db.Column(db.JSON, default=None)
    attribution = db.Column(db.Enum('unknown', 'rejected', 'verified'),
                            default='unknown')
    publication = db.relationship('Publication', backref='signatures')
    author = db.relationship('Author', backref='signatures')
    curator = db.relationship('User')

    def __repr__(self):
        return 'Signature(id=%d)' % self.id

    @utils.session_manager
    def claim(self, user):
        """TODO."""
        self.curator = user
        self.attribution = 'verified'
        db.session.add(self)

    @utils.session_manager
    def disclaim(self, user):
        """TODO."""
        self.curator = user
        self.attribution = 'rejected'
        db.session.add(self)

    @utils.session_manager
    def move(self, author, user=None):
        """TODO."""
        self.curator = user
        self.author = author
        db.session.add(self)

    @staticmethod
    @utils.session_manager
    def reassign(author, user, signature):
        """TODO."""
        try:
            Signature.query.filter_by(publication=signature.publication,
                                      author=author).one()
            raise SignatureExistsError("""Cannot reassign signature %s to
            author %s for publication %s. The author already has a signature
            for this publication.""" % (signature,
                                        author,
                                        signature.publication))
        except NoResultFound:
            signature.disclaim(user)
            new_signature = Signature(publication=signature.publication,
                                      author=author)
            db.session.add(new_signature)
        except MultipleResultsFound, e:
            # This should not happen.
            raise e


__all__ = ['Publication', 'Author', 'Signature', 'Citation']
