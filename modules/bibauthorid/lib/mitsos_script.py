import csv
__author__ = 'artem'

from xml.etree.ElementTree import Element, SubElement, tostring
from collections import OrderedDict
from invenio.bibauthorid_name_utils import create_normalized_name
from invenio.bibauthorid_name_utils import split_name_parts
import re

def indent(elem, level=0):
  i = "\n" + level*"  "
  if len(elem):
    if not elem.text or not elem.text.strip():
      elem.text = i + "  "
    if not elem.tail or not elem.tail.strip():
      elem.tail = i
    for elem in elem:
      indent(elem, level+1)
    if not elem.tail or not elem.tail.strip():
      elem.tail = i
  else:
    if level and (not elem.tail or not elem.tail.strip()):
      elem.tail = i

def create_marc_xml(**kwargs):
    record = Element('record')

    def create_datafield(tag, subfields, ind1=' ', ind2=' '):
        datafield = SubElement(record, 'datafield', attrib={'tag': tag,
                                                            'ind1': ind1,
                                                            'ind2': ind2})
        #print subfields
        for code, sub_data in subfields.iteritems():
            subfield = SubElement(datafield, 'subfield', attrib={'code': code})
            subfield.text = sub_data

    #print kwargs
    for field_tag, field_subfields in kwargs.iteritems():

        #print(field_subfields)
        create_datafield(field_tag.split('_')[0], *field_subfields)

    indent(record)
    return record


def run(author_name, university, title, date, email, keyword1, keyword2):  #uni from indico
# example. T

   # remove parentheses from name
    marc = OrderedDict({'041': ({'a': 'eng'},),
                        '100': ({'a': author_name, 'u': university},),
                        '245': ({'a': title},),
                        '260': ({'c': '2014'},),
                        '269': ({'b': 'CERN', 'a': 'Geneva', 'c': date},),
                        '270': ({'m': email},),
                        '300': ({'a': '36'},),
                        '653_0': ({'a': 'ICTR PHE 2014', '9': 'CERN'}, '1'),
                        '653_1': ({'a': keyword1, '9': 'CERN'}, '1'),
                        '653_2': ({'a': keyword2, '9': 'CERN'}, '1'),
                        '650': ({'a': 'Talk'}, '1', '7'),
                        '690': ({'a': 'KTT-LSTALK'}, 'C'),
                        '859': ({'f': 'dimitrios.chlorokostas@cern.ch'}, '1', '7'),
                        '963': ({'a': 'PUBLIC'},),
                        '980': ({'a': 'KTT-LSCERNTALK'},),
                        })

    return create_marc_xml(**marc)


pattern_initials = '([A-Z]\\.)\\s([A-Z]\\.)'

if __name__ == '__main__':
    #print run('a', 'b', 'c', 'd', 'e', 'r', '6')
#title, author, affiliation, keywords, day, email
# {tag: (content, {'sub1':value, 'sub2': value})
    with open('master.csv', 'r', ) as f:
        lines_fields = csv.reader(f)
        for fields in lines_fields:
            name = fields[0].split(' (')[0]
            with open('indico.csv', 'r', ) as f2:
                reader = csv.reader(f2)
                email = None
                university = None
                for line in reader:
                    #print line, name.upper()
                    if name.upper().split()[1] in line[0]:
                        email = line[1]
                        university = line[2]
                if not email or not university:
                    print name
                #print name, university, fields[1], fields[2], email, fields[3], fields[4]
                #print fields
                name = create_normalized_name(split_name_parts(name))
        # remove spaces between initials
        # two iterations are required
                for _ in range(2):
                    name = re.sub(pattern_initials, r'\g<1>\g<2>', name)

                try:
                    a =  run(name.decode('utf8'), university.decode('utf8'), fields[1].decode('utf-8'), fields[2], email, fields[3],
                              fields[4])
                except AttributeError:
                    a =  run(name.decode('utf8'), university, fields[1].decode('utf-8'), fields[2], email, fields[3],
                              fields[4])
                #crossref_normalize_name(a)
                with open('to_push.xml', 'a') as final:
                     final.write(tostring(a) + '\n')


