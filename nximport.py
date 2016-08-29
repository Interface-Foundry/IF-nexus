#!/usr/bin/env python


"""Usage: nximport.py <datafile>
"""


import docopt
import odx
import yaml


DELIMITER_CHAR = '|'

PRODUCT_SCHEMA = {0: 'name', 1: 'catalog_id', 3:'timestamp'}
CATEGORY_SCHEMA = {2: 'name'}


def line_to_record(schema_dict, db_type, line, delimiter, field_indices):
    fields = line.split(delimiter)
    new_record = odx.VertexRecord(db_type)

    for i in field_indices:
        attr_name = schema_dict.get(i, '')
        if attr_name:
            new_record.add_attribute(attr_name, fields[i].strip())

    return new_record



def main(args):
    initfile_name = args['<datafile>']
    
    print 'will import records from file %s...' % initfile_name


    nexus_db = odx.OrientDatabase('localhost', 2424)
    nexus_db.connect('root', 'notobvious')
    nexus_db.open('nexus', 'root', 'notobvious')

    pmgr = odx.OrientDBPersistenceManager(nexus_db)
    

    with open(initfile_name) as f:
        linenum = 1

        batch = []
        category_names = set()
        for line in f:
            product_rec = line_to_record(PRODUCT_SCHEMA, 'NXProduct', line, DELIMITER_CHAR, [0,1,3])            
            product_rec.add_attribute('catalog_source', 'amazon')
            
            category_rec = line_to_record(CATEGORY_SCHEMA, 'NXCategory', line, DELIMITER_CHAR, [2])
            category_names.add(category_rec.attributes['name'])
                          
            batch.append(product_rec)

            print 'generating JSON record: %s' % product_rec.json()
            print 'generating JSON record: %s' % category_rec.json()

        try:
            for name in list(category_names):
                catrec = odx.VertexRecord('NXCategory')
                catrec.add_attribute('name', name)
                batch.append(catrec)
                
            pmgr.save_batch_txn(batch)

        except Exception, err:
            print '##### Error creating DB record: [ %s ]. \nPlease retry data file line # %d.' % (err.message, linenum)
        finally:
            linenum = linenum + 1

        nexus_db.close()
    



if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)



