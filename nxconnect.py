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


def get_category_id(category_name, db_instance):

    query = "select * from NXCategory where name = '%s'" % category_name
    results = db_instance.execute_query(query)
    return results


def get_product_id(product_record, db_instance):

    query = "select * from NXProduct where catalog_id = '%s'" % product_record.attributes['catalog_id']
    results = db_instance.execute_query(query)
    return results

    
    




def main(args):
    initfile_name = args['<datafile>']
    
    print 'generating connections from file %s...' % initfile_name


    nexus_db = odx.OrientDatabase('localhost', 2424)
    nexus_db.connect('root', 'notobvious')
    nexus_db.open('nexus', 'root', 'notobvious')

    pmgr = odx.OrientDBPersistenceManager(nexus_db)
    
    batch = []
    with open(initfile_name) as f:
        linenum = 1

        
        for line in f:
            product_rec = line_to_record(PRODUCT_SCHEMA, 'NXProduct', line, DELIMITER_CHAR, [0,1,3])            
            
            product_category = line.split(DELIMITER_CHAR)[2].strip()

            print 'Product %s is in Category %s.' % (product_rec.attributes['name'], product_category)

            category_result = get_category_id(product_category, nexus_db)
            if not category_result: 
                raise Exception('No DB entry found for category: %s' % product_category)
            
            cat_id = category_result[0]._rid
            print 'Product category %s has RID %s.' % (product_category, cat_id)

            product_result = get_product_id(product_rec, nexus_db)
            if not product_result:
                raise Exception('No DB entry found for product: %s' % product_rec.json())

            
            product_id = product_result[0]._rid
            
            print 'Product %s has RID %s.' % (product_rec.attributes['name'], product_id)

            new_edge = odx.EdgeRecord('NXBelongsTo', product_id, cat_id)
            new_edge.add_attribute('weight', 5.0)
            batch.append(new_edge)
        try:
            pmgr.save_batch_txn(batch)

        except Exception, err:
            print '##### Error creating DB record: %s. \nPlease retry.' % err.message 
       
        nexus_db.close()
    



if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)



