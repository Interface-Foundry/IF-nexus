#!/usr/bin/env python


"""Usage: nximport.py <datafile>
"""


import docopt
import odx
import yaml

"""
The format of the amazon string is:
asin | name | product group | timestamp | similar product name | similar product asin

In other words:
[0] : asin / catalog_id
[1] : title / product name
[2] : product group / highest level category
[3] : timestamp (when the item was scraped from the product API)
[4] : similarity product name
[5] : similarity product asin

The last two elements, "similar product name" and "similar product asin," are
delimited by a "~~~" by product. For example,

    Cheetah Cat and Ears Tail Set~~~Adult Ballet Tutu Pastel Rainbow

is a sample similarity product name field and 
    
    B00CF7G0UA~~~B00D0DJAEG

is a sample similarity product asin. It is important to note that the 
"similarity" feature provided by amazon reflects the frequency that items are 
bought together in the same cart, and that the API returns n similar items such
that n <= 5. There is also an accesories feature which may be useful to 
explore in the future.
"""


DELIMITER_CHAR = '|'
SIMILARITIES_DELIMITER = '~~~'

PRODUCT_SCHEMA = {0: 'name', 1: 'catalog_id', 3: 'timestamp'}
CATEGORY_SCHEMA = {2: 'name'}
SIMILARITIES_SCHEMA = {0: 'name', 1: 'catalog_id', 2: 'timestamp'}


def line_to_products(schema_dict, db_type, line, primary_delimiter, secondary_delimiter):
    fields = line.split(primary_delimiter)
    new_records = []

    timestamp = fields[3]
    similar_products_names = fields[4].split(secondary_delimiter)
    similar_products_asins = fields[5].split(secondary_delimiter)

    # below groups the items in the list in pairs
    # this is hard coded right now
    for name, asin in zip(similar_products_names, similar_products_asins):
        new_record = odx.VertexRecord(db_type)
        new_record.add_attribute(schema_dict[0], name)
        new_record.add_attribute(schema_dict[1], asin)
        new_record.add_attribute(schema_dict[2], timestamp)
        new_rec.add_attribute('catalog_source', 'amazon')

        new_records.append(new_record)

    return new_records


def get_product_id(product_catalog_id, db_instance):

    query = "select * from NXProduct where catalog_id = '%s'" % product_catalog_id
    results = db_instance.execute_query(query)
    return results



def main(args):
    initfile_name = args['<datafile>']
    
    print 'generating connections from file %s...' % initfile_name

    nexus_db = odx.OrientDatabase('14d1f37b.ngrok.io', 2480)
    nexus_db.connect('root', 'notobvious')
    nexus_db.open('nexus', 'root', 'notobvious')

    pmgr = odx.OrientDBPersistenceManager(nexus_db)
    
    
    with open(initfile_name) as f:
        batch = []
        catalog_ids = []

        for line in f:
            # line = f.readline()
            product_similar_recs = line_to_products(SIMILARITIES_SCHEMA, 'NXProduct', line, DELIMITER_CHAR, SIMILARITIES_DELIMITER)

            for similar_rec in product_similar_recs:
                product_result = get_product_id(similar_rec.attributes['catalog_id'], nexus_db)
                catalog_ids.append(similar_rec.attributes['catalog_id'])

                if not product_result:
                    similar_rec.add_attribute('catalog_source', 'amazon')
                    print 'generating JSON record: %s' % similar_rec.json()
                
                batch.append(similar_rec)
            
        try:
            pmgr.save_batch_txn(batch)
        except Exception, err:
            print '##### Error creating DB record: [ %s ]. \nPlease retry data file.' % err.message

        
               
        nexus_db.close()
    



if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)



