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


def line_to_record(schema_dict, db_type, line, delimiter, field_indices):
    fields = line.split(delimiter)
    new_record = odx.VertexRecord(db_type)

    for i in field_indices:
        attr_name = schema_dict.get(i, '')
        if attr_name:
            new_record.add_attribute(attr_name, fields[i].strip())

    return new_record


def get_product_id(product_catalog_id, db_instance):

    query = "select * from NXProduct where catalog_id = '%s'" % product_catalog_id
    results = db_instance.execute_query(query)
    return results


def main(args):
    initfile_name = args['<datafile>']
    
    print 'generating connections from file %s...' % initfile_name


    nexus_db = odx.OrientDatabase('14d1f37b.ngrok.io', 2424)
    nexus_db.connect('root', 'notobvious')
    nexus_db.open('nexus', 'root', 'notobvious')

    pmgr = odx.OrientDBPersistenceManager(nexus_db)
    
    batch = []
    with open(initfile_name) as f:
        linenum = 1

        for line in f:
            product_rec = line_to_record(PRODUCT_SCHEMA, 'NXProduct', line, DELIMITER_CHAR, [0,1,3])
            
            similar_products = line.split(DELIMITER_CHAR)[5].split(SIMILARITIES_DELIMITER).strip()

            print 'Product %s is in Category %s.' % (product_rec.attributes['name'], product_category)

            sim_ids = []
            for similar_product in similar_products:
                similar_result = get_product_id(similar_product.attributes['catalog_id'])
                sim_ids.append(similar_result[0]._rid)
                print 'Similar product %s has RIC %s.' % (similar_result[0]._rid)

            # category_result = get_category_id(product_category, nexus_db)
            # if not category_result: 
            #     raise Exception('No DB entry found for category: %s' % product_category)
            
            # cat_id = category_result[0]._rid
            # print 'Product category %s has RID %s.' % (product_category, cat_id)

            product_catalog_id = product_rec.attributes['catalog_id']
            product_result = get_product_id(product_catalog_id, nexus_db)
            if not product_result:
                raise Exception('No DB entry found for product: %s' % product_rec.json())

            
            product_id = product_result[0]._rid
            
            print 'Product %s has RID %s.' % (product_rec.attributes['name'], product_id)
            
            try:
                for sim_id in sim_ids:
                    query = "create edge NXSimilar from %s to %s set weight = 5.0" % (product_id, sim_id)
                    results = nexus_db.execute_query(query)
            except Exception, err:
                print '##### Error creating DB record: %s. \nPlease retry.' % err.message 
            
            
               
        nexus_db.close()




if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)







