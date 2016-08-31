#!/usr/bin/env python


"""Usage: nximport.py <datafile>
"""


import docopt
import odx
import yaml
import config


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

nximport.py

"""

IP_ADDRESS = config.IP_ADDRESS
PORT_NUMBER = config.PORT_NUMBER

DELIMITER_CHAR = config.DELIMITER_CHAR

PRODUCT_SCHEMA = {0: 'catalog_id', 1: 'name', 3:'timestamp'}
CATEGORY_SCHEMA = {2: 'name'}


def line_to_record(schema_dict, db_type, line, delimiter, field_indices):
    """
    @param schema_dict: dictionary mapping indices (keys) to properties in the
        graph schema.
    @param db_type: String of the name of the database object such as 
        NXProduct, NXCategory, NXBelongsTo, or NXSimilar
    @param line: String containing product data in the format described above
    @param delimiter: String that is used to delimit the data fields in each line
    @param field_indicies: [int] a list of integers with the indicies of the
        fields that are going to be used

    returns a record of type VertexRecord with fields outlined in the schema_dict

    This fucntion creates a VertexRecord object that can be added to the graph
    database later.
    """
    fields = line.split(delimiter)
    new_record = odx.VertexRecord(db_type)

    for i in field_indices:
        attr_name = schema_dict.get(i, '')
        if attr_name:
            new_record.add_attribute(attr_name, fields[i].strip())

    return new_record



def main(args):
    """
    This program reads the <datafile> line by line and calls line_to_record()
    on each line twice. Once to get the main product information and a second
    time to create category nodes. This imports both the product and the 
    category vertices.
    """
    initfile_name = args['<datafile>']
    
    print 'will import records from file %s...' % initfile_name
    

    nexus_db = odx.OrientDatabase(IP_ADDRESS, PORT_NUMBER)
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
            linenum += 1

            print 'generating JSON record: %s' % product_rec.json()
            print 'generating JSON record: %s' % category_rec.json()

            if linenum % 20 == 0:
                try:
                    pmgr.save_batch_txn(batch)
                    batch = []

                except Exception, err:
                    print '##### Error creating DB record: [ %s ]. \nPlease retry data file line # %d.' % (err.message, linenum)

        for name in list(category_names):
            catrec = odx.VertexRecord('NXCategory')
            catrec.add_attribute('name', name)
            batch.append(catrec)
        try:
            pmgr.save_batch_txn(batch)
        except Exception, err:
            print '##### Error creating DB record: [ %s ]. \nPlease retry data file line # %d.' % (err.message, linenum)


        nexus_db.close()
    



if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)



