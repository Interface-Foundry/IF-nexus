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
"""

IP_ADDRESS = config.IP_ADDRESS
PORT_NUMBER = config.PORT_NUMBER

DELIMITER_CHAR = config.DELIMITER_CHAR
SIMILARITIES_DELIMITER = config.SIMILARITIES_DELIMITER


PRODUCT_SCHEMA = {0: 'catalog_id', 1: 'name', 3: 'timestamp'}
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


def get_product_id(product_catalog_id, db_instance):
    """
    @param product_catalog_id: String containing the unique catalog ID for the
        particular product 
    @param db_instance: Instance of the database connection

    returns a list containing pyorient objects reponses from the database. Each
    item in the list corresponds to one product.

    This function maps the unique product catalog_id to the internal RID given
    by the database. The RID can be used to create edges.
    """

    query = "select * from NXProduct where catalog_id = '%s'" % product_catalog_id
    results = db_instance.execute_query(query)
    return results


def update_edge(rid_1, rid_2, db_instance):
    """
    @param rid_1: String containing the first RID
    @param rid_2: String containing the second RID
    @param db_instance: The instance of the database connection

    returns the results from either updating the NXSimilar edge between the 
    two items specified by rid_1 and rid_2.

    This function first checks to see if there is an edge betweent he RIDs. If
    there is an edge, then it increments the weight of the edge by 1.0. If 
    there is not an edge then it makes an edge with default weight 1.0 from
    NXProduct at rid_1 to the NXProduct at rid_2.

    This function also doubles as an edge maker.
    """
    query = "select * from NXSimilar where (in = '%s' OR in = '%s') AND (out = '%s' or out = '%s')" % (rid_1, rid_2, rid_1, rid_2)
    results = db_instance.execute_query(query)
    if results:
        edge_rid = results[0]._rid
        query = "update %s increment weight = 1" % edge_rid
        # query = "update increment weight = 1 where @rid = '%s'" % edge_rid
        results = db_instance.execute_query(query)
    else:
        query = "create edge NXSimilar from %s to %s set weight = 1.0" % (rid_1, rid_2)
        results = db_instance.execute_query(query)
    
    return results




def main(args):

    """
    The program will read from the <datafile> line by line and identify the 
    main product and the similar products associated with the main product.
    Then the program will locate the NXProduct vertices associated with the
    main product and the similar products and perform update_edge() for each
    similar product to the main product. 
    """

    initfile_name = args['<datafile>']
    
    print 'generating connections from file %s...' % initfile_name


    nexus_db = odx.OrientDatabase(IP_ADDRESS, PORT_NUMBER)
    nexus_db.connect('root', 'notobvious')
    nexus_db.open('nexus', 'root', 'notobvious')

    pmgr = odx.OrientDBPersistenceManager(nexus_db)
    
    batch = []
    sim_ids = []
    with open(initfile_name) as f:
        linenum = 1

        for line in f:
            product_rec = line_to_record(PRODUCT_SCHEMA, 'NXProduct', line, DELIMITER_CHAR, [0,1,3])
            
            similar_products = line.split(DELIMITER_CHAR)[5].strip().split(SIMILARITIES_DELIMITER)

            sim_ids = []
            for sim_id in similar_products:
                if sim_id:
                    similar_result = get_product_id(sim_id, nexus_db)
                    sim_ids.append(similar_result[0]._rid)
                    print 'Similar product %s has RID %s.' % (similar_result[0].name, similar_result[0]._rid)


            product_catalog_id = product_rec.attributes['catalog_id']
            product_result = get_product_id(product_catalog_id, nexus_db)
            if not product_result:
                raise Exception('No DB entry found for product: %s' % product_rec.json())

            
            product_id = product_result[0]._rid
            
            print 'Product %s has RID %s.' % (product_rec.attributes['name'], product_id)
            
            for sim_id in sim_ids:
                try:
                    update_edge(product_id, sim_id, nexus_db)

                except Exception, err:
                    print '##### Error creating DB record: %s. \nPlease retry.' % err.message         
            
        
            
               
        nexus_db.close()




if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)







