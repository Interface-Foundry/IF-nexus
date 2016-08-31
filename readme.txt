
===============================================================================
AMAZON DATA STRUCTURE
===============================================================================

This program attempts to solve the bundling problem using a graphical approach.
It uses OrientDB to model the products into a graph. The Amazon data is stored
in a .txt file. The basis of the graph is constructed using ~9.6 million
products from the Amazon review database. Although the basis is set using data
from Amazon, the database is able to take in data from other sources as well
provided that they have a product name, unique ID, and a timestamp.


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

===============================================================================
BASIC PROGRAM OVERVIEW
===============================================================================

The program is split into 4 different steps:

    1.) `nximport.py` imports the product information and basic categories
    2.) `nxconnect.py` connects the products to their root category, such as
        "Book" or "Sports"
    3.) `nxsimilar.py` imports similar products that are not already in the
        database.
    4.) `nxconnect_similar.py` connects the similar products together

Each of the python scripts requires an additional arugment in the command for
the <datafile> which contains information in the form described above in the
AMAZON DATA STRUCTURE section, although the delimiters are can be changed in
the config.py file. These steps will only be run ONCE for the initialization.
Adding additional elements should be done through a different service. I have 
provided database queries for upserting additional elements further down in the
readme.

TODO: move away from config.py and into a better config.

===============================================================================
DATABASE SCHEMAS
===============================================================================

The database uses the following schemas for vertices and edges:

    vertex NXProduct = {
        'name': String,
        'catalog_id': String,
        'catalog_source': String,
        'timestamp': Datetime
    }

The "catalog_id" is a unique ID with respect to the source, e.g. the ASIN for
Amazon. The "name" is the name of the item. The "catalog_source" describes 
where the item was found (e.g. Amazon, Nostram, etc.). The timestamp descibes 
when the product was scraped or pulled.

    vertex NXCatagory = {
        'name': String
    }

The category only has a name. The name is a unique index, so categories wont be
repeated.

    edge NXBelongsTo = {
        'weight': Float
    }

The weight along the BelongsTo edge describes the association with a particular
category. The NXBelongsTo edge is always a downstream edge that points from
product to category:
    
                 NXBelongsTo
    NXProduct ================> NXCategory

This indicates that the NXProduct belongs to a particular NXCategory. Each
weight is a float between 0 and 10.

    edge NXSimilar = {
        'weight': Float
    }

The NXSimilar edge connects two different NXProducts together. I have not been
able to find a way to create undirected edges. The weight of the NXSimilar edge
describes the "closeness" of the two products, or rather, the frequency that
the two products are bought together. The more the products are bought together
the higher the weight value along the edge between the two items.

===============================================================================
ORIENTDB QUERIES
===============================================================================

When constantly adding new products, instead performing a line_to_record(),
perform an upsert using the following query to add a new Product to the graph
database:
    
    UPDATE NXProduct SET name = {name}, catalog_id = {catalog_id}, source = {source} timestamp = {timestamp} UPSERT WHERE catalog_id = {catalog_id} AND source = {source}

The above query resets all the product fields and updates the timestamp. The
inclusion of all the product fields is for the convenience of the UPSERT. This
query should create a new vertex with the fields specified after the SET if
it does not already exist. We scan catalog_id and source together since
catalog_id's are unique only with respect to the source. 

NOTE: The current code works only if all the catalog_id's are unique. It does
consider that catalog_id's are only unique with respect to the source.

Upserting edges can be handled by the update_edge() function included in 
nxconnect_similar.py:


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
        query = "update %s increment weight = 1.0" % edge_rid
        results = db_instance.execute_query(query)
    else:
        query = "create edge NXSimilar from %s to %s set weight = 1.0" % (rid_1, rid_2)
        results = db_instance.execute_query(query)
    
    return results


This function uses pyorient and odx. In the future, cart data would be
processed through the update_edge() function and the execution of the vertex
UPSERT. This function requires multiple queries, first the query to check if
there is a edge and another query to either create a new edge or to update an
existing edge.

Upserting vertices can be written in python using the following funtion:


def upsert_vertex(product_rec, db_instance):
    """
    @param product_rec: A VertexRecord object from the odx module for NXProduct
    @param db_instance: The instance of the database connection

    returns the results of newly upserted vertex.

    This function upserts a vertex.
    """
    query = "UPDATE NXProduct SET name = '{name}', catalog_id = '{catalog_id}'," \
            "source = '{source}' timestamp = '{timestamp}' UPSERT WHERE " \
            "catalog_id = '{catalog_id}' AND source = '{source}'".format(
            name=product_rec.attributes('name'), 
            catalog_id=product_rec.attributes('catalog_id'),
            source=product_rec.attributes('source'),
            timestamp=product_rec.attributes('timestamp'))
    
    results = db_instance.execute_query(query)
    return results


This function requires the odx and pyorient modules.

===============================================================================
BUNDLE
===============================================================================

For a specific product, the product's bundle (of size N) is given by the N
products with the largest weighted edges. To sort the products, execute the 
following query:

    SELECT * FROM NXSimilar WHERE in = {product_rid} OR out = {product_rid} ORDER BY weight DESC

OrientDB will return a list of m pyorient objects where m is the degree or the
number of edges the product has. The first N - 1 elements will form a bundle of 
size N, including the initial product.

TODO: Cart-wise bundles are not supported.
