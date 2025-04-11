# Scenario6: In this scenario, we are planning to build KGs for Tabular Data.
# We are using SFLIGHT Schema tables(SBOOK, SCUSTOM) and build an Ontology. This will basically provide 
# semantic relation between these tables, join conditions, columns used etc. This information will help 
# us build KGs by importing the Ontology to SAP HANA CLoud. We could either import using local file or using Cloud Storages
#Required packages: Make Sure you install packages mentioned below. And the code is tested end to end using colab
#Here are the necessary Packages
#!pip install rdflib
# Import necessary libraries and modules
# Import required RDFLib components
from rdflib import Graph, Literal, Namespace, RDF, RDFS, XSD

# Main function to generate Turtle (TTL) file
def generate_ttl():
    # Define custom namespaces for our RDF graph
    ns = Namespace("http://flight_database.org/sflight/")  # Namespace for flight data
    db = Namespace("http://flight_database.org/database/")  # Namespace for database schema
    
    # Create an empty RDF graph
    g = Graph()
    
    # Bind namespace prefixes for cleaner serialization
    g.bind("sflight", ns)  # Associates "sflight" prefix with our namespace
    g.bind("db", db)       # Associates "db" prefix with database namespace

    # Define table resources
    sbook = ns.SBOOK      # Resource for bookings table
    scustom = ns.SCUSTOM  # Resource for customers table

    # Add metadata for SBOOK table
    g.add((sbook, RDF.type, db.Table))            # Set type as Table
    g.add((sbook, RDFS.label, Literal("Flight Bookings")))  # Human-readable label
    g.add((sbook, db.tableName, Literal("SBOOK"))) # Actual table name in database

    # Add metadata for SCUSTOM table
    g.add((scustom, RDF.type, db.Table))            # Set type as Table
    g.add((scustom, RDFS.label, Literal("Customer Details")))  # Human-readable label
    g.add((scustom, db.tableName, Literal("SCUSTOM"))) # Actual table name in database

    # Define columns and metadata for SBOOK table
    sbook_columns = {
        # Client column metadata
        ns.MANDT: {
            "label": "Client",
            "isKey": True,  # Mark as primary key
            "description": "Client identifier"
        },
        # Carrier ID column metadata
        ns.CARRID: {
            "label": "Carrier ID",
            "isKey": True,  # Part of composite key
            "groupBy": True,  # Can be used for grouping
            "description": "Airline carrier identifier"
        },
        # Connection ID column metadata
        ns.CONNID: {
            "label": "Connection ID", 
            "isKey": True,  # Part of composite key
            "description": "Flight connection identifier"
        },
        # Booking ID column metadata
        ns.BOOKID: {
            "label": "Booking ID",
            "isKey": True,  # Part of composite key
            "aggregation": db.COUNT,  # Can be used with COUNT function
            "description": "Unique booking identifier"
        },
        # Customer ID column metadata (foreign key)
        ns.CUSTOMID: {
            "label": "Customer ID",
            "isKey": True,  # Part of composite key
            "foreignKey": ns.ID,  # References SCUSTOM.ID
            "description": "Foreign key to SCUSTOM.ID"
        },
        # Price column metadata
        ns.LOCCURAM: {
            "label": "Price",
            "aggregation": db.SUM,  # Can be used with SUM function
            "description": "Booking price in local currency"
        },
        # Class column metadata
        ns.CLASS: {
            "label": "Class",
            "description": "Travel class (Economy/Business)"
        },
        # Order date column metadata
        ns.ORDER_DATE: {
            "label": "Booking Date",
            "filter": "TO_INT(LEFT({column}, 4))",  # Example filter transformation
            "aggregation": db.COUNT,  # Can be used with COUNT function
            "description": "Booking date (VARCHAR format)",
            "dataType": XSD.string  # Explicit data type
        }
    }

    # Add all SBOOK columns to the graph
    for col, meta in sbook_columns.items():
        # Basic column metadata
        g.add((col, RDF.type, db.Column))  # Set type as Column
        g.add((col, RDFS.label, Literal(meta["label"])))  # Human-readable label
        g.add((col, db.columnName, Literal(col.split("/")[-1])))  # Extract column name from URI
        g.add((col, db.description, Literal(meta["description"])))  # Description
        
        # Conditional metadata additions
        if meta.get("isKey"):
            g.add((col, db.isPrimaryKey, Literal(True)))  # Mark as primary key
        if meta.get("groupBy"):
            g.add((col, db.groupBy, Literal(True)))  # Mark as groupable
        if meta.get("aggregation"):
            g.add((col, db.aggregationFunction, meta["aggregation"]))  # Add aggregation function
        if meta.get("filter"):
            g.add((col, db.filterFunction, Literal(meta["filter"])))  # Add filter function
        if meta.get("foreignKey"):
            g.add((col, db.foreignKey, meta["foreignKey"]))  # Add foreign key reference
        if meta.get("dataType"):
            g.add((col, db.dataType, meta["dataType"]))  # Add explicit data type

    # Define columns and metadata for SCUSTOM table
    scustom_columns = {
        # Customer ID column metadata
        ns.ID: {
            "label": "Customer ID",
            "isKey": True,  # Primary key
            "description": "Primary key for customer"
        },
        # Customer name column metadata
        ns.NAME: {
            "label": "Customer Name",
            "description": "Full name of customer"
        }
    }

    # Add all SCUSTOM columns to the graph
    for col, meta in scustom_columns.items():
        # Basic column metadata
        g.add((col, RDF.type, db.Column))  # Set type as Column
        g.add((col, RDFS.label, Literal(meta["label"])))  # Human-readable label
        g.add((col, db.columnName, Literal(col.split("/")[-1])))  # Extract column name from URI
        g.add((col, db.description, Literal(meta["description"])))  # Description
        
        # Conditional metadata additions
        if meta.get("isKey"):
            g.add((col, db.isPrimaryKey, Literal(True)))  # Mark as primary key

    # Define relationships between tables
    g.add((sbook, db.relatedTo, scustom))  # General relationship between tables
    
    # Explicit foreign key relationship
    g.add((ns.CUSTOMID, db.foreignKey, ns.ID))  # SBOOK.CUSTOMID → SCUSTOM.ID
    
    # Join condition for the relationship
    g.add((ns.CUSTOMID, db.joinCondition, 
           Literal("SBOOK.MANDT = SCUSTOM.MANDT AND SBOOK.CUSTOMID = SCUSTOM.ID")))

    # Serialize the graph to Turtle format
    graph_string = g.serialize(format="turtle")

    # Write the Turtle string to a file
    with open('/content/sflight_tabular.ttl', 'w') as file:
        file.write(graph_string)
        

# Execute the function to generate the TTL file
generate_ttl()

#Set up HANA Cloud Connection to import the ttl file 
from hdbcli import dbapi
# Establish connection to SAP HANA Cloud database
conn = dbapi.connect(
    user = "<Your HANA Cloud User>",
    password="<Your HANA Cloud Password>",
    address = '<Your HANA Cloud Host>',
    port = 443,
)
cursor = conn.cursor()
#import the ttl file in to SAP HANA Cloud 
ttl_filename = "/content/sflight_tabular.ttl"
graphname = 'sflight_graph'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
try:
    with open(ttl_filename, 'r') as ttlfp:
        request_hdrs = ''
        request_hdrs += 'rqx-load-protocol: true' + '\r\n'            # required header for upload protocol
        request_hdrs += 'rqx-load-filename: ' + ttl_filename + '\r\n' # optional header
        request_hdrs += 'rqx-load-graphname: ' + graphname + '\r\n'   # optional header to specify name of the graph, if not provided RDF data will be loaded to internal-default-graph
        conn.cursor().callproc('SPARQL_EXECUTE', (ttlfp.read(), request_hdrs, '', None))
    
    print("Success! The RDF graph has been successfully ingested into SAP HANA Cloud as graph:", graphname)
    
except Exception as e:
    print("Error occurred while ingesting the graph:", str(e))
    
finally:
    # Close the database connection
    if conn is not None:
        conn.close()
        print("Database connection closed.")

#Another option to Upload the TTL files is to move ttl file to Cloud Storage and then INgest it from SAP HANA CLoud Db Explorer
#Here is the python code to ingest the ttl file to Amazon S3
"""
import boto3
from botocore.exceptions import NoCredentialsError

# AWS credentials
AWS_ACCESS_KEY = '<Your Access Key>'
AWS_SECRET_KEY = '<Your Secret Key>'
AWS_BUCKET_NAME = '<Your AWS Bucket name>'
AWS_REGION = 'us-east-1'
# File details

tt = 'Insert the path of the ttl file from your local system'
s3_key =  'fedml.ttl' # S3 key (path) where the file will be stored

# Initialize a session using Amazon S3
s3 = boto3.client('s3', 
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY,
                  region_name=AWS_REGION)
try:
    s3.upload_file(tt, AWS_BUCKET_NAME, s3_key)
    print(f"File {tt} uploaded to {AWS_BUCKET_NAME}/{s3_key}")
except FileNotFoundError:
    print("The file was not found")
except NoCredentialsError:
    print("Credentials not available")
    
    
 #Step 2 : To INgest the ttl file from Cloud storage to SAP HANA Cloud 
 IMPORT FROM RDF FILE 's3-region://xxxxkey:xxxkey@bucket/dir/graph1.ttl' INTO SPARQL_GRAPH 'http://sap.com/public/graph1'
    
    """
