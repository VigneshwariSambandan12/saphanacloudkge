from rdflib import Graph, Literal, Namespace, RDF, RDFS, XSD 

def generate_ttl():
    # Define namespaces
    ns = Namespace("http://flight_database.org/sflight/")
    db = Namespace("http://flight_database.org/database/")
    
    g = Graph()
    g.bind("sflight", ns)
    g.bind("db", db)

    # Add tables
    sbook = ns.SBOOK
    scustom = ns.SCUSTOM

    # SBOOK table structure
    g.add((sbook, RDF.type, db.Table))
    g.add((sbook, RDFS.label, Literal("Flight Bookings")))
    g.add((sbook, db.tableName, Literal("SBOOK")))

    # SCUSTOM table structure
    g.add((scustom, RDF.type, db.Table))
    g.add((scustom, RDFS.label, Literal("Customer Details")))
    g.add((scustom, db.tableName, Literal("SCUSTOM")))

    # Add columns with metadata for SBOOK
    sbook_columns = {
        ns.MANDT: {
            "label": "Client",
            "isKey": True,
            "description": "Client identifier"
        },
        ns.CARRID: {
            "label": "Carrier ID",
            "isKey": True,
            "groupBy": True,
            "description": "Airline carrier identifier"
        },
        ns.CONNID: {
            "label": "Connection ID", 
            "isKey": True,
            "description": "Flight connection identifier"
        },
        ns.BOOKID: {
            "label": "Booking ID",
            "isKey": True,
            "aggregation": db.COUNT,
            "description": "Unique booking identifier"
        },
        ns.CUSTOMID: {
            "label": "Customer ID",
            "isKey": True,
            "foreignKey": ns.ID,  # Corrected reference
            "description": "Foreign key to SCUSTOM.ID"
        },
        ns.LOCCURAM: {
            "label": "Price",
            "aggregation": db.SUM,
            "description": "Booking price in local currency"
        },
        ns.CLASS: {
            "label": "Class",
            "description": "Travel class (Economy/Business)"
        },
        ns.ORDER_DATE: {
            "label": "Booking Date",
            "filter": "TO_INT(LEFT({column}, 4))",
            "aggregation": db.COUNT,
            "description": "Booking date (VARCHAR format)",
            "dataType": XSD.string
        }
    }

    for col, meta in sbook_columns.items():
        g.add((col, RDF.type, db.Column))
        g.add((col, RDFS.label, Literal(meta["label"])))
        g.add((col, db.columnName, Literal(col.split("/")[-1])))
        g.add((col, db.description, Literal(meta["description"])))
        
        if meta.get("isKey"):
            g.add((col, db.isPrimaryKey, Literal(True)))
        if meta.get("groupBy"):
            g.add((col, db.groupBy, Literal(True)))
        if meta.get("aggregation"):
            g.add((col, db.aggregationFunction, meta["aggregation"]))
        if meta.get("filter"):
            g.add((col, db.filterFunction, Literal(meta["filter"])))
        if meta.get("foreignKey"):
            g.add((col, db.foreignKey, meta["foreignKey"]))
        if meta.get("dataType"):
            g.add((col, db.dataType, meta["dataType"]))

    # Add SCUSTOM columns
    scustom_columns = {
        ns.ID: {
            "label": "Customer ID",
            "isKey": True,
            "description": "Primary key for customer"
        },
        ns.NAME: {
            "label": "Customer Name",
            "description": "Full name of customer"
        }
    }

    for col, meta in scustom_columns.items():
        g.add((col, RDF.type, db.Column))
        g.add((col, RDFS.label, Literal(meta["label"])))
        g.add((col, db.columnName, Literal(col.split("/")[-1])))
        g.add((col, db.description, Literal(meta["description"])))
        if meta.get("isKey"):
            g.add((col, db.isPrimaryKey, Literal(True)))

    # Define table relationships
    g.add((sbook, db.relatedTo, scustom))
    g.add((ns.CUSTOMID, db.foreignKey, ns.ID))  # Corrected relationship
    g.add((ns.CUSTOMID, db.joinCondition, 
           Literal("SBOOK.MANDT = SCUSTOM.MANDT AND SBOOK.CUSTOMID = SCUSTOM.ID")))

    # Save to file
    graph_string = g.serialize(format="turtle")

    with open('/content/drive/<your_file_name>.ttl', 'w') as file: # TODO add the path to your file
      file.write(graph_string)


#Create ontologies
generate_ttl()