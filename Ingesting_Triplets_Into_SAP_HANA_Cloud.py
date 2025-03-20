
from hdbcli import dbapi

conn = dbapi.connect(
    user = "<username>",
    password = "<password>",
    address = "<instance id>", 
    port = 000,
)


def ingest_triplets_into_hana_cloud():

    # Call stored procedure to execute SPARQL query or insert RDF triples
    cursor = conn.cursor()
    try:
        # Iterate through the triples and insert them into the database
        for s, p, o in rdf_graph:
            # Construct a SPARQL INSERT query for each triple
            sparql_insert_query = f"""
                INSERT DATA {{
                    <{s}> <{p}> <{o}> .
                }}
            """

            # Execute the SPARQL query using the stored procedure
            resp = cursor.callproc('SPARQL_EXECUTE', (sparql_insert_query, 'Metadata headers describing Input and/or Output', '?', None))

            # Handle response if necessary
            metadata_headers = resp[3]  # OUT: RQX Response Metadata/Headers
            query_response = resp[2]     # OUT: RQX Response

            print("Inserted Triple:", (s, p, o))
            print("Response Metadata:", metadata_headers)
            print("Query Response:", query_response)

    finally:
        cursor.close()