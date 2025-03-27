
from hdbcli import dbapi

# Add your HANA credentials here TODO
conn = dbapi.connect(
    user = "<username>",
    password = "<password>",
    address = "<instance id>", 
    port = 000,
)


triples = []

# Call stored procedure to execute SPARQL query or insert RDF triples
cursor = conn.cursor()
try:
    # Iterate through the triples and insert them into the database
    for s, p, o in rdf_triples: #NOTE: This is the RDF triples object from the "Generating Triples file"
        
        # Append the formatted triple to the list
        triples.append(f"<{str(s)}> <{str(p)}> <{str(o)}>")

    # Join triples into SPARQL statement
    triples_str = " .\n    ".join(triples) + " ."
    
    sparql_insert_query = f"INSERT DATA {{ \n    {triples_str} \n}}"

    # Execute the SPARQL query using the stored procedure
    resp = cursor.callproc('SPARQL_EXECUTE', (sparql_insert_query,"Accept: application/sparql-results+xml Content-Type: application/sparql-query", '?', None))

    # Handle response if necessary
    metadata_headers = resp[3]  # OUT: RQX Response Metadata/Headers
    query_response = resp[2]     # OUT: RQX Response

    print("Inserted Triple:", (s, p, o))
    print("Response Metadata:", metadata_headers)
    print("Query Response:", query_response)

finally:
    cursor.close()