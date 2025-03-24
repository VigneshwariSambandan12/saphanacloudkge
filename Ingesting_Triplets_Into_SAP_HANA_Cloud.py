
from hdbcli import dbapi

# Add your HANA credentials here TODO
conn = dbapi.connect(
    user = "<username>",
    password = "<password>",
    address = "<instance id>", 
    port = 000,
)


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

# Validate by printing triplets
from rdflib import URIRef

def extract_local_part(uri):
    """Extract the local part of a URI."""
    # Convert to string, split by # or /, and take the last segment
    uri_str = str(uri)
    if '#' in uri_str:
        return uri_str.split('#')[-1]
    else:
        return uri_str.split('/')[-1]

def arrow_style_print_rdf_triples_no_ns(rdf_triples):
    """Print RDF triples without namespaces in 'subject -> predicate -> object' format."""
    for idx, (subject, predicate, obj) in enumerate(rdf_triples, start=1):
        subj = extract_local_part(subject)
        pred = extract_local_part(predicate)
        objt = extract_local_part(obj)

        print(f"Triple {idx}: {subj} -> {pred} -> {objt}")
        print("-" * 40)


arrow_style_print_rdf_triples_no_ns(rdf_triples)