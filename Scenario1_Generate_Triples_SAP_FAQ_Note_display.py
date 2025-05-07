#For Scenario1, we will consume the SAP FAQ note as mentioned in the mission, process it using graph transformers
#convert to triples and display all the generated RDF triples
#please make sure you install the following packages
# !pip install langchain_openai pypdf hdbcli langchain_experimental pdfplumber rdflib
# Importing the Python regex module for pattern matching in text
import re
# Importing Document class from langchain_core for structured document representation
from langchain_core.documents import Document
# Importing Graph Transformer from langchain_experimental to convert text to knowledge graphs
from langchain_experimental.graph_transformers import LLMGraphTransformer
# Importing Text Splitter for breaking down text into manageable chunks
from langchain_text_splitters import RecursiveCharacterTextSplitter
# ThreadPoolExecutor for parallel processing of text chunks
from concurrent.futures import ThreadPoolExecutor
# 'concurrent.futures' provides high-level interface for asynchronous execution to improve performance
import concurrent.futures
# Importing PyPDFLoader to extract text from PDF documents
from langchain_community.document_loaders import PyPDFLoader
# Importing AzureChatOpenAI to use Azure's OpenAI services for LLM processing
from langchain_openai import AzureChatOpenAI
# Importing TokenTextSplitter to divide text based on token count rather than characters
from langchain_text_splitters import TokenTextSplitter
# rdflib provides tools for RDF graph creation, manipulation and serialization
from rdflib import Graph, URIRef, Namespace, Literal
from rdflib.namespace import RDF
# OS module for file system operations and environment variable access
import os
# Importing database API from SAP HANA client for database connections
from hdbcli import dbapi
# Define a namespace URI for our RDF graph entities to ensure uniqueness
EX = Namespace("http://new_test_mission_faqhanahotspots.org/")

# Establish connection to SAP HANA Cloud database for storing triples
# The triplestore must be enabled on the target database beforehand
conn = dbapi.connect(
    user = "<Your HANA Cloud User Name>",  
    password = "<Your HANA Cloud Password>",
    address = '<Your HANA Cloud User Host',
    port = 443, # Standard HTTPS port for secure connection
)

# Azure OpenAI configuration - these credentials are used to access the Azure OpenAI service
AZURE_OPENAI_API_KEY = "<Your Azure API Keys>"
AZURE_OPENAI_ENDPOINT = "https://<Your Deployment Name>.openai.azure.com/"
AZURE_OPENAI_API_VERSION = "2024-05-01-preview"
AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = "<Provide your deployment Name for gpt4o>"

# Initialize the Azure OpenAI client with gpt-4o model
# Configuration includes timeout handling and retry policies for robustness
client = AzureChatOpenAI(
    azure_deployment="gpt-4o",
    azure_endpoint= AZURE_OPENAI_ENDPOINT,
    api_key= AZURE_OPENAI_API_KEY,
    api_version='2025-01-01-preview',
    temperature=1,  # Higher temperature for more creative outputs
    max_tokens=None, # No limit on response length
    timeout=None,    # No timeout limit
    max_retries=2    # Retry failed requests up to 2 times
)

# Create an empty RDF graph to store our knowledge triples
g = Graph()

# Initialize the LLM Graph Transformer that will convert text to structured graph data
# This uses the Azure OpenAI model for text understanding and extraction
llm_transformer = LLMGraphTransformer(
    llm=client,
)

# Function to load PDF documents from specified path
def load_documents():
    # Initialize empty list to store all loaded document objects
    documents = []
    # Path to the PDF file containing SAP HANA Hotspots information
    #Example path : file_path = "//content//pdf//2927209_E_20250327.pdf"
    file_path = "<Provide your path to SAP HANA HOtspots Note>"

    try:
        # Create a PDF loader for the specified file
        loader = PyPDFLoader(file_path)
        # Extract text and metadata from the PDF
        loaded_docs = loader.load()
        # If documents were successfully loaded, add them to our collection
        if loaded_docs:
            documents.extend(loaded_docs)
            print(loaded_docs)
    except Exception as e:
        # Log any errors that occur during document loading for debugging
        print(f"Error loading documents: {e}")

    # Return all loaded documents for further processing
    return documents

# Function to clean and normalize text by removing problematic characters
def clean_text(text):
    # List of characters that could cause issues in processing or RDF generation
    bad_chars = ['"', "\n", "'"]
    # Remove each problematic character from the text
    for char in bad_chars:
        text = text.replace(char, '')
    # Return the sanitized text
    return text

# Function to split documents into smaller, manageable chunks for processing
def create_chunks(documents, chunk_size=500, chunk_overlap=50):
    # Create a splitter that works based on token count (better for LLM processing)
    text_splitter = TokenTextSplitter(
        chunk_size=chunk_size,       # Maximum tokens per chunk
        chunk_overlap=chunk_overlap  # Overlap between chunks to maintain context
    )

    # List to store all generated chunks
    chunks = []
    # Process each document
    for doc in documents:
        # Clean the text before splitting to avoid issues
        cleaned_text = clean_text(doc.page_content)
        # Split the cleaned text into chunks
        doc_chunks = text_splitter.split_text(cleaned_text)
        # Create Document objects for each chunk, preserving original metadata
        for chunk in doc_chunks:
            chunks.append(Document(page_content=chunk, metadata=doc.metadata))
    # Return the list of chunk documents
    return chunks

# Main function to process documents into graph format using parallel execution
def process_documents(llm_transformer):
    # Load all documents from the source
    documents = load_documents()

    # Check if any documents were loaded
    if not documents:
        print("No documents loaded.")
        return []

    # Split documents into manageable chunks
    chunks = create_chunks(documents)
    print(f"Documents split into {len(chunks)} chunks.")

    # List to store processed graph documents
    graph_document_list = []

    # Use thread pool for parallel processing to improve performance
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []

        # Submit each chunk for processing asynchronously
        for i, chunk in enumerate(chunks):
            futures.append(executor.submit(llm_transformer.convert_to_graph_documents, [chunk]))

        # Collect results as they complete (not necessarily in order)
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                # Get the processed graph document
                graph_document = future.result()
                # Add to our collection of graph documents
                graph_document_list.extend(graph_document)
                print(f"Chunk {i} processed into graph document.")
            except Exception as e:
                # Log errors for specific chunks without failing the entire process
                print(f"Error processing chunk {i}: {e}")

    # Return all successfully processed graph documents
    return graph_document_list

# Function to create safe URIs by replacing problematic characters
# This ensures valid RDF identifiers for all entities
def safe_uri(string):
    # Replace spaces and slashes with underscores to create valid URI components
    return URIRef(EX[string.replace(" ", "_").replace("/", "_")])

# Process documents to extract structured graph information
graph_documents = process_documents(llm_transformer)

# Convert all extracted knowledge into RDF triples
for document in graph_documents:
    # Process each node (entity) in the document
    for node in document.nodes:
        # Create URIs for the node entity and its type
        node_uri = safe_uri(node.id)
        node_type_uri = safe_uri(node.type)

        # Add a triple indicating the node's type
        g.add((node_uri, RDF.type, node_type_uri))

        # Add all properties of the node as separate triples
        for key, value in node.properties.items():
            g.add((node_uri, safe_uri(key), Literal(value)))

    # Process each relationship between entities
    for relationship in document.relationships:
        # Create URIs for the source entity, target entity, and relationship type
        source_uri = safe_uri(relationship.source.id)
        target_uri = safe_uri(relationship.target.id)
        relationship_type_uri = safe_uri(relationship.type)

        # Add a triple representing the relationship
        g.add((source_uri, relationship_type_uri, target_uri))

# Create a list of all generated RDF triples for storage and querying
rdf_triples = []
# Extract all triples from the RDF graph
for s, p, o in g:
    # Convert RDF terms to strings and store as a tuple
    rdf_triples.append((str(s), str(p), str(o)))

# Print all generated RDF triples for verification
print(rdf_triples)
"""
# Create a list to store formatted triples for SPARQL insertion
triples = []

# Prepare to execute database operations with SAP HANA
cursor = conn.cursor()
try:
    # Format each triple for SPARQL insertion
    for s, p, o in rdf_triples:
        # Format as N-Triples syntax with angle brackets around URIs
        triples.append(f"<{str(s)}> <{str(p)}> <{str(o)}>")

    # Join all formatted triples into a single SPARQL insert statement
    triples_str = " .\n    ".join(triples) + " ."

    # Create the complete SPARQL INSERT DATA query
    sparql_insert_query = f"INSERT DATA {{ \n    {triples_str} \n}}"

    # Execute the query using SAP HANA's SPARQL stored procedure
    resp = cursor.callproc('SPARQL_EXECUTE', (sparql_insert_query,"Accept: application/sparql-results+xml Content-Type: application/sparql-query", '?', None))

    # Extract response details for logging and verification
    metadata_headers = resp[3]  # OUT: Response metadata and headers
    query_response = resp[2]     # OUT: Query execution response

    # Print details of the insertion operation
    print("Inserted Triple:", (s, p, o))
    print("Response Metadata:", metadata_headers)
    print("Query Response:", query_response)

finally:
    # Ensure cursor is closed to free resources
    cursor.close()
"""
