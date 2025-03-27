import re
from langchain_core.documents import Document
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_text_splitters import RecursiveCharacterTextSplitter
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import TokenTextSplitter
from rdflib import Graph, URIRef, Namespace, Literal
from rdflib.namespace import RDF

# Define a namespace for the RDF graph
EX = Namespace("http://test_mission_faqhanahotspots.org/")

# Initialize an RDF graph
g = Graph()

# Instantiate LLMGraphTransformer
llm_transformer = LLMGraphTransformer(
    llm="<Add your LLM object>",  #TODO 
)


# Function to load PDF documents
def load_documents():
    documents = []
    file_path = "<Your PDF Path>"  #TODO 

    try:
        loader = PyPDFLoader(file_path)
        loaded_docs = loader.load()
        if loaded_docs:
            documents.extend(loaded_docs)
    except Exception as e:
        print(f"Error loading documents: {e}")

    return documents

# Function to clean the text
def clean_text(text):
    bad_chars = ['"', "\n", "'"]
    for char in bad_chars:
        text = text.replace(char, '')
    return text

def create_chunks(documents, chunk_size=500, chunk_overlap=50):
    text_splitter = TokenTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    
    chunks = []
    for doc in documents:
        cleaned_text = clean_text(doc.page_content)
        doc_chunks = text_splitter.split_text(cleaned_text)
        for chunk in doc_chunks:
            chunks.append(Document(page_content=chunk, metadata=doc.metadata))
    return chunks

# Main processing function
def process_documents(llm_transformer):
    # Step 1: Load documents
    documents = load_documents()
    
    if not documents:
        print("No documents loaded.")
        return []

    # Step 2 and Step 3: Clean and chunk documents
    chunks = create_chunks(documents)
    print(f"Documents split into {len(chunks)} chunks.")

    # Step 4: Convert chunks to graph documents with LLM
    graph_document_list = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for i, chunk in enumerate(chunks):
            futures.append(executor.submit(llm_transformer.convert_to_graph_documents, [chunk]))

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                graph_document = future.result()
                graph_document_list.extend(graph_document)
                print(f"Chunk {i} processed into graph document.")
            except Exception as e:
                print(f"Error processing chunk {i}: {e}")

    return graph_document_list

# Function to safely create a URIRef from a string while handling spaces and special characters
def safe_uri(string):
    return URIRef(EX[string.replace(" ", "_").replace("/", "_")])


# Process the documents and get graph documents
graph_documents = process_documents(llm_transformer)

# Add nodes to the RDF graph
for document in graph_documents:
    for node in document.nodes:
        node_uri = safe_uri(node.id)
        node_type_uri = safe_uri(node.type)
        
        # Add the node type triple
        g.add((node_uri, RDF.type, node_type_uri))
        
        # Add properties as triples
        for key, value in node.properties.items():
            g.add((node_uri, safe_uri(key), Literal(value)))

    # Add relationships to the RDF graph
    for relationship in document.relationships:
        source_uri = safe_uri(relationship.source.id)
        target_uri = safe_uri(relationship.target.id)
        relationship_type_uri = safe_uri(relationship.type)
        
        # Add the relationship triple
        g.add((source_uri, relationship_type_uri, target_uri))

# Extract and print RDF triples
rdf_triples = []
for s, p, o in g:
    rdf_triples.append((str(s), str(p), str(o)))


#Optionally - print the triples to validate them
print(rdf_triples)