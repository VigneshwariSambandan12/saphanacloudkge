from langchain_core.prompts import PromptTemplate
from typing_extensions import TypedDict, Annotated
from hdbcli import dbapi

# Add your HANA credentials here TODO / Use the connection object you created to ingest triples
conn = dbapi.connect(
    user = "<username>",
    password = "<password>",
    address = "<instance id>", 
    port = 000,
)

namespace = "<Your namespace>" #TODO

#give example in template and try different LLMs 
template = '''Given an input question, your task is to create a syntactically correct SPARQL query to retrieve information from an RDF graph. The graph may contain variations in spacing, underscores, dashes, capitalization, reversed relationships, and word order. You must account for these variations using the `REGEX()` function in SPARQL. In the RDF graph, subjects are represented as "s", objects are represented as "o", and predicates are represented as "p". Account for underscores. 

Example Question: "Who was Marie Curie?"
Example SPARQL Query: SELECT ?s ?p ?o
WHERE {{
    ?s ?p ?o .
    FILTER(
        REGEX(str(?s), "Marie_Curie", "i") ||
        REGEX(str(?o), "Marie_Curie", "i")
    )
}}

Retrieve only triples beginning with f{namespace} 
Use the following format:
Question: {input} 
S: Subject to look for in the RDF graph
P: Predicate to look for in the RDF graph
O: Object to look for in the RDF graph
SPARQL Query: SPARQL Query to run, including s-p-o structure


'''

class State(TypedDict):
    question: str
    s: str
    p: str
    o: str
    query: str

#return dictionary where key is query and value is SPARQL query string 
class QueryOutput(TypedDict):
    """Generated SPARQL query."""
    query: Annotated[str, ..., "Syntactically valid SPARQL query."]

def write_query(state: State):
    """Generate SPARQL query to fetch information."""
    prompt = query_prompt_template.invoke(
        {
            "input": state["question"],
        }
    )
    structured_llm = anthropic.with_structured_output(QueryOutput)
    result = structured_llm.invoke(prompt)
    print(result["query"])
    return {"query": result["query"]}

def execute_sparql(query_response):
    print()
    cursor = conn.cursor()
    try:
        # Execute 
        resp = cursor.callproc('SPARQL_EXECUTE', (query_response["query"], 'Metadata headers describing Input and/or Output', '?', None))
        
        # Check if the response contains expected OUT parameters
        if resp:
            # Extract metadata and query results from the OUT parameters
            metadata_headers = resp[3]  # OUT: RQX Response Metadata/Headers
            query_response = resp[2]    # OUT: RQX Response
            
            # Handle response
            print("Query Response:", query_response)
            print("Response Metadata:", metadata_headers)
            return query_response
        else:
            print("No response received from stored procedure.")
        
    except Exception as e:
        print("Error executing stored procedure:", e)
    finally:
        cursor.close()

def summarize_info(question, query_response): 
    prompt = """Answer the user question below given the following relational information in XML format. Use as much as the query response as possible to give a full, detailed explanation. Interpret the URI and predicate information using context. Don't use phrases like 'the entity identified by the URI,' just say what the entity is. 
    Also make sure the output is readable in a format that can be display through an HTML file, add appropriate formatting.
    Please remove unnecessary information. Do not add information about the triples. Do not add the source of the data.
    Do not include details about what they are identified as or what kind of entity they are unless asked. Do not add any suggestions unless explicitly asked. Simply give a crisp and direct answer to what has been asked!
    If you do not have an answer, please say so. DO NOT HALLUCINATE!
    User Question: {question}
    Information: {information}
    """
    summarize = PromptTemplate.from_template(prompt)
    prompt_input = summarize.invoke(
            {
                "question": question,
                "information": query_response,
            }
        )

    class QuestionAnswer(TypedDict):
        """Generated SPARQL query."""
        final_answer: Annotated[str, ..., "Answer to user's question."]

    translate_llm = anthropic.with_structured_output(QuestionAnswer)
    final_answer = translate_llm.invoke(prompt_input)
    print(final_answer["final_answer"])

# Retrieval begin here
question = "<Your question>" #TODO : Add your question here
sparql = write_query({"question": question})
response = execute_sparql(sparql)
summarize_info(question, response)