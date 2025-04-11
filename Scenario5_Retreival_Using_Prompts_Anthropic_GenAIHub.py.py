# Scenario5: Provide User Prompt and retrieve response from KGs from SAP HANA Cloud Using Anthropic Claude from SAP GenAI Hub
#Required packages: Make Sure you install packages mentioned below. And the code is tested end to end using colab
#Here are the necessary Packages
#!%pip install generative-ai-hub-sdk[all]
# Import necessary libraries and modules
import os
from langchain_core.language_models.base import BaseLanguageModel  # Base class for language models
from pydantic import BaseModel, Field  # For data validation and settings management
from langchain_core.prompts import PromptTemplate  # For creating prompt templates
from typing_extensions import TypedDict, Annotated  # For type hints
from hdbcli import dbapi  # SAP HANA database connector
from gen_ai_hub.proxy.langchain.amazon import ChatBedrock
from pydantic import BaseModel, ConfigDict, model_validator
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client

# Set up the AI core credentials
os.environ['AICORE_AUTH_URL'] = "<Your AI Core Auth URL>" #TODO
os.environ['AICORE_CLIENT_ID'] = "<Your AI Core Client ID>"#TODO
os.environ['AICORE_RESOURCE_GROUP'] = 'default'
os.environ['AICORE_CLIENT_SECRET'] = "<Your AI Core Client Secret>"#TODO
os.environ['AICORE_BASE_URL'] = "<Your AI Core Base URL>" #TODO

# Custom language model implementation for AWS Bedrock
class CustomBedrockLLM(BaseLanguageModel):

    # Constructor method
    def __init__(self, **kwargs):
        # Call parent class constructor
        super().__init__(**kwargs)

    # Implementation of required LangChain interface methods
    def __call__(self, query):
        return self._call(query)

    def agenerate_prompt(self, input_prompt):
        return input_prompt

    def apredict(self, query):
        return self._call(query)

    def apredict_messages(self, messages):
        return self._call(messages[0].content)

    def generate_prompt(self, input_prompt):
        return input_prompt

    def invoke(self, query):
        return self._call(query)

    def predict(self, query):
        return self._call(query)

    def predict_messages(self, messages):
        return self._call(messages[0].content)
        
proxy_client = get_proxy_client('gen-ai-hub') # Get the proxy client

anthropic = ChatBedrock(
    model_name="anthropic--claude-3.5-sonnet",
    proxy_client=proxy_client # Pass the proxy client to ChatBedrock
)

# Establish connection to SAP HANA database
conn = dbapi.connect(
    user="<You HANA Cloud User Name>",  # Database username
    password="<Your HANA Cloud Password>",  # Database password
    address='<Your HANA Cloud host>',  # DB address
    port=443  # Connection port
)

# Define template for SPARQL query generation
#give example in template and try different LLMs
template = '''Given an input question, your task is to create a syntactically correct SPARQL query to retrieve information from an RDF graph. The graph may contain variations in spacing, underscores, dashes, capitalization, reversed relationships, and word order. You must account for these variations using the `REGEX()` function in SPARQL. In the RDF graph, subjects are represented as "s", objects are represented as "o", and predicates are represented as "p". Account for underscores.

Example Question: "What are SAP HANA Hotspots"
Example SPARQL Query: SELECT ?s ?p ?o
WHERE {{
    ?s ?p ?o .
    FILTER(
        REGEX(str(?s), "SAP_HANA_Hotspots", "i") ||
        REGEX(str(?o), "SAP_HANA_Hotspots", "i")
    )
}}

Retrieve only triples beginning with "http://new_test_mission_faqhanahotspots.org/"
Use the following format:
Question: f{input}
S: Subject to look for in the RDF graph
P: Predicate to look for in the RDF graph
O: Object to look for in the RDF graph
SPARQL Query: SPARQL Query to run, including s-p-o structure
'''

# Create prompt template from the template string
query_prompt_template = PromptTemplate.from_template(template)

# Define type for state dictionary using TypedDict
class State(TypedDict):
    question: str  # The input question
    s: str  # Subject for SPARQL query
    p: str  # Predicate for SPARQL query
    o: str  # Object for SPARQL query
    query: str  # The generated query

# Define output type for structured LLM response
class QueryOutput(TypedDict):
    """Generated SPARQL query."""
    query: Annotated[str, ..., "Syntactically valid SPARQL query."]

# Function to generate SPARQL query from natural language question
def write_query(state: State):
    """Generate SPARQL query to fetch information."""
    # Format the prompt with the input question
    prompt = query_prompt_template.invoke({"input": state["question"]})
    
    # Configure LLM to return structured output
    structured_llm = anthropic.with_structured_output(QueryOutput)
    
    # Get the generated query from LLM
    result = structured_llm.invoke(prompt)
    
    # Print and return the query
    print(result["query"])
    return {"query": result["query"]}

# Function to execute SPARQL query against HANA database
def execute_sparql(query_response):
    print()  # Print empty line for spacing
    
    # Create database cursor
    cursor = conn.cursor()
    
    try:
        # Execute SPARQL stored procedure
        resp = cursor.callproc('SPARQL_EXECUTE', (
            query_response["query"],  # The SPARQL query
            'Metadata headers describing Input and/or Output',  # Description
            '?',  # Output placeholder
            None  # Additional options
        ))

        # Process response if available
        if resp:
            # Extract metadata and results
            metadata_headers = resp[3]  # Response metadata
            query_response = resp[2]    # Actual query results
            
            # Print results
            print("Query Response:", query_response)
            print("Response Metadata:", metadata_headers)
            return query_response
        else:
            print("No response received from stored procedure.")

    except Exception as e:
        print("Error executing stored procedure:", e)
    finally:
        # Ensure cursor is closed
        cursor.close()

# Function to summarize query results into natural language
def summarize_info(question, query_response):
    # Define prompt template for summarization
    prompt = """Answer the user question below given the following relational information in XML format. Use as much as the query response as possible to give a full, detailed explanation. Interpret the URI and predicate information using context. Don't use phrases like 'the entity identified by the URI,' just say what the entity is.
    Also make sure the output is readable in a format that can be display through an HTML file, add appropriate formatting.
    Please remove unnecessary information. Do not add information about the triples. Do not add the source of the data.
    Do not include details about what they are identified as or what kind of entity they are unless asked. Do not add any suggestions unless explicitly asked. Simply give a crisp and direct answer to what has been asked!
    If you do not have an answer, please say so. DO NOT HALLUCINATE!
    User Question: {question}
    Information: {information}
    """    
    # Create prompt template
    summarize = PromptTemplate.from_template(prompt)
    
    # Format the prompt with question and results
    prompt_input = summarize.invoke({
        "question": question,
        "information": query_response
    })

    # Define output type for summarization
    class QuestionAnswer(TypedDict):
        """Generated answer."""
        final_answer: Annotated[str, ..., "Answer to user's question."]

    # Configure LLM for structured output
    translate_llm = anthropic.with_structured_output(QuestionAnswer)
    
    # Get final answer from LLM
    final_answer = translate_llm.invoke(prompt_input)
    
    # Print and return the answer
    print(final_answer["final_answer"])

# Main execution flow
question = "What are Hdbkpic?"  # The question to answer
sparql = write_query({"question": question})  # Generate SPARQL query
response = execute_sparql(sparql)  # Execute query
summarize_info(question, response)  # Generate and print answer