# Scenario4: Provide User Prompt and retrieve response from KGs from SAP HANA Cloud Using AWS Bedrock Anthropic Claude
#Required packages: Make Sure you install packages mentioned below. And the code is tested end to end using colab
#Here are the necessary Packages
#!pip install langchain_core typing hdbcli langchain-aws boto3
# Import necessary libraries and modules
from langchain_core.language_models.base import BaseLanguageModel  # Base class for language models
from pydantic import BaseModel, Field  # For data validation and settings management
import boto3  # AWS SDK for Python
from langchain_aws import ChatBedrock  # LangChain integration for AWS Bedrock
from langchain_core.prompts import PromptTemplate  # For creating prompt templates
from typing_extensions import TypedDict, Annotated  # For type hints
from hdbcli import dbapi  # SAP HANA database connector

# Define configuration model for AWS Bedrock using Pydantic
class CustomBedrockLLMConfig(BaseModel):
    # Required model ID field with description
    model_id: str = Field(..., description="ID of the Bedrock model to be used")
    # Required AWS access key field
    aws_access_key_id: str = Field(..., description="AWS IAM access key for authentication")
    # Required AWS secret key field
    aws_secret_access_key: str = Field(..., description="AWS IAM secret key for authentication")
    # Required AWS region field
    aws_region_name: str = Field(..., description="AWS region where Bedrock is available")

# Custom language model implementation for AWS Bedrock
class CustomBedrockLLM(BaseLanguageModel):
    # Class variable to hold configuration
    config: CustomBedrockLLMConfig

    # Constructor method
    def __init__(self, **kwargs):
        # Call parent class constructor
        super().__init__(**kwargs)
        # Initialize configuration with provided keyword arguments
        self.config = CustomBedrockLLMConfig(**kwargs)

    # Core method to call the model
    def _call(self, query):
        # Create AWS session with credentials
        session = boto3.Session(
            aws_access_key_id=self.config.aws_access_key_id,  # Set access key
            aws_secret_access_key=self.config.aws_secret_access_key,  # Set secret key
            region_name=self.config.aws_region_name  # Set AWS region
        )

        # Create Bedrock client from session
        bedrock_client = session.client('bedrock')

        # Invoke the Bedrock model
        response = bedrock_client.invoke_model(
            ModelId=self.config.model_id,  # Specify which model to use
            Input=query  # Pass the input query
        )

        # Return the model's output
        return response['Output']

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

# AWS credentials configuration (NOTE: These should be secured properly in production)
AWS_ACCESS_KEY_ID = "XXXXXXXX"  # AWS access key ID
AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXX"  # AWS secret access key
AWS_DEFAULT_REGION = "us-east-1"  # AWS region name

# Initialize Bedrock runtime client
bedrock_client = boto3.client(
    service_name='bedrock-runtime',  # Specify Bedrock runtime service
    aws_access_key_id=AWS_ACCESS_KEY_ID,  # Pass AWS credentials
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION  # Set AWS region
)

# Configure Claude LLM through LangChain
anthropic = ChatBedrock(
    model_id="anthropic.claude-3-sonnet-20240229-v1:0",  # Specific model version ID
    client=bedrock_client,  # Use configured Bedrock client
    model_kwargs={  # Model parameters
        "temperature": 0.5,  # Controls response randomness (0-1)
        "max_tokens": 2048  # Maximum length of response
    }
)

# Establish connection to SAP HANA database
conn = dbapi.connect(
    user="<You HANA Cloud User Name>",  # Database username
    password="<Your HANA Cloud Password>",  # Database password
    address='<Your HANA Cloud host>',  # DB address
    port=443  # Connection port
)

# Define template for SPARQL query generation
template = '''Given an input question, your task is to create a syntactically correct SPARQL query...'''
[Rest of the template string...]

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
    prompt = """Answer the user question below given the following relational information..."""
    [Rest of prompt string...]
    
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