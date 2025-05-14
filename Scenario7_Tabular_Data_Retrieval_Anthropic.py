#Scenario7: In this scenario, we answer questions based on tabular data by leveraging the ontologies generated in Scenario 6
#please make sure you install the following packages
#!pip install rdflib hdbcli langchain_aws langchain_core
from xml.etree import ElementTree as ET
from langchain_core.prompts import PromptTemplate
from hdbcli import dbapi
from langchain_aws import ChatBedrock
from typing import Dict, List
import pandas as pd

def setup(): 
    
    anthropic = ChatBedrock(
        model_id="anthropic.claude-3-5-sonnet-20240620-v1:0",
        aws_access_key_id="AWS_ACCESS_KEY_ID", #TODO
        aws_secret_access_key="AWS_SECRET_ACCESS_KEY",#TODO
        region_name="AWS_DEFAULT_REGION" #TODO
    
    )

    conn = dbapi.connect(
        user="HANA_ADMIN", #TODO Add your credentials
        password="HANA_ADMIN_PW",#TODO Add your credentials
        address='instance',#TODO Add your credentials
        port=443,
    )

    return anthropic, conn

"""""
Alternatively, if you wish to use Anthropic from SAP GenAI Hub, you can use the following setup() function:
#Here are the necessary Packages

#!%pip install generative-ai-hub-sdk[all]

import pandas as pd
from xml.etree import ElementTree as ET
from langchain_core.prompts import PromptTemplate
from hdbcli import dbapi
from typing import Dict, List
from gen_ai_hub.proxy.langchain.amazon import ChatBedrock
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client
import os

# Set up the AI core credentials
os.environ['AICORE_AUTH_URL'] = "<Your AI Core Auth URL>" #TODO
os.environ['AICORE_CLIENT_ID'] = "<Your AI Core Client ID>"#TODO
os.environ['AICORE_RESOURCE_GROUP'] = 'default'
os.environ['AICORE_CLIENT_SECRET'] = "<Your AI Core Client Secret>"#TODO
os.environ['AICORE_BASE_URL'] = "<Your AI Core Base URL>" #TODO

def setup(): 
    
    proxy_client = get_proxy_client('gen-ai-hub') # Get the proxy client

    anthropic = ChatBedrock(
        model_name="anthropic--claude-3.5-sonnet",
        proxy_client=proxy_client # Pass the proxy client to ChatBedrock
    )

    conn = dbapi.connect(
        user="HANA_ADMIN", #TODO Add your credentials
        password="HANA_ADMIN_PW",#TODO Add your credentials
        address='instance',#TODO Add your credentials
        port=443,
    )

    return anthropic, conn

"""""
def extract_metadata(question: str, conn) -> List[Dict]:
    """Extract relevant metadata from RDF triples using SPARQL"""
    cursor = conn.cursor()
    
    try:
        # Execute SPARQL query to get all relevant triples
        sparql_query = """
        SELECT ?s ?p ?o
        WHERE {
            ?s ?p ?o.
            FILTER(STRSTARTS(STR(?s), "http://flight_database.org/")) #TODO
        }
        """
        
        resp = cursor.callproc('SPARQL_EXECUTE', (sparql_query, 'Metadata headers describing Input and/or Output', '?', None))
        
        if resp and len(resp) >= 3 and resp[2]:
            # Parse the XML response
            xml_response = resp[2]
            results = parse_sparql_results(xml_response)
            
            # Convert to our standard format
            metadata = []
            for row in results:
                metadata.append({
                    's': row.get('s', ''),
                    'p': row.get('p', ''),
                    'o': row.get('o', '')
                })
            return metadata
        return []
    
    except Exception as e:
        print(f"Error executing SPARQL query: {e}")
        return []
    finally:
        cursor.close()

def analyze_metadata(metadata: List[Dict], question: str, anthropic) -> Dict:
    """Analyze the metadata to identify tables, columns, and relationships"""
    # Convert metadata to a format the LLM can understand
    metadata_str = "\n".join([f"{item['s']} {item['p']} {item['o']}" for item in metadata])
    
    prompt_template = """Given the following RDF metadata about database tables and columns, analyze the user's question and identify:
    1. The main table(s) involved with their schema (SFLIGHT)
    2. The columns needed (including any aggregation functions)
    3. Any filters or conditions
    4. Any joins required

    Important Rules:
    - Always include the schema name (SFLIGHT) before table names
    - When using GROUP BY, include the grouping columns in SELECT
    - Never include any explanatory text in the SQL output
    - For airline codes like American Airlines, use 'AA' in filters

    For each column, include:
    - The column name (prefix with table alias if needed)
    - Any aggregation function (SUM, COUNT, etc.)
    - Any filter conditions
    - Whether it's a grouping column

    For tables, include:
    - The full table name with schema (e.g., SFLIGHT.SBOOK)
    - Any relationships to other tables


    Metadata:
    {metadata}

    Question: {question}

    Return your analysis in this exact format (without any additional explanations):
    Tables: [schema.table]
    Columns: [column names with aggregations like SUM(LOCCURAM)]
    Filters: [filter conditions]
    Joins: [join conditions]
    GroupBy: [columns to group by]
    """
    
    prompt = PromptTemplate.from_template(prompt_template).invoke({
        "metadata": metadata_str,
        "question": question
    })
    
    # We'll use the LLM to extract the key components
    analysis = anthropic.invoke(prompt)
    return parse_analysis(analysis.content)

def parse_analysis(analysis_text: str) -> Dict:
    """Parse the LLM's analysis into a structured format"""
    components = {
        "tables": [],
        "columns": [],
        "filters": [],
        "joins": [],
        "group_by": []
    }
    
    # Remove any "Explanation:" text
    analysis_text = analysis_text.split("Explanation:")[0].strip()
    
    # Parse each section
    current_section = None
    for line in analysis_text.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        if line.startswith('Tables:'):
            current_section = 'tables'
            tables = line.split(':')[1].strip()
            components['tables'] = [t.strip() for t in tables.split(',') if t.strip()]
        elif line.startswith('Columns:'):
            current_section = 'columns'
            cols = line.split(':')[1].strip()
            for col_part in cols.split(','):
                col_part = col_part.strip()
                if col_part:
                    if '(' in col_part and ')' in col_part:
                        agg = col_part.split('(')[0].strip().upper()
                        col = col_part.split('(')[1].split(')')[0].strip()
                        components['columns'].append((agg, col))
                    else:
                        components['columns'].append((None, col_part))
        elif line.startswith('Filters:'):
            current_section = 'filters'
            filters = line.split(':')[1].strip()
            components['filters'] = [f.strip() for f in filters.split(' AND ') if f.strip()]
        elif line.startswith('Joins:'):
            current_section = 'joins'
            joins = line.split(':')[1].strip()
            components['joins'] = [j.strip() for j in joins.split(',') if j.strip()]
        elif line.startswith('GroupBy:'):
            current_section = 'group_by'
            group_bys = line.split(':')[1].strip()
            components['group_by'] = [g.strip() for g in group_bys.split(',') if g.strip()]
        elif current_section:
            # Handle multi-line sections
            if current_section == 'tables':
                components['tables'].extend([t.strip() for t in line.split(',') if t.strip()])
            elif current_section == 'columns':
                for col_part in line.split(','):
                    col_part = col_part.strip()
                    if col_part:
                        if '(' in col_part and ')' in col_part:
                            agg = col_part.split('(')[0].strip().upper()
                            col = col_part.split('(')[1].split(')')[0].strip()
                            components['columns'].append((agg, col))
                        else:
                            components['columns'].append((None, col_part))
            elif current_section == 'filters':
                components['filters'].extend([f.strip() for f in line.split(' AND ') if f.strip()])
            elif current_section == 'joins':
                components['joins'].extend([j.strip() for j in line.split(',') if j.strip()])
            elif current_section == 'group_by':
                components['group_by'].extend([g.strip() for g in line.split(',') if g.strip()])
    
    # Ensure schema is included in table names
    components['tables'] = [f"SFLIGHT.{t.split('.')[-1]}" if '.' not in t else t for t in components['tables']]
    
    # Ensure grouping columns are included in SELECT - CORRECTED VERSION
    for group_col in components['group_by']:
        # Check if this exact (None, group_col) pair exists
        col_exists = any(col == (None, group_col) for col in components['columns'])
        # Check if group_col appears in any non-aggregated column reference
        col_part_of_ref = any(group_col in col[1] for col in components['columns'] if col[0] is None)
        
        if not col_exists and not col_part_of_ref:
            components['columns'].append((None, group_col))
    
    return components

def parse_sparql_results(xml_response: str) -> List[Dict]:
    """Parse SPARQL XML results into a list of dictionaries"""
    try:
        root = ET.fromstring(xml_response)
        results = []
        
        for result in root.findall('.//{http://www.w3.org/2005/sparql-results#}result'):
            row = {}
            for binding in result:
                var_name = binding.attrib['name']
                value = binding[0]  # uri or literal
                if value.tag.endswith('uri'):
                    row[var_name] = value.text
                elif value.tag.endswith('literal'):
                    row[var_name] = value.text
            results.append(row)
        return results
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return []
    
def generate_sql(components: Dict) -> str:
    """Generate clean SQL query from the analyzed components"""
    # Validate components
    if not components["tables"]:
        raise ValueError("No tables identified for SQL generation")
    
    # Clean all components first
    def clean_component(component):
        return component.replace('[', '').replace(']', '').strip()
    
    # Build SELECT clause - ensure GROUP BY columns are included
    select_parts = []
    
    # First add all GROUP BY columns to SELECT if they're not already there
    for group_col in components.get("group_by", []):
        group_col = clean_component(group_col)
        if not any(col[1] == group_col for col in components["columns"] if col[0] is None):
            select_parts.append(group_col)
    
    # Then add the requested columns
    for agg, col in components["columns"]:
        col = clean_component(col)
        if not col:
            continue
        if agg:
            select_parts.append(f"{agg}({col}) AS {agg}_{col}")
        else:
            if col not in select_parts:  # Don't add duplicates
                select_parts.append(col)
    
    if not select_parts:  # Default to all columns if none specified
        select_parts.append("*")

    select_clause = ", ".join(select_parts[1:])
    print("SELECT BEFORE "+select_clause)
    print(select_parts)
    
    # Build FROM clause
    from_table = clean_component(components["tables"][0])
    from_clause = from_table
    
    # Add joins only if they exist and are not empty
    join_clauses = []
    for join in components.get("joins", []):
        clean_join = clean_component(join)
        if clean_join and clean_join != 'INNER JOIN':
            join_clauses.append(f"INNER JOIN SFLIGHT.SCUSTOM ON {clean_join}")
    print("INNER JOIN "+clean_join)
    
    # Build WHERE clause
    where_clauses = []
    for filter_cond in components.get("filters", []):
        clean_filter = clean_component(filter_cond)
        if clean_filter:
            where_clauses.append(clean_filter)
    
    where_clause = " AND ".join(where_clauses) if where_clauses else ""
    where_clause = where_clause.replace(",", " AND")
    print("WHERE CLAUSE "+where_clause)
    
    # Build GROUP BY clause
    group_by_columns = [clean_component(g) for g in components.get("group_by", []) if clean_component(g)]
    group_by_clause = ", ".join(group_by_columns) if group_by_columns else ""
    
    # Construct the SQL
    sql = f"SELECT {select_clause} FROM {from_clause}"
    
    if join_clauses:
        sql += " " + " ".join(join_clauses)
    
    if where_clause and group_by_clause:
        sql += f" GROUP BY {group_by_clause} HAVING {where_clause}"
    
    elif where_clause:
        sql += f" WHERE {where_clause}"

    if group_by_clause and not where_clause:
        sql += f" GROUP BY {group_by_clause}"
    
    # Final formatting
    sql = sql.strip()
    if not sql.endswith(';'):
        sql += ';'
    
    return sql

def execute_sql(sql_query: str, conn) -> pd.DataFrame:
    """Execute the generated SQL query and return results"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return pd.DataFrame()
    finally:
        cursor.close()

def generate_response_structured(question: str, results: pd.DataFrame, anthropic) -> str:
    """Generate a natural language response from the query results"""
    if results.empty:
        return "No results found for your query."
    
    prompt_template = """Convert the following query results into a natural language response to the user's question. 
    Keep the response concise but informative. Include relevant numbers and comparisons where appropriate.
    
    Question: {question}
    
    Results:
    {results}
    
    Response:
    """
    
    prompt = PromptTemplate.from_template(prompt_template).invoke({
        "question": question,
        "results": results.to_string()
    })
    
    response = anthropic.invoke(prompt)
    return response.content

def process_question(question: str, conn, anthropic) -> str:
    """Main function to process a user question with better error handling"""
    try:
        # Step 1: Extract relevant metadata using SPARQL
        metadata = extract_metadata(question, conn)
        
        if not metadata:
            return "Could not retrieve database metadata."
        
        # Step 2: Analyze the metadata and question
        components = analyze_metadata(metadata, question, anthropic)
        
        # Step 3: Generate SQL query
        sql_query = generate_sql(components)
    
        # Step 4: Execute SQL
        results = execute_sql(sql_query, conn)
        
        # Step 5: Generate response
        response = generate_response_structured(question, results, anthropic)
        
        return response
    except Exception as e:
        return f"Error processing question: {str(e)}"


question = "Show me all flight booking revenue for American Airlines." #TODO Add your question here
anthropic, conn = setup()
answer = process_question(question, conn, anthropic)
print(answer)