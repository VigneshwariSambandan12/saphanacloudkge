import os
os.environ['AICORE_AUTH_URL'] = "" #TODO
os.environ['AICORE_CLIENT_ID'] = ""#TODO
os.environ['AICORE_RESOURCE_GROUP'] = 'default'#TODO
os.environ['AICORE_CLIENT_SECRET'] = ""#TODO
os.environ['AICORE_BASE_URL'] = ""#TODO

from gen_ai_hub.proxy.langchain.amazon import ChatBedrock
from pydantic import BaseModel, ConfigDict, model_validator
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client

proxy_client = get_proxy_client('gen-ai-hub') # Get the proxy client

anthropic = ChatBedrock(
    model_name="anthropic--claude-3.5-sonnet",
    proxy_client=proxy_client # Pass the proxy client to ChatBedrock
)