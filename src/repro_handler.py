
import asyncio
from unittest.mock import MagicMock, AsyncMock
from lxml import etree
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.handlers.facturatie_user_created import handle
from src import xml_validator

async def test_reproduction():
    # Mock Salesforce
    sf = MagicMock()
    sf.Contact = MagicMock()
    
    # Mock contact match: return "none" (completely new user)
    sf.query.return_value = {"totalSize": 0, "records": []}
    
    # Mock create_contact: simulate Salesforce refresh
    def mock_create(data):
        print(f"DEBUG: sf.Contact.create called with: {data}")
        return "003000000000001"
    
    def mock_get(contact_id):
        print(f"DEBUG: sf.Contact.get called for: {contact_id}")
        # THIS IS THE CRITICAL PART: DOES IT RETURN Company_ID__c?
        return {
            "Id": contact_id,
            "FirstName": "John",
            "LastName": "Doe",
            "Email": "john.doe@example.com",
            "CRM_ID__c": "123e4567-e89b-12d3-a456-426614174000",
            # We simulate it being present in the returned record
            "Company_ID__c": "550e8400-e29b-41d4-a716-446655440000" 
        }

    sf.Contact.create = mock_create
    sf.Contact.get = mock_get

    # Mock Sender
    from src import sender
    sender.publish_user_confirmed = AsyncMock()
    
    # Message Body
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<UserCreated>
    <email>john.doe@example.com</email>
    <firstName>John</firstName>
    <lastName>Doe</lastName>
    <role>COMPANY_CONTACT</role>
    <companyId>550e8400-e29b-41d4-a716-446655440000</companyId>
    <isActive>true</isActive>
</UserCreated>"""
    
    message = MagicMock()
    message.body = xml_content.encode("utf-8")
    message.ack = AsyncMock()
    
    # Run handler
    await handle(message, sf)
    
    # Verify Sender call
    call_args = sender.publish_user_confirmed.call_args
    if call_args:
        user_data = call_args[0][0]
        print(f"DEBUG: publish_user_confirmed called with: {user_data}")
        if "companyId" in user_data:
            print("SUCCESS: companyId found in outbound data")
        else:
            print("FAILURE: companyId MISSING from outbound data")
    else:
        print("FAILURE: publish_user_confirmed was not called")

if __name__ == "__main__":
    asyncio.run(test_reproduction())
