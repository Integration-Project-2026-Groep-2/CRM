
import asyncio
from lxml import etree
from datetime import datetime, timezone
from unittest.mock import MagicMock

# Mocking the sender
class MockSender:
    def __init__(self):
        self.published_data = None
    async def publish_user_confirmed(self, data):
        self.published_data = data

# Mocking mapping._build_user_data
def mock_build_user_data(contact):
    data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "firstName": contact.get("FirstName", ""),
        "lastName": contact.get("LastName", ""),
        "role": contact.get("Role__c", "VISITOR"),
        "isActive": True,
        "gdprConsent": True,
        "confirmedAt": "2026-05-13T22:33:00Z"
    }
    if contact.get("Company_ID__c"):
        data["companyId"] = contact["Company_ID__c"]
    return data

async def test_repro():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<UserCreated>
    <facturatieCustomerId>123</facturatieCustomerId>
    <firstName>John</firstName>
    <lastName>Doe</lastName>
    <email>john.doe@example.com</email>
    <role>COMPANY_CONTACT</role>
    <companyId>550e8400-e29b-41d4-a716-446655440000</companyId>
    <isActive>true</isActive>
    <createdAt>2026-05-13T22:33:00Z</createdAt>
</UserCreated>"""
    
    xml = etree.fromstring(xml_content.encode("utf-8"))
    
    email = xml.findtext("email")
    print(f"Parsed email: {email}")
    
    company_id = xml.findtext("companyId")
    print(f"Parsed company_id: {company_id}")
    
    contact_data = {
        "FirstName": xml.findtext("firstName"),
        "LastName": xml.findtext("lastName"),
        "Email": email,
        "Role__c": xml.findtext("role"),
        "GDPR_Consent__c": True,
    }
    
    if company_id:
        contact_data["Company_ID__c"] = company_id
        
    print(f"Contact data before create: {contact_data}")
    
    # Simulate create_contact
    contact_record = {
        "Id": "003...",
        "CRM_ID__c": "user-uuid",
        "Email": contact_data["Email"],
        "FirstName": contact_data["FirstName"],
        "LastName": contact_data["LastName"],
        "Role__c": contact_data["Role__c"],
        "Company_ID__c": contact_data.get("Company_ID__c")
    }
    
    outbound_data = mock_build_user_data(contact_record)
    print(f"Outbound data: {outbound_data}")
    
    if "companyId" in outbound_data:
        print("SUCCESS: companyId found in outbound data")
    else:
        print("FAILURE: companyId NOT found in outbound data")

if __name__ == "__main__":
    asyncio.run(test_repro())
