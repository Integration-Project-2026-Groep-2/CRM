
import xml.etree.ElementTree as ET

def test_namespace():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<UserCreated xmlns="urn:facturatie:crm:contract">
    <email>john.doe@example.com</email>
    <companyId>550e8400-e29b-41d4-a716-446655440000</companyId>
</UserCreated>"""
    
    xml = ET.fromstring(xml_content)
    
    print(f"findtext('email'): {xml.findtext('email')}")
    print(f"findtext('companyId'): {xml.findtext('companyId')}")
    
    # Try with namespace
    ns = {"f": "urn:facturatie:crm:contract"}
    print(f"find('f:email', namespaces=ns).text: {xml.find('f:email', namespaces=ns).text}")

if __name__ == "__main__":
    test_namespace()
