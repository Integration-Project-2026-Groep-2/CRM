"""Manual Salesforce connection test — run with: python test_sf_connection.py

Tests three things:
1. Can we authenticate with Salesforce?
2. Can we query existing objects?
3. Can we create a Contact with custom fields?

Step 3 will fail until custom fields are created in Salesforce Setup.
"""

import asyncio
import sys

from dotenv import load_dotenv

load_dotenv()

from src.config import load_config
from src.salesforce_client import get_salesforce_client, create_contact


async def main() -> None:
    # --- Step 1: Authenticate ---
    print("=" * 50)
    print("STEP 1: Salesforce authenticatie")
    print("=" * 50)
    try:
        config = load_config()
        print(f"Connecting as {config.salesforce_username}...")
        sf = await get_salesforce_client(config)
        print("PASS — Connected to Salesforce!\n")
    except Exception as e:
        print(f"FAIL — {e}")
        sys.exit(1)

    # --- Step 2: Query existing objects ---
    print("=" * 50)
    print("STEP 2: SOQL query test (standaard velden)")
    print("=" * 50)
    try:
        result = await asyncio.to_thread(
            sf.query, "SELECT Id, Name FROM Account LIMIT 1"
        )
        print(f"PASS — Query returned {result['totalSize']} Account(s)\n")
    except Exception as e:
        print(f"FAIL — {e}\n")

    # --- Step 3: Create Contact with custom fields ---
    print("=" * 50)
    print("STEP 3: Contact aanmaken met custom fields")
    print("=" * 50)
    print("Vereiste custom fields op Contact object:")
    print("  - CRM_ID__c (Text, Unique)")
    print("  - Role__c (Picklist: VISITOR | COMPANY_CONTACT)")
    print("  - GDPR_Consent__c (Checkbox)")
    print("  - Registration_ID__c (Text)")
    print()
    try:
        result = await create_contact(sf, {
            "FirstName": "Test",
            "LastName": "CRM Integration",
            "Email": "test-crm-integration@example.com",
            "Role__c": "VISITOR",
            "GDPR_Consent__c": True,
        })
        print(f"PASS — Contact aangemaakt!")
        print(f"  ID:       {result.get('Id')}")
        print(f"  CRM UUID: {result.get('CRM_ID__c')}")
        print(f"  Email:    {result.get('Email')}")
    except Exception as e:
        print(f"FAIL — {e}")
        print()
        print("Dit betekent waarschijnlijk dat de custom fields nog niet")
        print("aangemaakt zijn in Salesforce Setup. Zie salesforce_client.py")
        print("docstring voor de vereiste fields.")


if __name__ == "__main__":
    asyncio.run(main())
