"""Manual test — Contract 18: crm.user.updated

Run with: .venv/Scripts/python test_c18_user_updated.py

Tests:
1. Connect to RabbitMQ
2. Publish a UserUpdated message (required fields only)
3. Publish a UserUpdated message (all fields including address)

Requires:
- .env with RABBITMQ_URL (and SF credentials to pass config validation)
- Running RabbitMQ instance (localhost or Docker)
"""

import asyncio
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from src.config import load_config
from src.connection import get_rabbitmq_connection
from src import sender


async def main() -> None:
    shutdown = asyncio.Event()

    # --- Step 1: Connect to RabbitMQ ---
    print("=" * 50)
    print("STEP 1: RabbitMQ connectie")
    print("=" * 50)
    try:
        config = load_config()
        print(f"Connecting to {config.rabbitmq_url}...")
        connection = await get_rabbitmq_connection(config.rabbitmq_url, shutdown)
        channel = await connection.channel()
        await sender.init(channel)
        print("PASS — Connected to RabbitMQ!\n")
    except Exception as e:
        print(f"FAIL — {e}")
        sys.exit(1)

    # --- Step 1b: Declare queue (in productie doet het consuming team dit) ---
    print("Declaring queue crm.user.updated (durable=true)...")
    queue = await channel.declare_queue("crm.user.updated", durable=True)
    print(f"PASS — Queue ready ({queue.declaration_result.message_count} existing messages)\n")

    # --- Step 2: Publish with required fields only ---
    print("=" * 50)
    print("STEP 2: Publish UserUpdated (verplichte velden)")
    print("=" * 50)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        await sender.publish_user_updated({
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "email": "jan.janssen@example.com",
            "firstName": "Jan",
            "lastName": "Janssen",
            "role": "VISITOR",
            "isActive": True,
            "gdprConsent": True,
            "updatedAt": now,
        })
        print(f"PASS — Message published to crm.user.updated")
        print(f"  updatedAt: {now}\n")
    except Exception as e:
        print(f"FAIL — {e}\n")

    # --- Step 3: Publish with all fields (including address) ---
    print("=" * 50)
    print("STEP 3: Publish UserUpdated (alle velden + adres)")
    print("=" * 50)
    try:
        await sender.publish_user_updated({
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "email": "jan.janssen@example.com",
            "firstName": "Johan",
            "lastName": "Janssen",
            "phone": "+32 471 12 34 56",
            "role": "COMPANY_CONTACT",
            "companyId": "660e8400-e29b-41d4-a716-446655440001",
            "badgeCode": "BADGE-042",
            "street": "Kerkstraat",
            "houseNumber": "42",
            "postalCode": "1000",
            "city": "Brussel",
            "country": "BE",
            "isActive": True,
            "gdprConsent": True,
            "updatedAt": now,
        })
        print(f"PASS — Message published to crm.user.updated (all fields)")
        print(f"  Inclusief: phone, companyId, badgeCode, adres (5 velden)\n")
    except Exception as e:
        print(f"FAIL — {e}\n")

    # Cleanup
    await connection.close()
    print("=" * 50)
    print("Done. Check RabbitMQ Management UI voor de berichten:")
    print("  http://localhost:15672 → Queues → crm.user.updated")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
