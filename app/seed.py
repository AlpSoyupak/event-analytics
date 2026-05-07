"""Seed demo data: one tenant + synthetic events across 30 days.

Usage: docker-compose run --rm api python -m app.seed
"""

import asyncio
import random
import uuid
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = "http://api:8000/api/v1"

EVENT_TYPES = [
    "page_view", "button_click", "form_submit", "purchase",
    "signup", "login", "logout", "search", "add_to_cart", "checkout",
]

PAGES = ["/home", "/pricing", "/docs", "/blog", "/about", "/login", "/signup"]


async def main(api_key: str | None = None) -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30) as client:
        if api_key:
            print(f"Using existing API key: {api_key}\n")
        else:
            resp = await client.post("/tenants", json={"name": "Demo Corp", "plan": "pro"})
            resp.raise_for_status()
            tenant = resp.json()
            api_key = tenant["api_key"]
            print(f"Created tenant: {tenant['name']}")
            print(f"API Key:        {api_key}")
            print(f"Tenant ID:      {tenant['id']}\n")

        headers = {"X-API-Key": api_key}
        users = [str(uuid.uuid4()) for _ in range(50)]
        now = datetime.now(timezone.utc)

        total = 0
        for day_offset in range(30):
            day = now - timedelta(days=30 - day_offset)
            batch = []
            for _ in range(random.randint(50, 200)):
                user = random.choice(users)
                # Spread events across random hours within the day
                event_time = day + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                batch.append({
                    "event_type": random.choice(EVENT_TYPES),
                    "user_id": user,
                    "session_id": str(uuid.uuid4()),
                    "received_at": event_time.isoformat(),
                    "properties": {
                        "page": random.choice(PAGES),
                        "value": round(random.uniform(0, 500), 2),
                    },
                    "meta": {"ip": f"10.0.{random.randint(0, 255)}.{random.randint(0, 255)}"},
                })

            resp = await client.post("/events/batch", json={"events": batch}, headers=headers)
            resp.raise_for_status()
            total += len(batch)
            print(f"Day -{30 - day_offset:02d}: ingested {len(batch)} events")

        print(f"\nDone — {total} events seeded.")
        print("\nTry these commands:")
        print(f'  curl -H "X-API-Key: {api_key}" {BASE_URL}/analytics/summary')
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/timeseries?granularity=day"')
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/funnel?steps=page_view&steps=add_to_cart&steps=purchase"')


if __name__ == "__main__":
    import sys
    key = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main(key))
