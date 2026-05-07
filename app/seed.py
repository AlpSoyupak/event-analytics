"""Seed demo data: one tenant + synthetic e-commerce events across 30 days.

Signals included:
  - Referrer / traffic source with per-source conversion multipliers (Level 2)
  - Device type (mobile / desktop / tablet)
  - order_id on purchases (basket analysis)
  - results_count on search events (catalog gap detection)
  - 4 controlled A/B experiments (Level 3):
      exp_001  smart_watch_price_discount
      exp_002  headphones_social_proof
      exp_003  yoga_mat_cross_sell_widget
      exp_004  coffee_maker_loyalty_email

Usage: docker-compose run --rm api python -m app.seed
"""

import asyncio
import random
import uuid
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = "http://api:8000/api/v1"

PRODUCTS = [
    {"id": "p001", "name": "Wireless Headphones",  "category": "Electronics",  "price": 79.99},
    {"id": "p002", "name": "Running Shoes",         "category": "Apparel",      "price": 124.99},
    {"id": "p003", "name": "Coffee Maker",          "category": "Kitchen",      "price": 49.99},
    {"id": "p004", "name": "Yoga Mat",              "category": "Fitness",      "price": 34.99},
    {"id": "p005", "name": "Desk Lamp",             "category": "Home Office",  "price": 39.99},
    {"id": "p006", "name": "Bluetooth Speaker",     "category": "Electronics",  "price": 59.99},
    {"id": "p007", "name": "Hiking Backpack",       "category": "Outdoors",     "price": 89.99},
    {"id": "p008", "name": "Protein Powder",        "category": "Health",       "price": 44.99},
    {"id": "p009", "name": "Sunglasses",            "category": "Apparel",      "price": 29.99},
    {"id": "p010", "name": "Smart Watch",           "category": "Electronics",  "price": 199.99},
]

PRODUCT_BY_ID = {p["id"]: p for p in PRODUCTS}

# ── Traffic sources ───────────────────────────────────────────────────────────
REFERRERS = ["search", "homepage_banner", "category_page", "email_campaign", "deals_page"]
REFERRER_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]
REFERRER_CART_BOOST = {
    "search":           1.80,
    "email_campaign":   2.00,
    "deals_page":       1.50,
    "category_page":    1.20,
    "homepage_banner":  0.65,
}

# ── Devices ───────────────────────────────────────────────────────────────────
DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.45, 0.10]

# ── Search queries with catalog-gap simulation ────────────────────────────────
SEARCH_TERMS = [
    {"query": "wireless headphones",  "results_count": 12, "related_id": "p001"},
    {"query": "running shoes",        "results_count": 8,  "related_id": "p002"},
    {"query": "coffee maker",         "results_count": 5,  "related_id": "p003"},
    {"query": "yoga mat",             "results_count": 6,  "related_id": "p004"},
    {"query": "smart watch",          "results_count": 3,  "related_id": "p010"},
    {"query": "hiking backpack",      "results_count": 4,  "related_id": "p007"},
    # catalog gaps — high demand, zero results
    {"query": "wireless earbuds",     "results_count": 0,  "related_id": None},
    {"query": "standing desk",        "results_count": 0,  "related_id": None},
    {"query": "air fryer",            "results_count": 0,  "related_id": None},
    {"query": "water bottle",         "results_count": 0,  "related_id": None},
    {"query": "resistance bands",     "results_count": 2,  "related_id": "p004"},
    {"query": "laptop stand",         "results_count": 1,  "related_id": "p005"},
]
# Weighted so gap queries appear often enough to be statistically meaningful
SEARCH_WEIGHTS = [3, 3, 3, 3, 3, 3, 5, 4, 5, 5, 2, 2]

PRODUCT_SEARCH_QUERY = {
    "p001": "wireless headphones", "p002": "running shoes",
    "p003": "coffee maker",        "p004": "yoga mat",
    "p005": "laptop stand",        "p006": "bluetooth speaker",
    "p007": "hiking backpack",     "p008": "protein powder",
    "p009": "sunglasses",          "p010": "smart watch",
}

PAGES = ["/home", "/products", "/categories", "/deals", "/about"]

# ── A/B Experiments ───────────────────────────────────────────────────────────
# cart_boost: multiplier on add-to-cart probability for treatment group
# bundle_product_id: if set, treatment group also sees (and may buy) this related product
EXPERIMENTS = [
    {
        "id": "exp_001", "name": "smart_watch_price_discount", "product_id": "p010",
        "control":   {"label": "original_price",        "price_override": None,   "cart_boost": 1.0, "bundle_product_id": None},
        "treatment": {"label": "discount_10pct",        "price_override": 179.99, "cart_boost": 2.8, "bundle_product_id": None},
    },
    {
        "id": "exp_002", "name": "headphones_social_proof", "product_id": "p001",
        "control":   {"label": "no_reviews",            "price_override": None, "cart_boost": 1.0, "bundle_product_id": None},
        "treatment": {"label": "show_47_reviews",       "price_override": None, "cart_boost": 3.2, "bundle_product_id": None},
    },
    {
        "id": "exp_003", "name": "yoga_mat_cross_sell_widget", "product_id": "p004",
        "control":   {"label": "no_widget",             "price_override": None, "cart_boost": 1.0, "bundle_product_id": None},
        # Treatment shows "frequently bought with Protein Powder" → boosts cart and adds bundle item
        "treatment": {"label": "show_related_products", "price_override": None, "cart_boost": 1.6, "bundle_product_id": "p008"},
    },
    {
        "id": "exp_004", "name": "coffee_maker_loyalty_email", "product_id": "p003",
        "control":   {"label": "no_email",              "price_override": None,  "cart_boost": 1.0, "bundle_product_id": None},
        # Treatment received a loyalty email with 10% discount
        "treatment": {"label": "loyalty_discount_email","price_override": 44.99, "cart_boost": 2.2, "bundle_product_id": None},
    },
]

_EXP_BY_PRODUCT = {e["product_id"]: e for e in EXPERIMENTS}


def _make_session_events(user: str, ts: datetime, referrer: str, device: str) -> list[dict]:
    """Simulate one user session with all Level-2 and Level-3 signals."""
    session  = str(uuid.uuid4())
    order_id = str(uuid.uuid4())   # all purchases in this session share one order
    events: list[dict] = []
    cart_boost = REFERRER_CART_BOOST[referrer]

    def ev(event_type: str, props: dict) -> dict:
        return {
            "event_type":  event_type,
            "user_id":     user,
            "session_id":  session,
            "received_at": ts.isoformat(),
            "properties":  {"device": device, **props},
        }

    # Always: page view
    landing = "/home" if referrer == "homepage_banner" else random.choice(PAGES)
    events.append(ev("page_view", {"page": landing, "referrer": referrer}))

    # ~80%: view 1–3 products
    if random.random() < 0.80:
        viewed = random.sample(PRODUCTS, k=random.randint(1, 3))
        viewed_with_meta: list[tuple[dict, dict | None]] = []  # (product, variant_cfg | None)

        for p in viewed:
            time_on_page = random.randint(15, 300)
            view_props: dict = {
                "product_id":   p["id"],
                "product_name": p["name"],
                "category":     p["category"],
                "price":        p["price"],
                "referrer":     referrer,
                "time_on_page": time_on_page,
            }
            if referrer == "search":
                view_props["search_query"] = PRODUCT_SEARCH_QUERY.get(p["id"], "product")

            variant_cfg: dict | None = None
            if p["id"] in _EXP_BY_PRODUCT:
                exp = _EXP_BY_PRODUCT[p["id"]]
                vk = "treatment" if random.random() < 0.50 else "control"
                variant_cfg = exp[vk]
                view_props.update({
                    "experiment_id":   exp["id"],
                    "experiment_name": exp["name"],
                    "variant":         variant_cfg["label"],
                })
                if variant_cfg["price_override"]:
                    view_props["price"] = variant_cfg["price_override"]

            events.append(ev("product_view", view_props))
            ts += timedelta(seconds=time_on_page + random.randint(10, 60))
            viewed_with_meta.append((p, variant_cfg))

        # Effective cart-add probability
        exp_boost = max((vc["cart_boost"] for _, vc in viewed_with_meta if vc), default=1.0)
        base_cart_prob = min(0.40 * cart_boost * exp_boost, 0.95)

        if random.random() < base_cart_prob:
            # At least one product goes to cart; others randomly
            cart_items: list[tuple[dict, dict | None]] = [
                (p, vc) for p, vc in viewed_with_meta if random.random() < 0.70
            ] or [viewed_with_meta[0]]

            # Cross-sell: treatment of exp_003 adds Protein Powder alongside Yoga Mat
            extra_bundles: list[tuple[dict, dict | None]] = []
            for p, vc in cart_items:
                if vc and vc.get("bundle_product_id") and random.random() < 0.65:
                    bundle = PRODUCT_BY_ID[vc["bundle_product_id"]]
                    if not any(bp["id"] == bundle["id"] for bp, _ in cart_items):
                        extra_bundles.append((bundle, None))
            cart_items.extend(extra_bundles)

            for p, vc in cart_items:
                qty = random.randint(1, 3)
                price = (vc["price_override"] if vc and vc.get("price_override") else None) or p["price"]
                cart_props: dict = {
                    "product_id":   p["id"],
                    "product_name": p["name"],
                    "category":     p["category"],
                    "price":        price,
                    "quantity":     qty,
                    "value":        round(price * qty, 2),
                    "referrer":     referrer,
                }
                if vc and p["id"] in _EXP_BY_PRODUCT:
                    exp = _EXP_BY_PRODUCT[p["id"]]
                    cart_props.update({"experiment_id": exp["id"], "experiment_name": exp["name"], "variant": vc["label"]})
                events.append(ev("add_to_cart", cart_props))
            ts += timedelta(minutes=random.randint(1, 3))

            # ~60% reach checkout
            if random.random() < 0.60:
                total = sum(
                    ((vc["price_override"] if vc and vc.get("price_override") else None) or p["price"])
                    * random.randint(1, 2)
                    for p, vc in cart_items
                )
                events.append(ev("checkout", {
                    "value":    round(total, 2),
                    "items":    len(cart_items),
                    "referrer": referrer,
                    "order_id": order_id,
                }))
                ts += timedelta(minutes=random.randint(1, 4))

                # ~70% complete purchase
                if random.random() < 0.70:
                    for p, vc in cart_items:
                        qty = random.randint(1, 2)
                        price = (vc["price_override"] if vc and vc.get("price_override") else None) or p["price"]
                        purchase_props: dict = {
                            "product_id":   p["id"],
                            "product_name": p["name"],
                            "category":     p["category"],
                            "price":        price,
                            "quantity":     qty,
                            "value":        round(price * qty, 2),
                            "referrer":     referrer,
                            "order_id":     order_id,
                        }
                        if vc and p["id"] in _EXP_BY_PRODUCT:
                            exp = _EXP_BY_PRODUCT[p["id"]]
                            purchase_props.update({"experiment_id": exp["id"], "experiment_name": exp["name"], "variant": vc["label"]})
                        events.append(ev("purchase", purchase_props))

    # ~25%: search (weighted toward gap queries)
    if random.random() < 0.25:
        term = random.choices(SEARCH_TERMS, weights=SEARCH_WEIGHTS, k=1)[0]
        events.append(ev("search", {
            "query":         term["query"],
            "results_count": term["results_count"],
            "referrer":      referrer,
        }))

    return events


async def main(api_key: str | None = None) -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30) as client:
        if api_key:
            print(f"Using existing API key: {api_key}\n")
        else:
            resp = await client.post("/tenants", json={"name": "Demo Shop", "plan": "pro"})
            resp.raise_for_status()
            tenant = resp.json()
            api_key = tenant["api_key"]
            print(f"Created tenant: {tenant['name']}")
            print(f"API Key:        {api_key}")
            print(f"Tenant ID:      {tenant['id']}\n")

        headers = {"X-API-Key": api_key}
        # 60 users reused across days to simulate returning customers
        users = [str(uuid.uuid4()) for _ in range(60)]
        now = datetime.now(timezone.utc)

        total = 0
        for day_offset in range(30):
            day = now - timedelta(days=30 - day_offset)
            batch: list[dict] = []

            sessions_today = random.randint(20, 80)
            for _ in range(sessions_today):
                user     = random.choice(users)
                referrer = random.choices(REFERRERS, weights=REFERRER_WEIGHTS, k=1)[0]
                device   = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
                session_start = day + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                batch.extend(_make_session_events(user, session_start, referrer, device))

            resp = await client.post("/events/batch", json={"events": batch}, headers=headers)
            resp.raise_for_status()
            total += len(batch)
            print(f"Day -{30 - day_offset:02d}: {sessions_today} sessions → {len(batch)} events")

        print(f"\nDone — {total} events seeded.")
        print("\nTry these commands:")
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/customers"')
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/search-gaps"')
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/basket"')
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/selling-times"')
        print(f'  curl -H "X-API-Key: {api_key}" "{BASE_URL}/analytics/experiments"')


if __name__ == "__main__":
    import sys
    key = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main(key))
