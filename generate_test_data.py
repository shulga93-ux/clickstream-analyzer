"""Generate realistic test clickstream CSV data."""
import csv
import random
from datetime import datetime, timedelta

random.seed(42)

pages = ["/", "/catalog", "/product/123", "/product/456", "/product/789",
         "/cart", "/checkout", "/checkout/payment", "/checkout/success",
         "/search", "/account", "/account/orders", "/blog", "/about"]

events = ["page_view", "page_view", "page_view", "click", "click",
          "add_to_cart", "search", "scroll", "form_submit", "checkout_start"]

users = [f"user_{i:04d}" for i in range(1, 201)]

rows = []
base = datetime(2024, 3, 1, 6, 0, 0)

for day in range(7):
    # Normal traffic pattern
    for hour in range(24):
        # More traffic during day
        if 9 <= hour <= 21:
            volume = random.randint(30, 80)
        elif 22 <= hour or hour <= 7:
            volume = random.randint(2, 15)
        else:
            volume = random.randint(15, 35)

        # Inject anomaly: spike on day 3 at 14:00
        if day == 3 and hour == 14:
            volume = random.randint(250, 320)

        # Inject drop on day 5 at 18:00
        if day == 5 and hour == 18:
            volume = random.randint(1, 3)

        for _ in range(volume):
            user = random.choice(users)
            session_id = f"sess_{user}_{day}_{random.randint(1,3)}"
            ts = base + timedelta(days=day, hours=hour,
                                   minutes=random.randint(0, 59),
                                   seconds=random.randint(0, 59))
            rows.append({
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": user,
                "session_id": session_id,
                "event_type": random.choice(events),
                "page": random.choice(pages),
                "duration": random.randint(1, 300),
            })

# Add a bot user (anomalous)
bot_day = 2
for i in range(500):
    ts = base + timedelta(days=bot_day, hours=random.randint(0, 23),
                           minutes=random.randint(0, 59))
    rows.append({
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": "user_bot_001",
        "session_id": "sess_bot_001",
        "event_type": "page_view",
        "page": random.choice(pages),
        "duration": random.randint(0, 2),
    })

rows.sort(key=lambda r: r["timestamp"])

with open("test_data.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["timestamp","user_id","session_id","event_type","page","duration"])
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} events → test_data.csv")
