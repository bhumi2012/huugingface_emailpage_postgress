import requests

BASE_URL = "http://localhost:8000"

# --- POST reviews ---
print("=== Posting Reviews ===")
sample_reviews = [
    {"email": "alice@gmail.com", "review": "Amazing food and great service"},
    {"email": "bob@company.org", "review": "Terrible experience, very slow"},
    {"email": "charlie@example.com", "review": "Decent place, average food"},
    {"email": "diana@mail.com", "review": "Best pizza I have ever had"},
    {"email": "eve@test.com", "review": "Good ambiance but overpriced"},
]

for data in sample_reviews:
    r = requests.post(f"{BASE_URL}/review", json=data)
    print(f"POST: {r.json()}")

# --- GET all reviews ---
print("\n=== All Reviews ===")
r = requests.get(f"{BASE_URL}/reviews")
for rev in r.json()["reviews"]:
    print(f"  [{rev['id']}] {rev['email']}: {rev['review']}")

# --- SEARCH reviews ---
print("\n=== Search: 'pizza' ===")
r = requests.get(f"{BASE_URL}/search", params={"q": "pizza"})
data = r.json()
print(f"  Found {data['matches']} match(es):")
for rev in data["results"]:
    print(f"  [{rev['id']}] {rev['email']}: {rev['review']}")

print("\n=== Search: 'great' ===")
r = requests.get(f"{BASE_URL}/search", params={"q": "great"})
data = r.json()
print(f"  Found {data['matches']} match(es):")
for rev in data["results"]:
    print(f"  [{rev['id']}] {rev['email']}: {rev['review']}")
