from fastapi.testclient import TestClient
from app import app

client = TestClient(app)


def test_home():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["message"] == "API running"


def test_add_review():
    data = {
        "email": "test@gmail.com",
        "review": "This product is very good"
    }

    response = client.post("/review", json=data)

    assert response.status_code == 200
    body = response.json()

    assert body["status"] == "saved"
    assert "stars" in body
    assert "confidence" in body


def test_get_reviews():
    response = client.get("/reviews")

    assert response.status_code == 200
    body = response.json()

    assert "reviews" in body
    assert "total" in body


def test_search_reviews():
    response = client.get("/search?q=test")

    assert response.status_code == 200
    body = response.json()

    assert "results" in body
