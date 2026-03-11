from locust import HttpUser, task

class ReviewUser(HttpUser):

    @task
    def get_reviews(self):
        self.client.get("/reviews")

    @task
    def analytics(self):
        self.client.get("/analytics")
