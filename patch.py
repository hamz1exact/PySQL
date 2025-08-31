from faker import Faker

import random
fake = Faker()
# fake = Faker()
# Faker.seed(0)  # reproducible results

# def generate_database(n=50):
#     database = {"users": []}
#     for i in range(1, n+1):
#         user = {
#             "id": i,
#             "name": fake.name(),
#             "age": random.randint(18, 40),
#             "isStudent": random.choice([True, False]),
#             "email": fake.email(),
#             "city": fake.city(),
#             "country": fake.country()
#         }
#         database["users"].append(user)
#     return database

print(fake.phone_number())  