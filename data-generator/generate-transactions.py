import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

NUM_TRANSACTIONS = 1000000

operators = ["MTN", "MOOV", "ORANGE"]
cities = ["Cotonou", "Porto-Novo", "Parakou", "Abomey", "Bohicon"]
transaction_types = ["deposit", "withdraw", "transfer"]

data = []

for i in range(NUM_TRANSACTIONS):

    transaction = {
        "transaction_id": i,
        "user_id": random.randint(1000, 50000),
        "operator": random.choice(operators),
        "transaction_type": random.choice(transaction_types),
        "amount": random.randint(100, 2000000),
        "city": random.choice(cities),
        "timestamp": fake.date_time_between(start_date="-1y", end_date="now"),
        "device_id": fake.uuid4()
    }

    data.append(transaction)

df = pd.DataFrame(data)


# Ajout de montants négatifs (erreurs)
for i in random.sample(range(NUM_TRANSACTIONS), 500):
    df.loc[i, "amount"] = -random.randint(100, 1000)

# Ajout de valeurs nulles
for i in random.sample(range(NUM_TRANSACTIONS), 500):
    df.loc[i, "city"] = None



duplicates = df.sample(1000)

df = pd.concat([df, duplicates], ignore_index=True)



for i in random.sample(range(len(df)), 500):

    df.loc[i, "amount"] = random.randint(3000000, 10000000)




df.to_csv("transactions_dataset.csv", index=False)

print("Dataset généré avec succès")
print("Nombre total de lignes :", len(df))
