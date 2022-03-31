from faunadb import query as q
from faunadb.objects import Ref
from faunadb.client import FaunaClient

client = FaunaClient(secret="fnAEg5TK1FACUfENBIGx1m6hbz6LmLOQBsAex6OT")

for i in range(20):
    client.query(
        q.update(q.ref(q.collection("test"), i),{"data": {"key":i, "value": 0}}
    ))