from faunadb import query as q
from faunadb.objects import Ref
from faunadb.client import FaunaClient

# client = FaunaClient(secret="fnAEg5TK1FACUfENBIGx1m6hbz6LmLOQBsAex6OT")
client = FaunaClient(secret="fnAEg5XV2JACUd7Fi62BFHIKzEMQRWSXnjRcnmfn")
# client.query(
#     q.do(
#         q.create(
#             q.ref(q.collection("test"), "0"),
#             {"data": { "key": 0, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "1"),
#             {"data": { "key": 1, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "2"),
#             {"data": { "key": 2, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "3"),
#             {"data": { "key": 3, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "4"),
#             {"data": { "key": 4, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "5"),
#             {"data": { "key": 5, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "6"),
#             {"data": { "key": 6, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "7"),
#             {"data": { "key": 7, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "8"),
#             {"data": { "key": 8, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "9"),
#             {"data": { "key": 9, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "10"),
#             {"data": { "key": 10, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "11"),
#             {"data": { "key": 11, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "12"),
#             {"data": { "key": 12, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "13"),
#             {"data": { "key": 13, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "14"),
#             {"data": { "key": 14, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "15"),
#             {"data": { "key": 15, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "16"),
#             {"data": { "key": 16, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "17"),
#             {"data": { "key": 17, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "18"),
#             {"data": { "key": 18, "value": 0}}
#         ),
#         q.create(
#             q.ref(q.collection("test"), "19"),
#             {"data": { "key": 19, "value": 0}}
#         ),
#     )
# )

# # Create read and write function
# client.query(
#     q.create_function({
#         "name": "read",
#         "body": q.query(
#             q.lambda_("x", q.paginate(q.match(q.index("test"), q.var("x"))))
#         ),
#     })
# )
# client.query(
#     q.create_function({
#         "name": "write",
#         "body": q.query(
#             q.lambda_(["x", "y"], q.update(q.ref(q.collection("test"), q.var("x")),
#             {
#                 "data": {"value": q.var("y")}
#             })
#         )),
#     })
# )

result = client.query(
    q.call("write",[3,5])
)
print(result)
result = client.query(
    q.call("read",3)
)
print(result)
# a = "w"
# result = client.query(
#     q.if_(a=="w", q.call("write",[0,0]), q.call("read",3))
# )

# a = "w"
# b = "r"
# c = "w"
# result = client.query([
#         q.if_(a=="w", q.call("write",[0,3]), q.call("read",0)),
#         q.if_(b=="w", q.call("write",[0,2]), q.call("read",0)),
#         q.if_(c=="w", q.call("write",[0,0]), q.call("read",0))]
#     )
# result = client.query(
#     q.paginate(q.match(q.index("test"),11))
# )
# print(result)

