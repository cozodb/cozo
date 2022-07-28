import json

from cozopy import CozoDbPy


class CozoDb:
    def __init__(self, *args, **kwargs):
        self.inner = CozoDbPy(*args, **kwargs)

    def tx_attr(self, payload):
        return json.loads(self.inner.transact_attributes(json.dumps(payload, ensure_ascii=False)))

    def tx(self, payload):
        return json.loads(self.inner.transact_triples(json.dumps(payload, ensure_ascii=False)))

    def run(self, payload):
        return json.loads(self.inner.run_query(json.dumps(payload, ensure_ascii=False)))


if __name__ == '__main__':
    db = CozoDb('_test', destroy_on_exit=True)
    res = db.tx_attr({"attrs": [
        {"put": {"keyword": "person.idd", "cardinality": "one", "type": "string", "index": "identity",
                 "history": False}},
        {"put": {"keyword": "person.first_name", "cardinality": "one", "type": "string", "index": True}},
        {"put": {"keyword": "person.last_name", "cardinality": "one", "type": "string", "index": True}},
        {"put": {"keyword": "person.age", "cardinality": "one", "type": "int"}},
        {"put": {"keyword": "person.friend", "cardinality": "many", "type": "ref"}},
        {"put": {"keyword": "person.weight", "cardinality": "one", "type": "float"}},
        {"put": {"keyword": "person.covid", "cardinality": "one", "type": "bool"}},
    ]
    })
    print(res)
    print(db.tx_attr({
        "attrs": [
            {"put": {"id": res["results"][0][0], "keyword": ":person.id", "cardinality": "one", "type": "string",
                     "index": "identity", "history": False}},
            {"retract": {"id": res["results"][-1][0], "keyword": ":person.covid", "cardinality": "one", "type": "bool"}}
        ]
    }))
    print(db.tx({
        "tx": [
            {"put": {
                "_temp_id": "alice",
                "person.first_name": "Alice",
                "person.age": 7,
                "person.last_name": "Amorist",
                "person.id": "alice_amorist",
                "person.weight": 25,
                "person.friend": "eve"}},
            {"put": {
                "_temp_id": "bob",
                "person.first_name": "Bob",
                "person.age": 70,
                "person.last_name": "Wonderland",
                "person.id": "bob_wonderland",
                "person.weight": 100,
                "person.friend": "alice"
            }},
            {"put": {
                "_temp_id": "eve",
                "person.first_name": "Eve",
                "person.age": 18,
                "person.last_name": "Faking",
                "person.id": "eve_faking",
                "person.weight": 50,
                "person.friend": [
                    "alice",
                    "bob",
                    {
                        "person.first_name": "Charlie",
                        "person.age": 22,
                        "person.last_name": "Goodman",
                        "person.id": "charlie_goodman",
                        "person.weight": 120,
                        "person.friend": "eve"
                    }
                ]
            }},
            {"put": {
                "_temp_id": "david",
                "person.first_name": "David",
                "person.age": 7,
                "person.last_name": "Dull",
                "person.id": "david_dull",
                "person.weight": 25,
                "person.friend": {
                    "_temp_id": "george",
                    "person.first_name": "George",
                    "person.age": 7,
                    "person.last_name": "Geomancer",
                    "person.id": "george_geomancer",
                    "person.weight": 25,
                    "person.friend": "george"}}},
        ]
    }))
    res = db.run({
        "q": [
            {
                "rule": "ff",
                "args": [["?a", "?b"], ["?a", "person.friend", "?b"]]
            },
            {
                "rule": "ff",
                "args": [["?a", "?b"], ["?a", "person.friend", "?c"], {"rule": "ff", "args": ["?c", "?b"]}]
            },
            {
                "rule": "?",
                "args": [["?a"],
                         {"not_exists": ["?a", "person.last_name", "Goodman"]},
                         {"disj": [
                             {"pred": "Eq", "args": ["?n", {"pred": "StrCat", "args": ["A", "l", "i", "c", "e"]}]},
                             {"pred": "Eq", "args": ["?n", "Bob"]},
                             {"pred": "Eq", "args": ["?n", 12345]},
                         ]},
                         {"rule": "ff", "args": [{"person.id": "alice_amorist"}, "?a"]},
                         ["?a", "person.first_name", "?n"]
                         ]
            }
        ],
        "out": {"friend": {"pull": "?a", "spec": ["person.first_name"]}}
    })
    print(res)
