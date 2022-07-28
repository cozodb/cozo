
if __name__ == '__main__':
    import json

    rules = [
        R.ancestor(["?a", "?b"],
                   T.parent("?a", "?b")),
        R.ancestor(["?a", "?b"],
                   T.parent("?a", "?c"),
                   R.ancestor("?c", "?b")),
        Q(["?a"],
          R.ancestor("?a", {"name": "Anne"}))
    ]
    print(json.dumps(rules, indent=2))
    rules = [
        Q.at("1990-01-01")(["?old_than_anne"],
                           T.age({"name": "Anne"}, "?anne_age"),
                           T.age("?older_than_anne", "?age"),
                           Gt("?age", "?anne_age"))
    ]
    print(json.dumps(rules, indent=2))
