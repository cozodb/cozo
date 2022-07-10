canonical form

```json
{
  "asserts": [
    [
      "temp_id",
      "Person/name",
      "Alice"
    ],
    [
      "temp_id",
      "Person/email",
      "alice@example.com"
    ],
    [
      {
        "Person/name": "Phillip"
      },
      "Person/friends",
      [
        {
          "Person/name": "Maxwells"
        },
        123332212
      ]
    ],
    {
      "_id": "tempxxx",
      "Person/name": "Bloopy",
      "Person/email": "b@example.com"
    }
  ],
  "retracts": [
    [
      1234567
    ],
    [
      {
        "Person/name": "Jack"
      }
    ]
  ]
}
```

```json
{
  "add_attrs": [],
  "amend_attrs": [],
  "retract_attrs": [],
  "commit_msg": "ZAODDK"
}
```

```json
{
  "q": {
    "ancestor": [
      "?a",
      "?c"
    ]
  },
  "ancestor": [
    {
      "out": [
        "?a",
        "?b"
      ],
      "where": [
        [
          "?a",
          "Person/parent",
          "?b"
        ]
      ]
    },
    {
      "out": [
        "?a",
        "?b"
      ],
      "where": [
        [
          "?a",
          "Person/parent",
          "?c"
        ],
        {
          "ancestor": [
            "?c",
            "?b"
          ]
        }
      ]
    }
  ]
}
```

```
attr {
    keyword: Person/parent,
    cardinality: many,
}.

attr {
    keyword: Person/name,
    type: string,
    index: identity
}.

Person { name: "Alice", parent_of: "Bob" }.
Person { name: "Bob" }.

Person/name("eve", "Eve").
Person/parent_of("eve", "Bob").

Ancestor(?a, ?b) :- Person/parent_of(?a, ?b).
Ancestor(?a, ?b) :- Person/parent_of(?a, ?c),
                    Ancestor(?c, ?b).
                    
?- Ancestor(Person/name("bob"), ?ancestor).
```