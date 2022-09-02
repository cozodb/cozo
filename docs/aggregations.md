# Aggregations

Aggregations in Cozo can be thought of as a function that acts on a string of values and produces a single value (the aggregate). Due to Datalog semantics, the stream is never empty.

There are two kinds of aggregations in Cozo, _meet aggregations_ and _normal aggregations_. Meet aggregations satisfy the additional properties of _idempotency_: the aggregate of a single value `a` is `a` itself, _commutivity_: the aggregate of `a` then `b` is equal to the aggregate of `b` then `a`, and _commutivity_: it is immaterial where we put the parentheses in an aggregate application. They are implemented differently in Cozo, with meet aggregations faster and more powerful (only meet aggregations can be recursive).

Meet aggregations can be used as normal ones, but the reverse is impossible.

## Meet aggregations



## Normal aggregations