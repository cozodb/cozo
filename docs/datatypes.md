# Datatypes

A runtime value in Cozo can be of the following _value-types_:
* Null
* Bool
* Number
* String
* Bytes
* List

Number can be Float (double precision) or Int (signed, 64 bits). Cozo will do auto promotion from Int to Float when necessary.

List can contain any number of mixed-type values, including other lists.

Cozo defines a total order for all values according to the above order. Lists are ordered lexicographically by their elements. Strings are ordered lexicographically by their UTF-8 byte representation.

In schema definition, the required type for a value can be specified by any of the following _schema-types_

* Ref
* Component
* Int
* Float
* Bool
* String
* Bytes
* List

When retrieving values of triples, values of the first three schema-types (Ref, Component, Int) are all represented by the value-type Number (actually Int).

Note the absence of Null type in schema-types.

When asserting (inserting or updating) triples, if a value given is not of the correct schema-type, Cozo will first try to coerce the value and will only error out if no known coercion methods exist.