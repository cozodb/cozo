# Aggregations

Aggregations in Cozo can be thought of as a function that acts on a string of values and produces a single value (the aggregate). Due to Datalog semantics, the stream is never empty.

There are two kinds of aggregations in Cozo, _meet aggregations_ and _normal aggregations_. Meet aggregations satisfy the additional properties of _idempotency_: the aggregate of a single value `a` is `a` itself, _commutivity_: the aggregate of `a` then `b` is equal to the aggregate of `b` then `a`, and _commutivity_: it is immaterial where we put the parentheses in an aggregate application. They are implemented differently in Cozo, with meet aggregations faster and more powerful (only meet aggregations can be recursive).

Meet aggregations can be used as normal ones, but the reverse is impossible.

## Meet aggregations

`min(x)`: aggregate the minimum value of all `x`.

`max(x)`: aggregate the maximum value of all `x`.

`and(var)`: aggregate the logical conjunction of the variable passed in.

`or(var)`: aggregate the logical disjunction of the variable passed in.

`union(var)`: aggregate the unions of `var`, which must be a list.

`intersection(var)`: aggregate the intersections of `var`, which must be a list.

`choice(var)`, `choice_last(var)`: non-deterministically chooses one of the values of `var` as the aggregate. The first one simply chooses the first value it meets (the order that it meets values should be considered non-deterministic), whereas the second one chooses the last value.

`min_cost(var)`: `var` should be a list of two elements: `[data, cost]`, and this aggregation chooses the list of the minimum `cost`.

`shortest(var)`: `var` must be a list. Returns the shortest list among all values. Ties will be broken non-deterministically.

`coalesce(var)`: returns the first non-null value it meets. The order is non-deterministic.

`bit_and(var)`: `var` must be bytes. Returns the bitwise 'and' of the values.

`bit_or(var)`: `var` must be bytes. Returns the bitwise 'or' of the values.

## Normal aggregations

`count(var)`: count how many values are generated for `var` (using bag instead of set semantics).

`count_unique(var)`: count how many unique values there are for `var`.

`collect(var)`: collect all values for `var` into a list.

`unique(var)`: collect `var` into a list, keeping each unique value only once.

`group_count(var)`: count the occurrence of unique values of `var`, putting the result into a list of lists, e.g. when applied to `'a'`, `'b'`, `'c'`, `'c'`, `'a'`, `'c'`, the results is `[['a', 2], ['b', 1], ['c', 3]]`.

`bit_xor(var)`: `var` must be bytes. Returns the bitwise 'xor' of the values.

### Statistical aggregations

`mean(x)`: the mean value of `x`.

`sum(x)`: the sum of `x`.

`product(x)`: the product of `x`.

`variance(x)`: the sample variance of `x`.

`std_dev(x)`: the sample standard deviation of `x`.
