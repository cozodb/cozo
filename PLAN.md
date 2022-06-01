# Operations on relations

Operations

* [x] `from(...rels)`, can use chain notation
* [x] `left_join(left, right, ...conds)`, similarly for `right_join`, `outer_join`)
* [x] `concat(...rels)`
* [x] `intersect(...rels)`, similarly for `union`
* [x] `diff(left, right)`, similarly for `sym_diff`
* [x] `select(rel, binding: {..})`
* [x] `where(rel, ..conds)`
* [x] `take(rel, n)`
* [x] `skip(rel, n)`
* [x] `sort(rel, expr1, expr2: sort_dir)`
* [x] `group(rel, binding: {*key1: expr1, val1: expr2}, *ordering)` may order elements within groups
* [ ] `walk(pattern, ...conds, ...bindings)`
* [ ] `walk_repeat(pattern, ...conds, ...bindings)` every element contains additional `_iter` and `_visited` fields
* [x] `values(data, ?Table)`
* [x] `nested_values(data, ?Table).extract(Table)`
* [x] `update(rel, Table)`
* [x] `delete(rel, Table)`
* [x] `insert(rel, Table)`
* [x] `upsert(rel, Table)`

Helpers

* `print(rel)`
* `print_schema(rel)`
* `print_plan(rel)`
* `print_optimized(rel)`

Aggregation

* Aggregation functions should implement `.step()` and `.result()`

Differentiation

* function calls use parentheses, names start with lowercase letters or "_"
* aggregation calls are the same as function calls except that square brackets are used instead
* query calls are the same as function calls except that query names start with upper case or "#"

Others

* [ ] assoc magic better work
* [ ] language within query
* [ ] constraints and indices
* [ ] datetime and array types
* [ ] GUI and TUI
* [ ] query optimization
* [ ] regex as a value type

```
Walk(a:A-[e:E]->b:B,
     a => (a.id == 10),
     e => (e.id < 20, count[] <= 1, rand() => asc),
     b: {
        ...b,
        ...e,
        ...a
     })

WalkChain(a:A-[p:Path]->b:B,
          b => b._steps < 100)
WalkChainOverlapping(...)
```