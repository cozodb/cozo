# Structure of the project

* Parser
* Query AST
* Typing support
* Logical plan/optimizers
* Physical plan/optimizers
* Runtime representation (values)
* DB interaction/(de)serialization
* Evaluator/interpreter/VM
* Plan executors

```
{
    *id: id,
    name: d.name[0]
}
```

```
$name
${name:?Int}
```

```
where a: b.id == c.id
trail 
```

```
select ...
view X = select {} : Vertex
let z = select {} : Vertex
```

```
 from(e:Employee)
.where(e.id > 10)
.select(x: {id: e.id, name: e.first_name ++ e.last_name})
.skip(10)
.take(20);
```

```
 from(e:Employee-[hj:HasJob]->j:Job)
.where(e.id == 20)
.select({..j});

query DoThis(x:Int, y:Any) {
    return select(-[v:V]->)
          .where(v.id >= x, v.data == y);
}
```

```
query DoThat(r:Rel(*id: Int)) {
    ...
}
```

```
global query XXX() {
    ...
}
```

# Operations on relations

Operations

* [x] `from(...rels)`, can use chain notation
* [ ] `left_join(left, right, ...conds)`, similarly for `right_join`, `outer_join`)
* [ ] `intersect(...rels)`, similarly for `union`
* [ ] `diff(left, right)`, similarly for `sym_diff`
* [x] `select(rel, binding: {..})`
* [x] `where(rel, ..conds)`
* [x] `take(rel, n)`
* [x] `skip(rel, n)`
* [x] `sort(rel, expr1, expr2: sort_dir)`
* [ ] `group(rel, binding: {*key1: expr1, val1: expr2})`
* [ ] `walk(pattern, ...conds, ...bindings)`
* [ ] `walk_repeat(pattern, ...conds, ...bindings)` every element contains additional `_iter` and `_visited` fields
* [x] `values(data, ?Table)`
* [x] `nested_values(data, ?Table).extract(Table)`
* [ ] `update(rel, Table)`
* [ ] `delete(rel, Table)`
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