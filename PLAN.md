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

* `from(...rels)`, can use chain notation
* `join(left, right, ...conds)`
* `left_join(left, right, ...conds)`, similarly for `right_join`, `outer_join`)
* `intersect(...rels)`, similarly for `union`
* `diff(left, right)`, similarly for `sym_diff`
* `select(rel, binding: {..})`
* `where(rel, ..conds)`
* `take(rel, n)`
* `skip(rel, n)`
* `sort(rel, expr1, expr2: sort_dir)`
* `group(rel, binding: {*key1: expr1, val1: expr2})`
* `window(rel, ...)`, maybe various flavours
* `freeze(rel)`, disables structural optimization
* `materialize(rel)`, forces materialization
* `merge(...rels)`, concat data cols, key cols must be the same, if names clash last wins. Same for `left_merge`, `right_merge`, `outer_merge`
* `walk(pattern, ...conds, ...bindings)`
* `walk_repeat(pattern, ...conds, ...bindings)` every element contains additional `_iter` and `_visited` fields
* `values(data, ?Table)`
* `nested_values(data, ?Table).extract(Table)`
* `as(rel, Table)`
* `as_keyed_by(rel, Table)`
* `update(rel, Table)`
* `delete(rel, Table)`
* `insert(rel, Table)`
* `upsert(rel, Table)`

Helpers

* `print(rel)`
* `print_schema(rel)`
* `print_plan(rel)`
* `print_optimized(rel)`