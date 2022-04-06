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
let vs with
[a:V]
[v:X]
[p:=z]
```