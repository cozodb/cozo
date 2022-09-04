// fn parse_pull_spec(src: Pair<'_>) -> Result<JsonValue> {
//     let mut src = src.into_inner();
//     let name = src.next().unwrap().as_str();
//     let args: Vec<_> = src
//         .next()
//         .unwrap()
//         .into_inner()
//         .map(parse_pull_arg)
//         .try_collect()?;
//     Ok(json!({"pull": name, "spec": args}))
// }
