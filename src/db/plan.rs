// struct Filter;
//
// enum QueryPlan {
//     Union {
//         args: Vec<QueryPlan>
//     },
//     Intersection {
//         args: Vec<QueryPlan>
//     },
//     Difference {
//         left: Box<QueryPlan>,
//         right: Box<QueryPlan>,
//     },
//     Selection {
//         arg: Box<QueryPlan>,
//         filter: (),
//     },
//     Projection {
//         arg: Box<QueryPlan>,
//         keys: (),
//         fields: (),
//     },
//     Product {
//         args: Vec<QueryPlan>
//     },
//     Join {
//         args: Vec<QueryPlan>
//     },
//     LeftJoin {
//         left: Box<QueryPlan>,
//         right: Box<QueryPlan>
//     },
//     BaseRelation {
//         relation: ()
//     },
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Parser, Rule};
    use pest::Parser as PestParser;

    #[test]
    fn parse_patterns() {
        let s = "from a:Friend, (b:Person)-[:Friend]->(c:Person), x:GGG";
        let parsed = Parser::parse(Rule::from_pattern, s).unwrap().next().unwrap();
        assert_eq!(parsed.as_rule(), Rule::from_pattern);
        println!("{:#?}", parsed);
    }
}