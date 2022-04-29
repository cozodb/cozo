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