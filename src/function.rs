// use std::fmt::{Debug, Formatter};
// use crate::typing::{Typing};
// use crate::value::{StaticValue, Value};
// // use lazy_static::lazy_static;
//
// #[derive(PartialEq, Debug)]
// pub struct Function {
//     pub args: Vec<Arg>,
//     pub var_arg: Option<Typing>,
//     pub ret_type: Typing,
//     pub fn_impl: FunctionImpl,
// }
//
// pub enum FunctionImpl {
//     Native(&'static str, for<'a> fn(&[Value<'a>]) -> Value<'a>),
//     UserDefined(()),
// }
//
// impl Debug for FunctionImpl {
//     fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }
//
// impl PartialEq for FunctionImpl {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (FunctionImpl::Native(a,_), FunctionImpl::Native(b,_)) => a == b,
//             (FunctionImpl::UserDefined(a), FunctionImpl::UserDefined(b)) => a == b,
//             (_, _) => false
//         }
//     }
// }
//
// #[derive(PartialEq, Debug)]
// pub struct Arg {
//     pub typing: Typing,
//     pub default_val: Option<StaticValue>,
//     pub name: Option<String>,
// }
//
// // lazy_static! {
// //     static ref BUILT_IN_FUNCTIONS : BTreeMap<&'static str, Function> = {
// //         let mut ret = BTreeMap::new();
// //
// //         fn add_int<'a>(_args: &[Value<'a>]) -> Value<'a> {
// //             todo!()
// //         }
// //
// //         fn add_float<'a>(_args: &[Value<'a>]) -> Value<'a> {
// //             todo!()
// //         }
// //
// //         ret.insert("_add_int",
// //             Function {
// //                 args: vec![],
// //                 var_arg: Some(Typing::Base(BaseType::Int)),
// //                 ret_type: Typing::Base(BaseType::Int),
// //                 fn_impl: FunctionImpl::Native("_add_int", add_int)
// //             });
// //
// //         ret.insert("_add_float",
// //             Function {
// //                 args: vec![],
// //                 var_arg: Some(Typing::Base(BaseType::Float)),
// //                 ret_type: Typing::Base(BaseType::Float),
// //                 fn_impl: FunctionImpl::Native("_add_float", add_float)
// //             });
// //
// //         ret
// //     };
// // }
