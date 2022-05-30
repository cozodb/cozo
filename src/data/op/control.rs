use crate::data::eval::{EvalError, PartialEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::value::Value;
use anyhow::Result;

pub(crate) const NAME_OP_COALESCE: &str = "~~";

pub(crate) fn row_eval_coalesce<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    args: &'a [Expr],
) -> Result<Value<'a>> {
    for arg in args {
        let arg = arg.row_eval(ctx)?;
        if arg != Value::Null {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

pub(crate) const IF_NAME: &str = "if";

pub(crate) fn partial_eval_coalesce<T: PartialEvalContext>(
    ctx: &T,
    args: Vec<Expr>,
) -> Result<Expr> {
    let mut collected = vec![];
    for arg in args {
        match arg.partial_eval(ctx)? {
            Expr::Const(Value::Null) => {}
            Expr::OpCoalesce(args) => {
                collected.extend(args);
            }
            expr => collected.push(expr),
        }
    }
    Ok(match collected.len() {
        0 => Expr::Const(Value::Null),
        1 => collected.pop().unwrap(),
        _ => Expr::OpCoalesce(collected),
    })
}

pub(crate) fn row_eval_if_expr<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    cond: &'a Expr,
    if_part: &'a Expr,
    else_part: &'a Expr,
) -> Result<Value<'a>> {
    let cond = cond.row_eval(ctx)?;
    match cond {
        Value::Bool(b) => Ok(if b {
            if_part.row_eval(ctx)?
        } else {
            else_part.row_eval(ctx)?
        }),
        Value::Null => Ok(Value::Null),
        v => Err(EvalError::OpTypeMismatch(IF_NAME.to_string(), vec![v.into_static()]).into()),
    }
}

pub(crate) fn partial_eval_if_expr<T: PartialEvalContext>(
    ctx: &T,
    cond: Expr,
    if_part: Expr,
    else_part: Expr,
) -> Result<Expr> {
    let cond = cond.partial_eval(ctx)?;
    match cond {
        Expr::Const(Value::Null) => Ok(Expr::Const(Value::Null)),
        Expr::Const(Value::Bool(b)) => Ok(if b {
            if_part.partial_eval(ctx)?
        } else {
            else_part.partial_eval(ctx)?
        }),
        cond => Ok(Expr::IfExpr(
            (
                cond,
                if_part.partial_eval(ctx)?,
                else_part.partial_eval(ctx)?,
            )
                .into(),
        )),
    }
}

pub(crate) fn row_eval_switch_expr<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    args: &'a [(Expr, Expr)],
) -> Result<Value<'a>> {
    let mut args = args.iter();
    let (expr, default) = args.next().unwrap();
    let expr = expr.row_eval(ctx)?;
    for (cond, target) in args {
        let cond = cond.row_eval(ctx)?;
        if cond == expr {
            return target.row_eval(ctx);
        }
    }
    default.row_eval(ctx)
}

pub(crate) fn partial_eval_switch_expr<T: PartialEvalContext>(
    ctx: &T,
    args: Vec<(Expr, Expr)>,
) -> Result<Expr> {
    let mut args = args.into_iter();
    let (expr, mut default) = args.next().unwrap();
    let expr = expr.partial_eval(ctx)?;
    let expr_evaluated = matches!(expr, Expr::Const(_));
    let mut collected = vec![];
    for (cond, target) in args {
        let cond = cond.partial_eval(ctx)?;
        if expr_evaluated && matches!(cond, Expr::Const(_)) {
            if cond == expr {
                default = target.partial_eval(ctx)?;
                break;
            } else {
                // cannot match, fall through
            }
        } else {
            collected.push((cond, target.partial_eval(ctx)?))
        }
    }
    if collected.is_empty() {
        Ok(default)
    } else {
        let mut args = vec![(expr, default)];
        args.extend(collected);
        Ok(Expr::SwitchExpr(args))
    }
}
