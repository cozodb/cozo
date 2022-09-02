# Functions

All functions in Cozo are stateless. 

All functions except those with names starting with `rand_` are deterministic.

## Comparisons

_Equality_ comparison is with `=` or `==`, _inequality_ with `!=`. The two arguments of the (in)equality can be of different types, in which case the result is false.

?> The unify operation `?var <- 1` is equivalent to `?var == 1` if `?var` is bound.

The _comparison operators_ `>`, `>=`, `<`, and `<=` can only compare values of the same value type, with specific logic for each [type](datatypes.md). Note that `Int` and `Float` are of the same value-type `Number`, as described in [datatypes](datatypes.md) (see also the caveat related to sorting therein).

`max` and `min` can only be applied to numbers and return the maximum/minimum found, e.g. `max(1, 2) == 2`, `max(1, 3.5, 2) == 3.5`. It is an error to call them on empty arguments.

## Basic arithmetics

The four _basic arithmetic operators_ `+`, `-`, `*`, and `/` do what you expect, with the usual operator precedence. The precedence can be overridden by inserting parentheses `(...)`.

`-` can also be used as a unary operator: `-(1) == -1`, `-(-1) = 1`.

`x ^ y` raises `x` to the power `y`. This always returns a float.

`x % y` returns the remainder when `x` is divided by `y`. Arguments can be floats. The returned value has the same sign as `x`.

## Boolean functions

## Maths functions

`add(...)`, `sub(x, y)`, `mul(...)`, `div(x, y)`: the function forms of `+`, `-`, `*`, `/`. `add` and `mul` can take multiple arguments (or no arguments).

`minus(x)`: the function form of `-(x)`.

`abs(x)`: returns the absolute value of the argument, preserves integer value, e.g. `abs(-1) = 1`.

`signum(x)`: returns `1`, `0` or `-1` which has the same sign as the argument, e.g. `signum(to_float('NEG_INFINITY')) == -1`, `signum(0.0) == 0`, but `signum(-0.0) == -1`. Will return `NAN` when applied to `NAN`.

`floor(x)` and `ceil(x)`: the floor and ceiling of the number passed in, e.g. `floor(1.5) == 1.0`, `floor(-3.4) == -4.0`, `ceil(-8.8) == -8.0`, `ceil(100) == 100`. Does not change the type of the argument.

`round(x)`: returns the nearest integer to the argument (represented as Float if the argument itself is a Float). Round halfway cases away from zero. E.g. `round(0.5) == 1.0`, `round(-0.5) == -1.0`, `round(1.4) == 1.0`.

`pow(x, y)`: power, same as `x ^ y`.

`mod(x, y)`: modulus, same as `x % y`.

`exp(x)`: returns the exponential base _e_ of the argument.

`exp2(x)`: returns the exponential base 2 of the argument. Always returns a float. E.g. `exp2(10) == 1024.0`.

`ln(x)`, `log2(x)`, `log10(x)`: returns thelogarithm, base _e_, 2 and 10 respectively, of the argument.

`sin(x)`, `cos(x)`, `tan(x)`: the sine, cosine, and tangent trigonometric functions.

`asin(x)`, `acos(x)`, `atan(x)`: the inverse functions to sine, cosine and tangent.

`atan2(x, y)`: the inverse tangent but passing `x` and `y` separately, c.f. [atan2 on Wikipedia](https://en.wikipedia.org/wiki/Atan2).

`sinh(x)`, `cosh(x)`, `tanh(x)`, `asinh(x)`, `acosh(x)`, `atanh(x)`: the hyperbolic sine, cosine, tangent and their inverses.

## Functions on strings

## Functions on lists

`list` constructs a list from its argument, e.g. `list(1, 2, 3)`. You may prefer to use the literal form `[1, 2, 3]`.

`is_in` tests the membership of an element in a list, e.g. `is_in(1, [1, 2, 3])` is true, whereas `is_in(5, [1, 2, 3])` is false.

?> The spread-unify operator `?var <- ..[1, 2, 3]` is equivalent to `is_in(?var, [1, 2, 3])` if `?var` is bound.

## Functions on bytes

## Random functions

## Type checking functions

## Explicit type conversions