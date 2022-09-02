# Functions

All function arguments in Cozo are immutable. 

All functions except those having names starting with `rand_` are deterministic.

## Comparisons

_Equality_ comparison is with `=` or `==`, _inequality_ with `!=`. The two arguments of the (in)equality can be of different types, in which case the result is false. They have the function forms `eq(x, y)` and `new(x, y)`.

?> The unify operation `?var <- 1` is equivalent to `?var == 1` if `?var` is bound.

The _comparison operators_ `>`, `>=`, `<`, and `<=` can only compare values of the same value type, with specific logic for each [type](datatypes.md). They have the function forms `gt(x, y)`, `ge(x, y)`, `lt(x, y)` and `le(x, y)`.

?> `Int` and `Float` are of the same value-type `Number`, as described in [datatypes](datatypes.md) (see also the caveat related to sorting therein).

`max` and `min` can only be applied to numbers and return the maximum/minimum found, e.g. `max(1, 2) == 2`, `max(1, 3.5, 2) == 3.5`. It is an error to call them on empty arguments.

## Basic arithmetics

The four _basic arithmetic operators_ `+`, `-`, `*`, and `/` do what you expect, with the usual operator precedence. The precedence can be overridden by inserting parentheses `(...)`.

`-` can also be used as a unary operator: `-(1) == -1`, `-(-1) = 1`.

`x ^ y` raises `x` to the power `y`. This always returns a float.

`x % y` returns the remainder when `x` is divided by `y`. Arguments can be floats. The returned value has the same sign as `x`.

## Boolean functions

`x && y`, `and(...)`: conjunction. The function form takes multiple arguments, with `and() == true`.

`x || y`, `or(...)`: disjunction. The function form takes multiple arguments, with `or() == false`.

`!x`, `negate(x)`: negation.

!> `negate(...)` is not the same as `not ...`, the former denotes the negation of a boolean expression, whereas the latter denotes the negation of a Horn clause.

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

`str_cat(...)` concatenates strings. Takes any number of arguments. The operator form `x ++ y` is also available for binary arguments.

`str_includes(x, y)` returns `true` if `x` contains the substring `y`, `false` otherwise.

`lowercase(x)`, `uppercase(x)`: returns the string with the corresponding case change. Supports Unicode.

`trim(x)`, `trim_start(x)`, `trim_end(x)`: removes whitespace from both ends / start / end of the string. "Whitespace" is defined by [Unicode](https://en.wikipedia.org/wiki/Whitespace_character).

`starts_with(x, y)`, `ends_with(x, y)`: tests if `x` starts / ends with `y`.

?> `starts_with(?var, str)` is prefered over equivalent (e.g. regex) conditions, since the compiler may more easily compile the clause into a range scan.

## Regex functions

`regex_matches(x, reg)`: tests if `x` matches the regular expression `reg`.

`regex_replace(x, reg, y)`: replaces the first occurrence of the pattern `reg` in `x` with `y`.

`regex_replace_all(x, reg, y)`: replaces all occurrences of the pattern `reg` in `x` with `y`.

`regex_extract(x, reg)`: extracts all occurrences of the pattern `reg` in `x` and returns them in a list.

`regex_extract_first(x, reg)`: extracts the first occurrence of the pattern `reg` in `x` and returns it. If none is found, returns `null`.

### Regex syntax

The following describes what is supported by the regex implementation used in Cozo.

#### Matching one character

```
.             any character except new line
\d            digit (\p{Nd})
\D            not digit
\pN           One-letter name Unicode character class
\p{Greek}     Unicode character class (general category or script)
\PN           Negated one-letter name Unicode character class
\P{Greek}     negated Unicode character class (general category or script)
```

#### Character classes

```
[xyz]         A character class matching either x, y or z (union).
[^xyz]        A character class matching any character except x, y and z.
[a-z]         A character class matching any character in range a-z.
[[:alpha:]]   ASCII character class ([A-Za-z])
[[:^alpha:]]  Negated ASCII character class ([^A-Za-z])
[x[^xyz]]     Nested/grouping character class (matching any character except y and z)
[a-y&&xyz]    Intersection (matching x or y)
[0-9&&[^4]]   Subtraction using intersection and negation (matching 0-9 except 4)
[0-9--4]      Direct subtraction (matching 0-9 except 4)
[a-g~~b-h]    Symmetric difference (matching `a` and `h` only)
[\[\]]        Escaping in character classes (matching [ or ])
```

#### Composites

```
xy    concatenation (x followed by y)
x|y   alternation (x or y, prefer x)
```

#### Repetitions

```
x*        zero or more of x (greedy)
x+        one or more of x (greedy)
x?        zero or one of x (greedy)
x*?       zero or more of x (ungreedy/lazy)
x+?       one or more of x (ungreedy/lazy)
x??       zero or one of x (ungreedy/lazy)
x{n,m}    at least n x and at most m x (greedy)
x{n,}     at least n x (greedy)
x{n}      exactly n x
x{n,m}?   at least n x and at most m x (ungreedy/lazy)
x{n,}?    at least n x (ungreedy/lazy)
x{n}?     exactly n x
```

#### Empty matches

```
^     the beginning of the text
$     the end of the text
\A    only the beginning of the text
\z    only the end of the text
\b    a Unicode word boundary (\w on one side and \W, \A, or \z on the other)
\B    not a Unicode word boundary
```

## Functions on lists

`list` constructs a list from its argument, e.g. `list(1, 2, 3)`. You may prefer to use the literal form `[1, 2, 3]`.

`is_in` tests the membership of an element in a list, e.g. `is_in(1, [1, 2, 3])` is true, whereas `is_in(5, [1, 2, 3])` is false.

?> The spread-unify operator `?var <- ..[1, 2, 3]` is equivalent to `is_in(?var, [1, 2, 3])` if `?var` is bound.

## Functions on bytes

`bit_and(x, y)`, `bit_or(x, y)`, `bit_not(x)`, `bit_xor(x, y)`: calculate the respective boolean functions on bytes regarded as bit arrays. The two bytes must have the same lengths.

`pack_bits([x, ...])` packs a list of booleans into a byte array; if the list is not divisible by 8, it is padded with `false`. `unpack_bits(x)` does the reverse. E.g. `unpack_bits(pack_bits([false, true, true])) == [false, true, true, false, false, false, false, false]`.

## Random functions

## Type checking functions

The usual ones: `is_null(x)`, `is_int(x)`, `is_float(x)`, `is_num(x)`, `is_bytes(x)`, `is_list(x)`, `is_string(x)`.

`is_finite(x)` returns `true` if `x` is `Int` or a finite `Float`.

`is_infinite(x)` returns `true` if `x` is infinity or negative infinity.

`is_nan(x)` returns `true` if `x` is the special float `NAN`

## Conversion functions