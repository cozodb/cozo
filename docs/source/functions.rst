=========
Functions
=========

This page describes all functions that can be used in expressions in Cozo.

All function arguments in Cozo are immutable.

All functions except those having names starting with ``rand_`` are deterministic.

========================
Equality and Comparisons
========================

.. function:: eq(x, y)

    Equality comparison. The operator form is ``x == y`` or ``x = y``. The two arguments of the equality can be of different types, in which case the result is ``false``.

.. NOTE::

    The unify operation ``?var <- 1`` is equivalent to ``?var == 1`` if ``?var`` is bound.

.. function:: neq(x, y)

    Inequality comparison. The operator form is ``x != y``. The two arguments of the equality can be of different types, in which case the result is ``true``.

.. function:: gt(x, y)

    Equivalent to ``x > y``

.. function:: ge(x, y)

    Equivalent to ``x >= y``

.. function:: lt(x, y)

    Equivalent to ``x < y``

.. function:: le(x, y)

    Equivalent to ``x <= y``


.. NOTE::

    The four comparison operators can only compare values of the same value type. Integers and floats are of the same type ``Number``.

.. function:: max(x, ...)

    Returns the maximum of the arguments. Can only be applied to numbers.

.. function:: min(x, ...)

    Returns the minimum of the arguments. Can only be applied to numbers.

=================
Boolean functions
=================

.. function:: and(...)

    Variadic conjunction. For binary arguments it is equivalent to ``x && y``.

.. function:: or(..)

    Variadic disjunction. For binary arguments it is equivalent to ``x || y``.

.. function:: negate(x)

    Negation. Equivalent to ``!x``.

.. function:: assert(x, ...)

    Returns ``true`` if ``x`` is ``true``, otherwise will raise an error containing all its arguments as the error message.

=================
Mathematics
=================

.. function:: add(...)

    Variadic addition. The binary version is the same as ``x + y``.

.. function:: sub(x, y)

    Equivalent to ``x - y``.

.. function:: mul(...)

    Variadic multiplication. The binary version is the same as ``x * y``.

.. function:: div(x, y)

    Equivalent to ``x / y``.

.. function:: minus(x)

    Equivalent to ``-x``.

.. function:: pow(x, y)

    Raises ``x`` to the power of ``y``. Equivalent to ``x ^ y``. Always returns floating number.

.. function:: mod(x, y)

    Returns the remainder when ``x`` is divided by ``y``. Arguments can be floats. The returned value has the same sign as ``x``.  Equivalent to ``x % y``.

.. function:: abs(x)

    Returns the absolute value.

.. function:: signum(x)

    Returns ``1``, ``0`` or ``-1``, whichever has the same sign as the argument, e.g. ``signum(to_float('NEG_INFINITY')) == -1``, ``signum(0.0) == 0``, but ``signum(-0.0) == -1``. Returns ``NAN`` when applied to ``NAN``.

.. function:: floor(x)

    Returns the floor of ``x``.

.. function:: ceil(x)

    Returns the ceiling of ``x``.

.. function:: round(x)

    Returns the nearest integer to the argument (represented as Float if the argument itself is a Float). Round halfway cases away from zero. E.g. ``round(0.5) == 1.0``, ``round(-0.5) == -1.0``, ``round(1.4) == 1.0``.

.. function:: exp(x)

    Returns the exponential of the argument, natural base.

.. function:: exp2(x)

    Returns the exponential base 2 of the argument. Always returns a float.

.. function::ln(x)

    Returns the natual logarithm.

.. function::log2(x)

    Returns the logarithm base 2.

.. function::log10(x)

    Returns the logarithm base 10.

`sin(x)`, `cos(x)`, `tan(x)`: the sine, cosine, and tangent trigonometric functions.

`asin(x)`, `acos(x)`, `atan(x)`: the inverse functions to sine, cosine and tangent.

`atan2(x, y)`: the inverse tangent but passing `x` and `y` separately, c.f. [atan2 on Wikipedia](https://en.wikipedia.org/wiki/Atan2).

`sinh(x)`, `cosh(x)`, `tanh(x)`, `asinh(x)`, `acosh(x)`, `atanh(x)`: the hyperbolic sine, cosine, tangent and their inverses.

`deg_to_rad(x)`: converts degrees to radians.

`rad_to_deg(x)`: converts radians to degrees.

`haversine(a_lat, a_lon, b_lat, b_lon)`: returns the angle measured in radians between two points on a sphere specified by their latitudes and longitudes. The inputs are in radians. You probably want the next function since most maps measure angles in radians. See [Haversine formula](https://en.wikipedia.org/wiki/Haversine_formula) for more details.

`haversine_deg_input(a_lat, a_lon, b_lat, b_lon)`: same as the previous function, but the inputs are in degrees instead of radians. The return value is still in radians. If you want the approximate distance measured on the surface of the earth instead of the angle between two points, multiply the result by the radius of the earth, which is about `6371` kilometres, `3959` miles, or `3440` nautical miles.
