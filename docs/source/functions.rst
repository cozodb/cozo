=========
Functions
=========

Functions can be used to build expressions.

In the following, all functions except those having names starting with ``rand_`` are deterministic.

------------------------
Equality and Comparisons
------------------------

.. module:: Func.EqCmp
    :noindex:
    
.. function:: eq(x, y)

    Equality comparison. The operator form is ``x == y``. The two arguments of the equality can be of different types, in which case the result is ``false``.

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

    The four comparison operators can only compare values of the same runtime type. Integers and floats are of the same type ``Number``.

.. function:: max(x, ...)

    Returns the maximum of the arguments. Can only be applied to numbers.

.. function:: min(x, ...)

    Returns the minimum of the arguments. Can only be applied to numbers.

------------------------
Boolean functions
------------------------

.. module:: Func.Bool
    :noindex:
    
.. function:: and(...)

    Variadic conjunction. For binary arguments it is equivalent to ``x && y``.

.. function:: or(...)

    Variadic disjunction. For binary arguments it is equivalent to ``x || y``.

.. function:: negate(x)

    Negation. Equivalent to ``!x``.

.. function:: assert(x, ...)

    Returns ``true`` if ``x`` is ``true``, otherwise will raise an error containing all its arguments as the error message.

------------------------
Mathematics
------------------------

.. module:: Func.Math
    :noindex:
    
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

.. function:: ln(x)

    Returns the natual logarithm.

.. function:: log2(x)

    Returns the logarithm base 2.

.. function:: log10(x)

    Returns the logarithm base 10.

.. function:: sin(x)

    The sine trigonometric function.

.. function:: cos(x)

    The cosine trigonometric function.

.. function:: tan(x)

    The tangent trigonometric function.

.. function:: asin(x)

    The inverse sine.

.. function:: acos(x)

    The inverse cosine.

.. function:: atan(x)

    The inverse tangent.

.. function:: atan2(x, y)

    The inverse tangent `atan2 <https://en.wikipedia.org/wiki/Atan2>`_ by passing `x` and `y` separately.

.. function:: sinh(x)

    The hyperbolic sine.

.. function:: cosh(x)

    The hyperbolic cosine.

.. function:: tanh(x)

    The hyperbolic tangent.

.. function:: asinh(x)

    The inverse hyperbolic sine.

.. function:: acosh(x)

    The inverse hyperbolic cosine.

.. function:: atanh(x)

    The inverse hyperbolic tangent.

.. function:: deg_to_rad(x)

    Converts degrees to radians.

.. function:: rad_to_deg(x)

    Converts radians to degrees.

.. function:: haversine(a_lat, a_lon, b_lat, b_lon)

    Computes with the `haversine formula <https://en.wikipedia.org/wiki/Haversine_formula>`_
    the angle measured in radians between two points ``a`` and ``b`` on a sphere
    specified by their latitudes and longitudes. The inputs are in radians.
    You probably want the next function when you are dealing with maps,
    since most maps measure angles in degrees instead of radians.

.. function:: haversine_deg_input(a_lat, a_lon, b_lat, b_lon)

    Same as the previous function, but the inputs are in degrees instead of radians.
    The return value is still in radians.

    If you want the approximate distance measured on the surface of the earth instead of the angle between two points,
    multiply the result by the radius of the earth,
    which is about ``6371`` kilometres, ``3959`` miles, or ``3440`` nautical miles.

    .. NOTE::

        The haversine formula, when applied to the surface of the earth, which is not a perfect sphere, can result in an error of less than one percent.

------------------------
String functions
------------------------

.. module:: Func.String
    :noindex:

.. function:: length(str)

    Returns the number of Unicode characters in the string.

    Can also be applied to a list or a byte array.


    .. WARNING::

        ``length(str)`` does not return the number of bytes of the string representation.
        Also, what is returned depends on the normalization of the string.
        So if such details are important, apply ``unicode_normalize`` before ``length``.


.. function:: concat(x, ...)

    Concatenates strings. Equivalent to ``x ++ y`` in the binary case.

    Can also be applied to lists.

.. function:: str_includes(x, y)

    Returns ``true`` if ``x`` contains the substring ``y``, ``false`` otherwise.

.. function:: lowercase(x)

    Convert to lowercase. Supports Unicode.

.. function:: uppercase(x)

    Converts to uppercase. Supports Unicode.

.. function:: trim(x)

    Removes `whitespace <https://en.wikipedia.org/wiki/Whitespace_character>`_ from both ends of the string.

.. function:: trim_start(x)

    Removes `whitespace <https://en.wikipedia.org/wiki/Whitespace_character>`_ from the start of the string.

.. function:: trim_end(x)

    Removes `whitespace <https://en.wikipedia.org/wiki/Whitespace_character>`_ from the end of the string.

.. function:: starts_with(x, y)

    Tests if ``x`` starts with ``y``.

    .. TIP::

        ``starts_with(var, str)`` is preferred over equivalent (e.g. regex) conditions,
        since the compiler may more easily compile the clause into a range scan.

.. function:: ends_with(x, y)

    tests if ``x``  ends with ``y``.

.. function:: unicode_normalize(str, norm)

    Converts ``str`` to the `normalization <https://en.wikipedia.org/wiki/Unicode_equivalence>`_ specified by ``norm``.
    The valid values of ``norm`` are ``'nfc'``, ``'nfd'``, ``'nfkc'`` and ``'nfkd'``.

.. function:: chars(str)

    Returns Unicode characters of the string as a list of substrings.

.. function:: from_substrings(list)

    Combines the strings in ``list`` into a big string. In a sense, it is the inverse function of ``chars``.

    .. WARNING::

        If you want substring slices, indexing strings, etc., first convert the string to a list with ``chars``,
        do the manipulation on the list, and then recombine with ``from_substring``.

--------------------------
List functions
--------------------------

.. module:: Func.List
    :noindex:

.. function:: list(x, ...)

    Constructs a list from its argument, e.g. ``list(1, 2, 3)``. Equivalent to the literal form ``[1, 2, 3]``.

.. function:: is_in(el, list)

    Tests the membership of an element in a list.

.. function:: first(l)

    Extracts the first element of the list. Returns ``null`` if given an empty list.

.. function:: last(l)

    Extracts the last element of the list. Returns ``null`` if given an empty list.

.. function:: get(l, n)

    Returns the element at index ``n`` in the list ``l``. Raises an error if the access is out of bounds. Indices start with 0.

.. function:: maybe_get(l, n)

    Returns the element at index ``n`` in the list ``l``. Returns ``null`` if the access is out of bounds. Indices start with 0.

.. function:: length(list)

    Returns the length of the list.

    Can also be applied to a string or a byte array.

.. function:: slice(l, start, end)

    Returns the slice of list between the index ``start`` (inclusive) and ``end`` (exclusive).
    Negative numbers may be used, which is interpreted as counting from the end of the list.
    E.g. ``slice([1, 2, 3, 4], 1, 3) == [2, 3]``, ``slice([1, 2, 3, 4], 1, -1) == [2, 3]``.

.. function:: concat(x, ...)

    Concatenates lists. The binary case is equivalent to `x ++ y`.

    Can also be applied to strings.

.. function:: prepend(l, x)

    Prepends ``x`` to ``l``.

.. function:: append(l, x)

    Appends ``x`` to ``l``.

.. function:: reverse(l)

    Reverses the list.

.. function:: sorted(l)

    Sorts the list and returns the sorted copy.

.. function:: chunks(l, n)

    Splits the list ``l`` into chunks of ``n``, e.g. ``chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]``.

.. function:: chunks_exact(l, n)

    Splits the list ``l`` into chunks of ``n``, discarding any trailing elements, e.g. ``chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4]]``.

.. function:: windows(l, n)

    Splits the list ``l`` into overlapping windows of length ``n``. e.g. ``windows([1, 2, 3, 4, 5], 3) == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]``.

.. function:: union(x, y, ...)

    Computes the set-theoretic union of all the list arguments.

.. function:: intersection(x, y, ...)

    Computes the set-theoretic intersection of all the list arguments.

.. function:: difference(x, y, ...)

    Computes the set-theoretic difference of the first argument with respect to the rest.



----------------
Binary functions
----------------

.. module:: Func.Bin
    :noindex:

.. function:: length(bytes)

    Returns the length of the byte array.

    Can also be applied to a list or a string.

.. function:: bit_and(x, y)

    Calculate the bitwise and. The two bytes must have the same lengths.

.. function:: bit_or(x, y)

    Calculate the bitwise or. The two bytes must have the same lengths.

.. function:: bit_not(x)

    Calculate the bitwise not.

.. function:: bit_xor(x, y)

    Calculate the bitwise xor. The two bytes must have the same lengths.

.. function:: pack_bits([...])

    packs a list of booleans into a byte array; if the list is not divisible by 8, it is padded with ``false``.

.. function:: unpack_bits(x)

    Unpacks a byte array into a list of booleans.

.. function:: encode_base64(b)

    Encodes the byte array ``b`` into the `Base64 <https://en.wikipedia.org/wiki/Base64>`_-encoded string.

    .. NOTE::
        ``encode_base64`` is automatically applied when output to JSON since JSON cannot represent bytes natively.

.. function:: decode_base64(str)

    Tries to decode the ``str`` as a `Base64 <https://en.wikipedia.org/wiki/Base64>`_-encoded byte array.


--------------------------------
Type checking and conversions
--------------------------------

.. module:: Func.Typing
    :noindex:

.. function:: to_string(x)

    Convert ``x`` to a string: the argument is unchanged if it is already a string, otherwise its JSON string representation will be returned.

.. function:: to_float(x)

    Tries to convert ``x`` to a float. Conversion from numbers always succeeds. Conversion from strings has the following special cases in addition to the usual string representation:

    * ``INF`` is converted to infinity;
    * ``NEG_INF`` is converted to negative infinity;
    * ``NAN`` is converted to NAN (but don't compare NAN by equality, use ``is_nan`` instead);
    * ``PI`` is converted to pi (3.14159...);
    * ``E`` is converted to the base of natural logarithms, or Euler's constant (2.71828...).

.. function:: to_uuid(x)

    Tries to convert ``x`` to a UUID. The input must either be a hyphenated UUID string representation or already a UUID for it to succeed.

.. function:: uuid_timestamp(x)

    Extracts the timestamp from a UUID version 1, as seconds since the UNIX epoch. If the UUID is not of version 1, ``null`` is returned. If ``x`` is not a UUID, an error is raised.

.. function:: is_null(x)

    Checks for ``null``.

.. function:: is_int(x)

    Checks for integers.

.. function:: is_float(x)

    Checks for floats.

.. function:: is_finite(x)

    Returns ``true`` if ``x`` is an integer or a finite float.

.. function:: is_infinite(x)

    Returns ``true`` if ``x`` is infinity or negative infinity.

.. function:: is_nan(x)

    Returns ``true`` if ``x`` is the special float ``NAN``. Returns ``false`` when the argument is not of number type.

.. function:: is_num(x)

    Checks for numbers.

.. function:: is_bytes(x)

    Checks for bytes.

.. function:: is_list(x)

    Checks for lists.

.. function:: is_string(x)

    Checks for strings.

.. function:: is_uuid(x)

    Checks for UUIDs.

-----------------
Random functions
-----------------

.. module:: Func.Rand
    :noindex:

.. function:: rand_float()

    Generates a float in the interval [0, 1], sampled uniformly.

.. function:: rand_bernoulli(p)

    Generates a boolean with probability ``p`` of being ``true``.

.. function:: rand_int(lower, upper)

    Generates an integer within the given bounds, both bounds are inclusive.

.. function:: rand_choose(list)

    Randomly chooses an element from ``list`` and returns it. If the list is empty, it returns ``null``.

.. function:: rand_uuid_v1()

    Generate a random UUID, version 1 (random bits plus timestamp).

.. function:: rand_uuid_v4()

    Generate a random UUID, version 4 (completely random bits).

------------------
Regex functions
------------------

.. module:: Func.Regex
    :noindex:

.. function:: regex_matches(x, reg)

    Tests if ``x`` matches the regular expression ``reg``.

.. function:: regex_replace(x, reg, y)

    Replaces the first occurrence of the pattern ``reg`` in ``x`` with ``y``.

.. function:: regex_replace_all(x, reg, y)

    Replaces all occurrences of the pattern ``reg`` in ``x`` with ``y``.

.. function:: regex_extract(x, reg)

    Extracts all occurrences of the pattern ``reg`` in ``x`` and returns them in a list.

.. function:: regex_extract_first(x, reg)

    Extracts the first occurrence of the pattern ``reg`` in ``x`` and returns it. If none is found, returns ``null``.


^^^^^^^^^^^^^^^^^
Regex syntax
^^^^^^^^^^^^^^^^^

Matching one character::

    .             any character except new line
    \d            digit (\p{Nd})
    \D            not digit
    \pN           One-letter name Unicode character class
    \p{Greek}     Unicode character class (general category or script)
    \PN           Negated one-letter name Unicode character class
    \P{Greek}     negated Unicode character class (general category or script)

Character classes::

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

Composites::

    xy    concatenation (x followed by y)
    x|y   alternation (x or y, prefer x)

Repetitions::

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

Empty matches::

    ^     the beginning of the text
    $     the end of the text
    \A    only the beginning of the text
    \z    only the end of the text
    \b    a Unicode word boundary (\w on one side and \W, \A, or \z on the other)
    \B    not a Unicode word boundary


--------------------
Timestamp functions
--------------------

.. function:: now()

    Returns the current timestamp as seconds since the UNIX epoch.

.. function:: format_timestamp(ts, tz?)

    Interpret ``ts`` as seconds since the epoch and format as a string according to `RFC3339 <https://www.rfc-editor.org/rfc/rfc3339>`_.

    If a second string argument is provided, it is interpreted as a `timezone <https://en.wikipedia.org/wiki/Tz_database>`_ and used to format the timestamp.

.. function:: parse_timestamp(str)

    Parse ``str`` into seconds since the epoch according to RFC3339.