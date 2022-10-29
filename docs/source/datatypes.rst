==============
Types
==============

--------------
Runtime types
--------------

Values in Cozo have the following *runtime types*:

* ``Null``
* ``Bool``
* ``Number``
* ``String``
* ``Bytes``
* ``Uuid``
* ``List``

``Number`` can be ``Float`` (double precision) or ``Int`` (signed, 64 bits). Cozo will auto-promote ``Int`` to ``Float`` when necessary.

``List`` can contain any number of mixed-type values, including other lists.

Cozo sorts values according to the above order, e.g. ``null`` is smaller than ``true``, which is in turn smaller than the list ``[]``.

Within each type values are *compared* according to:

* ``false < true``;
* ``-1 == -1.0 < 0 == 0.0 < 0.5 == 0.5 < 1 == 1.0``;
* Lists are ordered lexicographically by their elements;
* Bytes are compared lexicographically;
* Strings are compared lexicographically by their UTF-8 byte representations;
* UUIDs are sorted in a way that UUIDv1 with similar timestamps are near each other.
  This is to improve data locality and should be considered an implementation detail.
  Depending on the order of UUID in your application is not recommended.

.. WARNING::

    ``1 == 1.0`` evaluates to ``true``, but ``1`` and ``1.0`` are distinct values,
    meaning that a relation can contain both as keys according to set semantics.
    This is especially confusing when using JavaScript, which converts all numbers to float,
    and python, which does not show a difference between the two when printing.
    Using floating point numbers in keys is not recommended if the rows are accessed by these keys
    (instead of accessed by iteration).

----------------
Literals
----------------

The standard notations ``null`` for the type ``Null``, ``false`` and ``true`` for the type ``Bool`` are used.

Besides the usual decimal notation for signed integers,
you can prefix a number with ``0x`` or ``-0x`` for hexadecimal representation,
with ``0o`` or ``-0o`` for octal,
or with ``0b`` or ``-0b`` for binary.
Floating point numbers include the decimal dot (may be trailing),
and may be in scientific notation.
All numbers may include underscores ``_`` in their representation for clarity.
For example, ``299_792_458`` is the speed of light in meters per second.

Strings can be typed in the same way as they do in JSON using double quotes ``""``,
with the same escape rules.
You can also use single quotes ``''`` in which case the roles of double quotes and single quotes are switched.
There is also a "raw string" notation::

    ___"I'm a raw string"___

A raw string starts with an arbitrary number of underscores, and then a double quote.
It terminates when followed by a double quote and the same number of underscores.
Everything in between is interpreted exactly as typed, including any newlines.
By varying the number of underscores, you can represent any string without quoting.

There is no literal representation for ``Bytes`` or ``Uuid``.
Use the appropriate functions to create them.
If you are inserting data into a stored relation with a column specified to contain bytes or UUIDs,
auto-coercion will kick in and use ``decode_base64`` and ``to_uuid`` for conversion.

Lists are items enclosed between square brackets ``[]``, separated by commas.
A trailing comma is allowed after the last item.

------------------------------------------------
Column types
------------------------------------------------

The following *atomic types* can be specified for columns in stored relations:

* ``Int``
* ``Float``
* ``Bool``
* ``String``
* ``Bytes``
* ``Uuid``

There is no ``Null`` type. Instead, if you put a question mark after a type, it is treated as *nullable*,
meaning that it either takes value in the type or is null.

Two composite types are available. A *homogeneous list* is specified by square brackets,
with the inner type in between, like this: ``[Int]``.
You may optionally specify how many elements are expected, like this: ``[Int; 10]``.
A *heterogeneous list*, or a *tuple*, is specified by round brackets, with the element types listed by position,
like this: ``(Int, Float, String)``. Tuples always have fixed lengths.

A special type ``Any`` can be specified, allowing all values except null.
If you want to allow null as well, use ``Any?``.
Composite types may contain other composite types or ``Any`` types as their inner types.