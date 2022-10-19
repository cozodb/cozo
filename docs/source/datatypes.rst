==============
Datatypes
==============

--------------
Value types
--------------

A runtime value in Cozo can be of the following *value-types*:

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

Within each type values are *compared* according to logic custom to each type:

* ``false < true``;
* ``-1 == -1.0 < 0 == 0.0 < 0.5 == 0.5 < 1 == 1.0`` (however, see the caveat below);
* Lists are ordered lexicographically by their elements;
* Bytes are compared lexicographically;
* Strings are ordered lexicographically by their UTF-8 byte representations.
* UUIDs are sorted in a way that UUIDv1 with similar timestamps are near each other. This is to improve data locality and should be considered an implementation detail. Depending on the order of UUID in your application is not recommended.

.. WARNING::

    Because there are two internal number types ``Int`` and ``Float`` under the umbrella type ``Number``, sorting numbers can be more complex than anticipated.

    When sorting, the integer always comes before the equivalent float. For example, ``1.0 == 1``, ``1.0 >= 1`` and ``1.0 <= 1`` all evaluate to true, but when sorting ``1`` and ``1.0`` are two _different_ values and ``1`` is placed before ``1.0``.

    This may create problems when applying aggregations since if a grouping key contains both ``1.0`` and ``1``, they are treated as separate group headings. In such cases, it may help to use explicit coercion ``to_float`` or ``round`` to coerce all sorted values to the same type.


----------------
Value literals
----------------

The standard notations ``null`` for the type ``Null``, ``false`` and ``true`` for the type ``Bool`` are followed.

Besides the usual decimal notation for signed integers,
you can prefix a number with ``0x`` or ``-0x`` for hexadecimal notation,
with ``0o`` or ``-0o`` for octal notation,
or with ``0b`` or ``-0b`` for binary notation.
Floating point numbers include the decimal dot, which may be trailing,
and may be in scientific notation.
All numbers may include underscores ``_`` in their representation for clarity.
For example, ``299_792_458`` is the speed of light in meters per second.

Strings can be typed in the same way as they do in JSON between double quotes ``""``,
with the same escape rules.
You can also use single quotes ``''`` in which case the roles of the double quote and single quote are switched.
In addition, there is a raw string notation::

    r___"I'm a raw string with "quotes"!"___

A raw string starts with the letter ``r`` followed by an arbitrary number of underscores, and then a double quote.
It terminates when followed by a double quote and the same number of underscores.
Everything in between is interpreted exactly as typed, including any newlines.

There is no literal representation for ``Bytes`` or ``Uuid`` due to restrictions placed by JSON.
You must pass in its Base64 encoding for bytes, or hyphened strings for UUIDs,
and use the appropriate functions to decode it.
If you are just inserting data into a stored relation with a column specified to contain bytes or UUIDs,
auto-coercion will kick in.

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