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
* UUIDs are sorted in a way that UUIDv1 with similar timestamps are near each other. This is to improve data locality and should be considered an implementation detail. Depending on the order of UUID in your application is nor recommended.

.. WARNING::

    Because there are two internal number types ``Int`` and ``Float`` under the umbrella type ``Number``, sorting numbers can be more complex than anticipated.

    When sorting, the integer always comes before the equivalent float. For example, ``1.0 == 1``, ``1.0 >= 1`` and ``1.0 <= 1`` all evaluate to true, but when sorting ``1`` and ``1.0`` are two _different_ values and ``1`` is placed before ``1.0``.

    This may create problems when applying aggregations since if a grouping key contains both ``1.0`` and ``1``, they are treated as separate group headings. In such cases, it may help to use explicit coercion ``to_float`` or ``round`` to coerce all sorted values to the same type.


----------------
Value literals
----------------

``null`` for the type ``Null``, ``false`` and ``true`` for the type ``Bool`` are standard.

Numbers ...

Strings ...

There is no literal representation for ``Bytes`` due to restrictions placed by JSON. But ...

Lists ...

----------------
Schema types
----------------

In schema definition, the required type for a value can be specified by any of the following *schema-types*

* ``Ref``
* ``Int``
* ``Float``
* ``Bool``
* ``String``
* ``Bytes``
* ``List``
* ``Uuid``

When retrieving triples' values, the schema-type ``Ref``, is represented by the type ``Uuid``. The entity (the subject of the triple) is always a ``Ref``, always represented by a ``Uuid``.

Note the absence of the ``Null`` type in schema-types.

When asserting (inserting or updating) triples, if a value given is not of the correct schema-type, Cozo will first try to coerce the value and will only error out if no known coercion methods exist.
