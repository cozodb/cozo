==============================
Algorithms
==============================

.. module:: Algo

.. function:: reorder_sort(rel[...], out: [...], sort_by: [...], descending: false, break_ties: false, skip: 0, take: 0)

    Sort and then extract new columns of the passed in relation ``rel``.

    :param required out: A list of expressions which will be used to produce the output relation. Any bindings in the expressions will be bound to the named positions in ``rel``.
    :param sort_by: A list of expressions which will be used to produce the sort keys. Any bindings in the expressions  will be bound to the named positions in ``rel``.
    :param descending: Whether the sorting process should be done in descending order. Defaults to ``false``.
    :param break_ties: Whether ties should be broken, e.g. whether the first two rows with *identical sort keys* should be given ordering numbers ``1`` and ``2`` instead of ``1`` and ``1``. Defaults to false.
    :param skip: How many rows to skip before producing rows. Defaults to zero.
    :param take: How many rows at most to produce. Zero means no limit. Defaults to zero.
    :return: The returned relation, in addition to the rows specified in the parameter ``out``, will have the ordering prepended. The ordering starts at ``1``.

.. TIP::

    This algorithm serves a similar purpose to the global ``:order``, ``:limit`` and ``:offset`` options, but can be applied to intermediate results. Prefer the global options if it is applied to the final output.