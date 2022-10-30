==============
System ops
==============

.. module:: SysOp
    :noindex:

System ops start with a double-colon ``::`` and must appear alone in a script. 
In the following, we explain what each system op does, and the arguments they expect.

--------------
Explain
--------------

.. function:: ::explain { <QUERY> }

    A single query is enclosed in curly braces. Query options are allowed but ignored.
    The query is not executed, but its query plan is returned instead.
    Currently, there is no specification for the return format,
    but if you are familiar with the semi-naïve evaluation of stratified Datalog programs
    subject to magic-set rewrites, you can decipher the result.

----------------------------------
Ops for stored relations
----------------------------------

.. function:: ::relations

    List all stored relations in the database

.. function:: ::columns <REL_NAME>

    List all columns for the stored relation ``<REL_NAME>``.

.. function:: ::remove <REL_NAME> (, <REL_NAME>)*

    Remove stored relations. Several can be specified, joined by commas.

.. function:: ::rename <OLD_NAME> -> <NEW_NAME> (, <OLD_NAME> -> <NEW_NAME>)*

    Rename stored relation ``<OLD_NAME>`` into ``<NEW_NAME>``. Several may be specified, joined by commas.

.. function:: ::show_triggers <REL_NAME>

    Display triggers associated with the stored relation ``<REL_NAME>``.

.. function:: ::set_triggers <REL_NAME> ...

    Set triggers for the stored relation ``<REL_NAME>``. This is explained in more detail in the transaction chapter.

.. function:: ::access_level <ACCESS_LEVEL> <REL_NAME> (, <REL_NAME>)*

    Sets the access level of ``<REL_NAME>`` to the given level. The levels are:

    * ``normal`` allows everything,
    * ``protected`` disallows ``::remove`` and ``:replace``,
    * ``read_only`` additionally disallows any mutations and setting triggers,
    * ``hidden`` additionally disallows any data access (metadata access via ``::relations``, etc., are still allowed).

    The access level functionality is to protect data from mistakes of the programmer,
    not from attacks by malicious parties.

------------------------------------
Monitor and kill
------------------------------------

.. function:: ::running

    Display running queries and their IDs.

.. function:: ::kill <ID>

    Kill a running query specified by ``<ID>``. The ID may be obtained by ``::running``.

------------------------------------
Maintenance
------------------------------------

.. function:: ::compact

    Instructs Cozo to run a compaction job.
    Compaction makes the database smaller on disk and faster for read queries.