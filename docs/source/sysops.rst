==============
System ops
==============

.. module:: SysOp
    :noindex:

--------------
Explain
--------------

.. function:: ::explain { <query> }

----------------------------------
Ops on stored relations
----------------------------------

.. function:: ::relations

.. function:: ::relation columns <rel_name>

.. function:: ::relation remove <rel_name> (, <rel_name>)*

.. function:: ::relation rename <old_name> -> <new_name> (, <old_name> -> <new_name>)*

.. function:: ::relation set_triggers <ident> <triggers>

.. function:: ::relation show_triggers <ident>

------------------------------------
Monitor and kill
------------------------------------

.. function:: ::running

.. function:: ::kill <pid>

------------------------------------
Maintenance
------------------------------------

.. function:: ::compact