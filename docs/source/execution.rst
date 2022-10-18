====================================
Query execution
====================================

How queries are executed in a particular database
is usually considered an implementation detail hidden behind an abstraction layer,
which normal users should not care about.
As everyone who has used databases knows, however,
it is at best a leaky abstraction, since bad query execution plans entail
unacceptable performance characteristics,
and fighting, or "optimizing" the query optimizers is a difficult but necessary task.