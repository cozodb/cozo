# Tuning RocksDB for Cozo

RocksDB has a lot of options, and by tuning them you can achieve better performance
for your workload. This is probably unnecessary for 95% of users, but if you are the
remaining 5%, Cozo gives you the options to tune RocksDB directly if you are using the
RocksDB storage engine.

When you create the CozoDB instance with the RocksDB backend option, you are asked to
provide a path to a directory to store the data (will be created if it does not exist). 
If you put a file named `options` inside this directory, the engine will expect this
to be a [RocksDB options file](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File)
and use it. If you are using `cozoserver`, you will get a log message if
this feature is activated.

Note that improperly set options can make your database misbehave!
In general, you should run your database once, copy the options file from `data/OPTIONS-XXXXXX`
from within your database directory, and use that as a base for your customization. 
If you are not an expert on RocksDB, we suggest you limit your changes to adjusting those numerical 
options that you at least have a vague understanding.