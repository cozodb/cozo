# Building on Windows

* Refer to https://github.com/facebook/rocksdb/wiki/Building-on-Windows, with the following changes.
* Build zlib using cmake instead of whatever was in the above instruction
  * Copy `zconf.h` to the base directory after building
* Edit `thirdparty.inc` appropriately
* `cmake -G "Visual Studio 16 2019" -A x64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=20 -DGFLAGS=1 -DLZ4=1 -DZSTD=1 -DUSE_RTTI=1 ..`
* `msbuild rocksdb.sln /m /p:Configuration=Release`
* Need to copy some dll files so that they could be found