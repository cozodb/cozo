# Cozo C lib

This directory contains the source of the Cozo C API.

For building, refer [here](../BUILDING.md).

The API is contained in this single [header file](./cozo_c.h).

An example for using the API is [here](./example.c).

To build and run the example:
```bash
gcc -L../target/release/ -lcozo_c example.c -o example && ./example
```

## Building for iOS

See [this guide](https://blog.mozilla.org/data/2022/01/31/this-week-in-glean-building-and-deploying-a-rust-library-on-ios/)
for detailed instructions on compilation for iOS.

All scripts are run from this directory.

For iOS devices:

```bash
ARCHS=arm64 ./comiple-ios.sh cozo_c release
```

For simulator on Apple ARM:
```bash
IS_SIMULATOR=1 ARCHS=arm64 ./comiple-ios.sh cozo_c release
```

For simulator on x86-64:
```bash
IS_SIMULATOR=1 ARCHS=x86_64 ./comiple-ios.sh cozo_c release
```

The libraries are then found in `../target` subdirectories. The static libraries can then be linked into
your iOS applications.