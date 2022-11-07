# Cozo C lib

This directory contains the source of the Cozo C API.

For building, refer [here](../BUILDING.md).

The API is contained in this single [header file](./cozo_c.h).

An example for using the API is [here](./example.c).

To build and run the example:
```bash
gcc -L../target/release/ -lcozo_c example.c -o example && ./example
```
