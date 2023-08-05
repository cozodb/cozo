# `cozo-node`

[![cozo-node](https://img.shields.io/npm/v/cozo-node)](https://www.npmjs.com/package/cozo-node)

Embedded [CozoDB](https://www.cozodb.org) for NodeJS.

This document describes how to set up the Cozo module for use in NodeJS.
To learn how to use CozoDB (CozoScript), read the [docs](https://docs.cozodb.org/en/latest/index.html).

## Installation

```bash
npm install --save cozo-node
```

If that doesn't work because there are no precompiled binaries for your platform,
scroll below to the building section.

## Usage

```javascript
const {CozoDb} = require('cozo-node')

const db = new CozoDb()

function printQuery(query, params) {
    return db.run(query, params)
        .then(data => console.log(data))
        .catch(err => console.error(err.display || err.message))
}

await printQuery("?[] <- [['hello', 'world!']]")
await printQuery("?[] <- [['hello', 'world', $name]]", {"name": "JavaScript"})
await printQuery("?[a] <- [[1, 2]]")
```

### Basic API

```ts
class CozoDb {
    /**
     * Constructor
     * 
     * @param engine:  defaults to 'mem', the in-memory non-persistent engine.
     *                 'sqlite', 'rocksdb' and maybe others are available,
     *                 depending on compile time flags.
     * @param path:    path to store the data on disk, defaults to 'data.db',
     *                 may not be applicable for some engines such as 'mem'
     * @param options: defaults to {}, ignored by all the engines in the published NodeJS artefact
     */
    constructor(engine: string, path: string, options: object): CozoDb;

    /**
     * You must call this method for any database you no longer want to use:
     * otherwise the native resources associated with it may linger for as
     * long as your program runs. Simply `delete` the variable is not enough.
     */
    close(): void;

    /**
     * Runs a query
     * 
     * @param script: the query
     * @param params: the parameters as key-value pairs, defaults to {}
     */
    async run(script: string, params: object): object;

    /**
     * Export several relations
     * 
     * @param relations:  names of relations to export, in an array.
     */
    async exportRelations(relations: Array<string>): object;

    /**
     * Import several relations.
     * 
     * Note that triggers are _not_ run for the relations, if any exists.
     * If you need to activate triggers, use queries with parameters.
     * 
     * @param data: in the same form as returned by `exportRelations`. The relations
     *              must already exist in the database.
     */
    async importRelations(data: object): object;

    /**
     * Backup database
     * 
     * @param path: path to file to store the backup.
     */
    async backup(path: string): object;

    /**
     * Restore from a backup. Will fail if the current database already contains data.
     * 
     * @param path: path to the backup file.
     */
    async restore(path: string): object;

    /**
     * Import several relations from a backup. The relations must already exist in the database.
     * 
     * Note that triggers are _not_ run for the relations, if any exists.
     * If you need to activate triggers, use queries with parameters.
     * 
     * @param path: path to the backup file.
     * @param rels: the relations to import.
     */
    async importRelationsFromBackup(path: string, rels: Array<string>): object;
}
```

More information are [here](https://docs.cozodb.org/en/latest/nonscript.html).

### Advanced API

There are API for multi-statement transactions, mutation callbacks and implementing custom fixed rules
for NodeJS, much like the [Python counterpart](https://github.com/cozodb/pycozo). If you are interested,
look at this [example](./example.js).

## Building

Building `cozo-node` requires a [Rust toolchain](https://rustup.rs). Run

```bash
cargo build --release -p cozo-node -F compact -F storage-rocksdb
```

and then find the dynamic library (names can vary a lot, the file extension is `.so` on Linux, `.dylib` on Mac,
and `.dll` on Windows) under the `../target/` folder (you may need to search for it).
Copy it to the file `native/6/cozo_node_prebuilt.node` under this directory (create intermediate directories if they don't exist).

If you did everything correctly, you should get the hello world message printed out when you run

```bash
node example.js
```

under this directory.
