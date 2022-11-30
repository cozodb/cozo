# `cozo-node`

[![cozo-node](https://img.shields.io/npm/v/cozo-node)](https://www.npmjs.com/package/cozo-node)

Embedded [CozoDB](https://github.com/cozodb/cozo) for NodeJS.

This document describes how to set up the Cozo module for use in NodeJS.
To learn how to use CozoDB (CozoScript), follow
the [tutorial](https://nbviewer.org/github/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
first and then read the [manual](https://cozodb.github.io/current/manual/). You can run all the queries
described in the tutorial with an in-browser DB [here](https://cozodb.github.io/wasm-demo/).

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
    db.run(query, params)
        .then(data => console.log(data))
        .catch(err => console.error(err.display || err.message))
}

printQuery("?[] <- [['hello', 'world!']]")
printQuery("?[] <- [['hello', 'world', $name]]", {"name": "JavaScript"})
printQuery("?[a] <- [[1, 2]]")
```

### API

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
     * Import several relations
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
     * @param path: path to the backup file.
     * @param rels: the relations to import.
     */
    async importRelationsFromBackup(path: string, rels: Array<string>): object;
}
```

## Building

Building `cozo-node` requires a [Rust toolchain](https://rustup.rs). Run

```bash
cargo build --release -p cozo-node -F compact -F storage-rocksdb
```

and then find the dynamic library (names can vary a lot, the file extension is `.so` on Linux, `.dylib` on Mac,
and `.dll` on Windows) under the `../target/` folder (you may need to search for it).
Copy it to the file `native/6/index.node` under this directory (create intermediate directories if they don't exist).

If you did everything correctly, you should get the hello world message printed out when you run

```bash
node example.js
```

under this directory.