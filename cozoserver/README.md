# CozoServer

The standalone executable for Cozo can be downloaded from the [release page](https://github.com/cozodb/cozo/releases) 
(look for those with names `cozoserver-*`).

This document describes how to set up cozoserver.
To learn how to use CozoDB (CozoScript), follow
the [tutorial](https://nbviewer.org/github/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
first and then read the [manual](https://cozodb.github.io/current/manual/). You can run all the queries
described in the tutorial with an in-browser DB [here](https://cozodb.github.io/wasm-demo/).

## Starting the server

Run the cozoserver command in a terminal:

```bash
./cozoserver
```

This starts an in-memory, non-persistent database.
For more options such as how to run a persistent database with other storage engines,
see `./cozoserver -h`

To stop Cozo, press `CTRL-C`, or send `SIGTERM` to the process with e.g. `kill`.

## The query API

Queries are run by sending HTTP POST requests to the server. 
By default, the API endpoint is `http://127.0.0.1:9070/text-query`. 
A JSON body of the following form is expected:
```json
{
    "script": "<COZOSCRIPT QUERY STRING>",
    "params": {}
}
```
params should be an object of named parameters. For example, if params is `{"num": 1}`, 
then `$num` can be used anywhere in your query string where an expression is expected. 
Always use params instead of concatenating strings when you need parametrized queries.

The HTTP API always responds in JSON. If a request is successful, then its `"ok"` field will be `true`,
and the `"rows"` field will contain the data for the resulting relation, and `"headers"` will contain
the headers. If an error occurs, then `"ok"` will contain `false`, the error message will be in `"message"`
and a nicely-formatted diagnostic will be in `"display"` if available.

> Cozo is designed to run in a trusted environment and be used by trusted clients. 
> It does not come with elaborate authentication and security features. 
> If you must access Cozo remotely, you are responsible for setting up firewalls, encryptions and proxies yourself.
> 
> As a guard against users accidentally exposing sensitive data, 
> If you bind Cozo to non-loopback addresses, 
> Cozo will generate a token string and require all queries from non-loopback addresses 
> to provide the token string in the HTTP header field x-cozo-auth. 
> The warning printed when you start Cozo with a 
> non-default binding will tell you where to find the token string. 
> This “security measure” is not considered sufficient for any purpose 
> and is only intended as a last defence against carelessness.

## API

* `POST /text-query`, described above.
* `GET /export/{relations: String}`, where `relations` is a comma-separated list of relations to export. 
   The query parameter `as_objects` can change the output format.
* `PUT /import`, import data into the database. Data should be in `application/json` MIME type in the body,
   in the same format as returned in the `data` field in the `/export` API.
* `POST /backup`, backup database, should supply a JSON body of the form `{"path": <PATH>}`
* `POST /import-from-backup`, import data into the database from a backup. Should supply a JSON body 
   of the form `{"path": <PATH>, "relations": <ARRAY OF RELATION NAMES>}`.
* `GET /`, if you open this in your browser and open your developer tools, you will be able to use
   a very simple client to query this database.

## Building

Building `cozo-node` requires a [Rust toolchain](https://rustup.rs). Run

```bash
cargo build --release -p cozoserver -F compact -F storage-rocksdb
```
