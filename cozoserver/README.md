# CozoServer

The standalone executable for Cozo can be downloaded from the [release page](https://github.com/cozodb/cozo/releases) 
(look for those with names `cozoserver-*`).

## Starting the server

Run the cozoserver command in a terminal:

```bash
./cozoserver <PATH_TO_DATA_DIRECTORY>
```

If `<PATH_TO_DATA_DIRECTORY>` does not exist, it will be created. 
Cozo will then start a web server and bind to address 127.0.0.1 and port 9070. 
These two can be customized: run the executable with the -h option to learn how.

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
