# Cozo WASM库

This crate provides Cozo web assembly modules for browsers.
If you are targeting NodeJS, use [this](../cozo-lib-nodejs) instead: 
native code is still _much_ faster than WASM.

This document describes how to set up the Cozo WASM module for use.
To learn how to use CozoDB (CozoScript), follow the [tutorial](https://github.com/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
first and then read the [manual](https://cozodb.github.io/current/manual/). You can run all the queries
described in the tutorial with an in-browser DB [here](https://cozodb.github.io/wasm-demo/).

## Installation

```
npm install cozo-lib-wasm
```

Alternatively, you can download `cozo_wasm-<VERSION>-wasm32-unknown-unknown.zip`
from the [release page](https://github.com/cozodb/cozo/releases) and include
the JS and WASM files directly in your project: see the `index.html` example 
[here](https://rustwasm.github.io/docs/wasm-bindgen/examples/without-a-bundler.html) for
what is required in your code.

## Usage

See the code [here](wasm-react-demo/src/App.js). Basically, you write

```js
import init, {CozoDb} from "cozo-lib-wasm";
```

and call

```js
let db;
init().then(() => {
    db = CozoDb.new();
    // db can only be used after the promise resolves 
})
```

## API

```ts
export class CozoDb {
    free(): void;

    static new(): CozoDb;

    run(script: string, params: string): string;

    export_relations(data: string): string;

    // Note that triggers are _not_ run for the relations, if any exists.
    // If you need to activate triggers, use queries with parameters.
    import_relations(data: string): string;
}
```

Note that this API is synchronous. If your computation runs for a long time, 
**it will block the main thread**. If you know that some of your queries are going to be heavy,
you should consider running Cozo in a web worker. However, the published module
may not work across browsers in web workers (look for the row "Support for ECMAScript
modules" [here](https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker#browser_compatibility)).

The next section contains some pointers for how to alleviate this, but expect a lot of work.

## Compiling

You will need to install [Rust](https://rustup.rs/), [NodeJS with npm](https://nodejs.org/),
and [wasm-pack](https://github.com/rustwasm/wasm-pack) first.

The published module was built with

```bash
wasm-pack build --target web --release
```

and the environment variable `CARGO_PROFILE_RELEASE_LTO=fat`.

The important option is `--target web`: the above usage instructions only work for this target.
See the documentation [here](https://rustwasm.github.io/wasm-pack/book/commands/build.html#target).

if you are interested in running Cozo in a web worker and expect it to run across browsers,
you will need to use the `--target no-modules` option, and write a lot of gluing code.
See [here](https://rustwasm.github.io/wasm-bindgen/examples/wasm-in-web-worker.html) for tips.