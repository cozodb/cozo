# Cozo WASM 库（浏览器）

可以在浏览器中运行的 Cozo WASM 库。NodeJS 用户请使用 [原生库](../cozo-lib-nodejs) ：速度更快，功能也更多。

本文叙述的是如何安装设置库本身。有关如何使用 CozoDB（CozoScript）的信息，见 [文档](https://docs.cozodb.org/zh_CN/latest/index.html) 。

安装

```
npm install cozo-lib-wasm
```

你也可以直接从 [发布页面](https://github.com/cozodb/cozo/releases) 下载 `cozo_wasm-<VERSION>-wasm32-unknown-unknown.zip` 文件，然后直接在你的网页代码中引用：见 [此处](https://rustwasm.github.io/docs/wasm-bindgen/examples/without-a-bundler.html) 的 `index.html` 范例。

## 使用

参考 [此文件](wasm-react-demo/src/App.js)。简单地说：

```js
import init, {CozoDb} from "cozo-lib-wasm";
```

然后：

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

    // 注意：通过此接口载入数据不会激活触发器
    import_relations(data: string): string;
}
```

注意所有的 API 都是同步的。如果你的查询需要比较长的时间返回，浏览器的主线程会被阻塞。阻塞浏览器主线程不是好事，因此在这种情况下你可以考虑在 web worker 中运行 Cozo WASM 模块。不过预编译的 WASM 模块不支持在有些浏览器的 web worker 中运行：见 [此页面](https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker#browser_compatibility) 的 "Support for ECMAScript
modules" 信息。

## 编译

编译需要 [Rust 工具链](https://rustup.rs/)，[NodeJS 与 npm](https://nodejs.org/)，再加上 [wasm-pack](https://github.com/rustwasm/wasm-pack)。

用以下命令来编译：

```bash
wasm-pack build --target web --release
```

建议编译时设置环境变量 `CARGO_PROFILE_RELEASE_LTO=fat` 使生成的库更快（以增加编译时间为代价）。

以上我们给出了参数 `--target web`：上面在浏览器中的使用例子只支持用此参数编译出的库。更多信息参见 [WASM 的文档](https://rustwasm.github.io/wasm-pack/book/commands/build.html#target)。

使用 `--target no-modules` 编译出的库可以在更多浏览器中的 web worker 运行，但是调用方式与上面给出的例子有区别，也更麻烦。详情见 [文档](https://rustwasm.github.io/wasm-bindgen/examples/wasm-in-web-worker.html) 。