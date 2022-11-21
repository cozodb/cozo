# `cozo-node`

[![cozo-node](https://img.shields.io/npm/v/cozo-node)](https://www.npmjs.com/package/cozo-node)

Embedded [CozoDB](https://github.com/cozodb/cozo) for NodeJS.

## Installation

```bash
npm install --save cozo-node
```

If that doesn't work because there are no precompiled binaries for your platform, 
scroll below to the building section.

## Usage

Refer to the main [docs](https://github.com/cozodb/cozo#nodejs).

## Building

Building `cozo-node` requires a [supported version of Node and Rust](https://github.com/neon-bindings/neon#platform-support).

Refer to the [script for linux](build_linux.sh), the [script for mac](build_mac.sh),
or the [script for windows](build_win.ps1) for the commands required.

After building, `npm install .` will install the package.

This project was bootstrapped by [create-neon](https://www.npmjs.com/package/create-neon).
To learn more about Neon, see the [Neon documentation](https://neon-bindings.com).
