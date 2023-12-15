const fs = require("fs");
const path = require("path");

const pkgPath = path.join(__dirname, "pkg", "package.json");
const pkg = require(pkgPath);

if (!pkg.files) {
  pkg.files = [];
}
pkg.files.push("indexeddb.js");

fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2));
