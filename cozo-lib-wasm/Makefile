.PHONY: help
help:
	@echo "Usage:"
	@echo "  make build    Build the project"
	@echo "  make publish  Publish the project into NPM"

.PHONY: build
build:
	wasm-pack build --target web --release
	echo "Copying js-bindings to pkg..."
	cp src/js/* pkg/
	echo "Update package.json..."
	node post_build.js

.PHONY: publish
publish:
	npm publish ./pkg --access public
