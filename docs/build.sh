#!/usr/bin/env bash
set -e

VERSION=$(cat ../Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')
DEST_DIR=../../cozodb_site/$VERSION

make html
touch build/html/.nojekyll
rm -fr $DEST_DIR
mkdir -p $DEST_DIR
mv build/html $DEST_DIR/manual

make latexpdf
mv build/latex/thecozodatabasemanual.pdf $DEST_DIR/manual.pdf

jupyter nbconvert --to html tutorial/tutorial.ipynb
mv tutorial/tutorial.html $DEST_DIR/