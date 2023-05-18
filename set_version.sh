#!/bin/bash
VERSION=$(echo $GITHUB_REF | cut -d / -f 3)
if [ -z "${VERSION}" ]; then
  VERSION=$(git tag | sort -V | grep '^v' | tail -n1)
fi
VERSION=$(echo $VERSION | sed 's/[^0-9.]//g')
sed -i "s/^version = .*/version = \"${VERSION}\"/" Cargo.toml
