#!/bin/bash
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

build_root=$(cd "$(dirname "$0")/.." && pwd)
cd $build_root

# -- C --
./tools/build.sh --run-unittests --run-e2e-tests --enable-nodejs-binding --enable-java-binding --enable-dotnet-core-binding --enable-java-remote-modules --enable-nodejs-remote-modules "$@" #-x
[ $? -eq 0 ] || exit $?
 