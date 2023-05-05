#!/bin/bash
#set -x
uname -s | grep -q Darwin && TOOL=xcrun
RUSTFLAGS="-C instrument-coverage" cargo test --tests 2>&1 | tee coverage.out
OBJECT_PATH=$(cat coverage.out | sed -n '/Running unittests/{s/.*(//;s/).*//p}')
$TOOL llvm-profdata merge -sparse default_*.profraw -o pto.profdata
$TOOL llvm-cov report \
    --use-color --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=pto.profdata \
    --object ${OBJECT_PATH}
echo $TOOL llvm-cov show \
        --use-color --ignore-filename-regex=\'/.cargo/registry\' \
        --instr-profile=pto.profdata \
        --object ${OBJECT_PATH} \
        --show-instantiations --show-line-counts-or-regions \
        --Xdemangler=rustfilt \| less
