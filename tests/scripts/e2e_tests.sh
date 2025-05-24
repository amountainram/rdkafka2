#!/bin/bash

set -e;

source ${TEST_E2E_OUTPUT};

cargo_args="--no-run --features=sasl,ssl --release --message-format=json"
executables=$(cargo test --tests $cargo_args \
  | jq -r 'select(.reason == "compiler-artifact" and .profile.test == true) | .executable' \
  | grep -v null \
  | grep -v '/rdkafka2-')
serialized_executables=`echo "${executables}" | base64 -w 0`;
echo "executables=${serialized_executables}" >> ${TEST_E2E_OUTPUT};

valgrind_args="--leak-check=full --suppressions=tests/suppressions/librdkafka.supp --gen-suppressions=all"

for exe in $executables; do
  echo "Running Valgrind on $exe with params $valgrind_args"

  test_name=$(basename "$exe" | sed -r 's/-([^-]+)$//');
  log_file="${log_dir}/${test_name}.valgrind.log";
  demangled_supp_file="${supp_dir}/${test_name}.demangled.supp";

  valgrind $valgrind_args --log-file=$log_file $exe --test-threads=1;

  cat $log_dir/${test_name}.valgrind.log \
    | sed -e "s/<insert_a_suppression_name_here>/${test_name}/" \
    | ./tests/scripts/suppressions.sh \
    | rustfilt \
    > $demangled_supp_file;
done
