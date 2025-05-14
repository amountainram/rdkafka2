#!/bin/bash

set -e;

source ${TEST_E2E_OUTPUT};
executables=$(echo "${executables}" | base64 -d);

for exe in $executables; do
  test_name=$(basename "$exe" | sed -r 's/-([^-]+)$//');
  demangled_supp_file="$supp_dir/${test_name}.demangled.supp";

  echo "";
  echo "----------------------------------------------------";
  echo "Running suppression checks for ${test_name}";

  if ! cmp -s "$demangled_supp_file" "./tests/suppressions/${test_name}.demangled.supp"; then
    echo -e "test ${test_name} valgrind suppression changed ... \033[31mfailed\033[0m";
    echo "actual:"
    cat "$demangled_supp_file"
    echo
    echo "-------------------------"
    echo "expected:"
    cat "./tests/suppressions/${test_name}.demangled.supp"
    exit 1
  else
    echo -e "test ${test_name} passed suppression check ... \033[32mok\033[0m";
  fi

  echo "----------------------------------------------------";
  echo "";
done
