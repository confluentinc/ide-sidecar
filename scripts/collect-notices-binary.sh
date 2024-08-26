#!/bin/bash

# This script extracts from all jars in the specified directory the NOTICE files.
# It then concatenates all NOTICE files into a single NOTICE file in the specified output directory.
# Be aware, that it does not deduplicate contents.

set -Eeuo pipefail

SRC=$(realpath ${1:-.})
DST=$(realpath ${2:-.})

PWD=$(pwd)
TMP="${DST}/tmp"
DIR=$(dirname "$0")

USAGE="collect-notices-binary <SOURCE_DIRECTORY:-.> <OUTPUT_DIRECTORY:-.>"

if [ "${SRC}" = "-h" ]; then
	echo "${USAGE}"
	exit 0
fi

jars=( $(find -L "${SRC}" -name "*.jar") )
n_jars="${#jars[@]}"

echo "Found ${n_jars} jars in ${SRC}"

for ((i=0; i<n_jars; i++))
do
  jar="${jars[$i]}"
	DIR="${TMP}/$(basename -- "$jar" .jar)"
	mkdir -p "${DIR}"
	(cd "${DIR}" && jar xf ${jar} META-INF/NOTICE)
done

NOTICE="${DST}/NOTICE.txt"
TMP_NOTICE_BINARY="${TMP}/NOTICE-binary.txt"
NOTICE_BINARY="${DST}/NOTICE-binary.txt"


append_notice() {
  local notice_file="${1}"
  local jar_name="${2}"
  echo "Appending NOTICE file: ${notice_file}"

  echo -e "\n============ ${jar_name} ============\n" >> "${TMP_NOTICE_BINARY}"
  cat ${notice_file} >> "${TMP_NOTICE_BINARY}"
}

create_binary_notice() {
  [ -f "${TMP_NOTICE_BINARY}" ] && rm "${TMP_NOTICE_BINARY}"
  cp "${NOTICE}" "${TMP_NOTICE_BINARY}"

  notices=( $(find . -type f -name "NOTICE*" -mindepth 2 | sort) )
  n="${#notices[@]}"
  for ((i=0; i<n; i++))
  do
     notice_file="${notices[$i]}"
     jar_name=$(echo "${notice_file}" | cut -d'/' -f2)
     append_notice "${notice_file}" "${jar_name}"
  done
}

(cd "${TMP}" && create_binary_notice && cp "${TMP_NOTICE_BINARY}" "${NOTICE_BINARY}")

rm -r "${TMP}"
