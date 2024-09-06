NATIVE_EXECUTABLE=$(find target -name "*-runner")
artifact push workflow ${NATIVE_EXECUTABLE} --destination native-executables/$(basename ${NATIVE_EXECUTABLE}-${OS}-${ARCH})
make ci-bin-sem-cache-store
