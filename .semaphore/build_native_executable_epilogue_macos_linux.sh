make store-test-results-to-semaphore
make ci-bin-sem-cache-store
make cache-docker-images
NATIVE_EXECUTABLE=$(find target -name "*-runner")
artifact push workflow ${NATIVE_EXECUTABLE} --destination native-executables/$(basename ${NATIVE_EXECUTABLE}-${OS}-${ARCH})
