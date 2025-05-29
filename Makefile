# Intentionally using the project's local settings.xml for all builds
# and to use the breadth-first dependency collector which is able to fetch dependency POMs in parallel,
# greatly speeding up cold cache build times
# 
MAVEN_ARGS += --settings .mvn/settings.xml -Daether.dependencyCollector.impl=bf 

include ./mk-files/begin.mk
include ./mk-files/semver.mk
include ./mk-files/semaphore.mk
include ./mk-files/release.mk
include ./mk-files/maven.mk

IDE_SIDECAR_VERSION_FILE := .versions/ide-sidecar-version.txt
IDE_SIDECAR_VERSION := $(shell cat $(IDE_SIDECAR_VERSION_FILE))

.PHONY: build
build:
	$(MAKE) mvn-install

.PHONY: test
test:
	$(MAKE) mvn-verify MAVEN_VERIFY_OPTS="$(if $(TEST),-Dtest=$(TEST),)"

.PHONY: test-native
test-native:
	$(MAKE) mvn-verify-native-executable

.PHONY: clean
clean:
	$(MAKE) mvn-clean

IDE_SIDECAR_SCRIPTS := $(CURDIR)/scripts
IDE_SIDECAR_STATIC_RESOURCE_DIR := $(CURDIR)/src/main/resources/static
GIT_REMOTE_NAME := origin

# Sign and notarize the macOS executable as documented on https://confluentinc.atlassian.net/wiki/spaces/DTX/pages/3541959564/Signing+and+notarizing+native+executables.
# This command requires a working Vault session and must be executed in the CI pipeline.
.PHONY: ci-sign-notarize-macos-executable
ci-sign-notarize-macos-executable:
ifeq ($(CI),true)
	sudo security create-keychain -p "" /Library/Keychains/VSCode.keychain; \
	sudo security default-keychain -s /Library/Keychains/VSCode.keychain; \
	sudo security unlock-keychain -p "" /Library/Keychains/VSCode.keychain; \
	vault kv get -field apple_certificate v1/ci/kv/vscodeextension/release | openssl base64 -d -A > certificate.p12; \
	sudo security import certificate.p12 -k /Library/Keychains/VSCode.keychain -P $$(vault kv get -field apple_certificate_password v1/ci/kv/vscodeextension/release) -T /usr/bin/codesign; \
	rm certificate.p12; \
	sudo security set-key-partition-list -S "apple-tool:,apple:,codesign:" -s -k "" /Library/Keychains/VSCode.keychain; \
	sudo security unlock-keychain -p "" /Library/Keychains/VSCode.keychain; \
	NATIVE_EXECUTABLE=$$(find target -name "*-runner"); \
	codesign -s "Developer ID Application: Confluent, Inc." -v $${NATIVE_EXECUTABLE} --options=runtime; \
	zip sidecar_signed.zip $${NATIVE_EXECUTABLE}; \
	vault kv get -field apple_key v1/ci/kv/vscodeextension/release | openssl base64 -d -A > auth_key.p8; \
	xcrun notarytool submit sidecar_signed.zip --apple-id $$(vault kv get -field apple_id_email v1/ci/kv/vscodeextension/release) --team-id $$(vault kv get -field apple_team_id v1/ci/kv/vscodeextension/release) --wait --issuer $$(vault kv get -field apple_issuer v1/ci/kv/vscodeextension/release) --key-id $$(vault kv get -field apple_key_id v1/ci/kv/vscodeextension/release) --key auth_key.p8; \
	rm auth_key.p8; \
	sudo security delete-keychain /Library/Keychains/VSCode.keychain
endif

# To run locally, ensure you're logged into Vault
# Generates the `THIRD_PARTY_NOTICES.txt` using FOSSA and saves it to the root of the project.
.PHONY: generate-third-party-notices
generate-third-party-notices:
	$(IDE_SIDECAR_SCRIPTS)/generate-third-party-notices.sh

# Creates a PR against the currently checked out branch with a newly generated `THIRD_PARTY_NOTICES.txt` file.
# Runs `generate-third-party-notices` before creating the PR.
.PHONY: update-third-party-notices-pr
update-third-party-notices-pr:
	$(IDE_SIDECAR_SCRIPTS)/update-third-party-notices-pr.sh

# Uploads the native executables and third party notices to the latest GitHub release.
.PHONY: upload-artifacts-to-github-release
upload-artifacts-to-github-release:
# Upload the native executables to the GitHub release
	for os_arch in macos-amd64 macos-arm64 linux-amd64 linux-arm64 windows-x64; do \
		executable=$$(find $$EXECUTABLES_DIR -name "*-$$os_arch*"); \
		gh release upload $(IDE_SIDECAR_VERSION) $$executable --clobber; \
	done; \
# Upload the third party notices to the GitHub release
	gh release upload $(IDE_SIDECAR_VERSION) THIRD_PARTY_NOTICES.txt --clobber; \
	gh release upload $(IDE_SIDECAR_VERSION) NOTICE-binary.txt --clobber; \
	gh release upload $(IDE_SIDECAR_VERSION) LICENSE.txt --clobber;

# Collects and appends all NOTICE files from the project's dependency JARs into a NOTICE-binary.txt file.
# Runs `mvn-package-native-sources-only` before collecting the notices to ensure the JARs are available.
.PHONY: collect-notices-binary
collect-notices-binary: clean mvn-package-native-sources-only
	$(IDE_SIDECAR_SCRIPTS)/collect-notices-binary.sh target/native-sources/lib

# Targets for managing cp-demo testcontainers used by the integration tests

# Start the cp-demo testcontainers
# Note: You do not need to run this in order to run the integration tests, however, if you want
# to manually bring up the cp-demo environment, you may run this target. You will be
# able to run the integration tests against the same environment, please keep that in mind!
.PHONY: cp-demo-start
cp-demo-start:
	export TESTCONTAINERS_RYUK_DISABLED=true; \
	./mvnw -s .mvn/settings.xml \
		-Dexec.mainClass=io.confluent.idesidecar.restapi.util.CPDemoTestEnvironment \
		-Dexec.classpathScope=test \
		test-compile exec:java

# Stop the cp-demo testcontainers
.PHONY: cp-demo-stop
cp-demo-stop:
	./mvnw -s .mvn/settings.xml test-compile && \
	./mvnw -s .mvn/settings.xml \
		-Dexec.mainClass=io.confluent.idesidecar.restapi.util.CPDemoTestEnvironment \
		-Dexec.classpathScope=test \
		-Dexec.args=stop \
		exec:java


CONFLUENT_DOCKER_TAG = $(shell yq e '.ide-sidecar.integration-tests.cp-demo.tag' src/main/resources/application.yml | sed 's/^v//')
# See io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer
CONFLUENT_LOCAL_DOCKER_TAG = 7.6.0
# See io.confluent.idesidecar.restapi.util.cpdemo.OpenldapContainer
OSIXIA_OPENLDAP_DOCKER_TAG = 1.3.0
# See io.confluent.idesidecar.restapi.util.cpdemo.ToolsContainer
CNFLDEMOS_TOOLS_DOCKER_TAG = 0.3

# Key for storing docker images in Semaphore CI cache
SEMAPHORE_CP_SERVER_DOCKER := ide-sidecar-docker-cp-server-$(CONFLUENT_DOCKER_TAG)
SEMAPHORE_CP_SCHEMA_REGISTRY_DOCKER := ide-sidecar-docker-cp-schema-registry-$(CONFLUENT_DOCKER_TAG)
SEMAPHORE_OPENLDAP_DOCKER := ide-sidecar-docker-openldap-$(OSIXIA_OPENLDAP_DOCKER_TAG)
SEMAPHORE_CNFLDEMOS_TOOLS_DOCKER := ide-sidecar-docker-cnfldemos-tools-$(CNFLDEMOS_TOOLS_DOCKER_TAG)
SEMAPHORE_CONFLUENT_LOCAL_DOCKER := ide-sidecar-docker-confluent-local-$(CONFLUENT_LOCAL_DOCKER_TAG)

## Cache docker images in Semaphore cache.
.PHONY: cache-docker-images
cache-docker-images:
	cache has_key $(SEMAPHORE_CP_SERVER_DOCKER) || (\
		docker pull confluentinc/cp-server:$(CONFLUENT_DOCKER_TAG) && \
		docker save confluentinc/cp-server:$(CONFLUENT_DOCKER_TAG) | gzip > cp-server.tgz && \
		cache store $(SEMAPHORE_CP_SERVER_DOCKER) cp-server.tgz && \
		rm -rf cp-server.tgz)

	cache has_key $(SEMAPHORE_CP_SCHEMA_REGISTRY_DOCKER) || (\
		docker pull confluentinc/cp-schema-registry:$(CONFLUENT_DOCKER_TAG) && \
		docker save confluentinc/cp-schema-registry:$(CONFLUENT_DOCKER_TAG) | gzip > cp-schema-registry.tgz && \
		cache store $(SEMAPHORE_CP_SCHEMA_REGISTRY_DOCKER) cp-schema-registry.tgz && \
		rm -rf cp-schema-registry.tgz)

	cache has_key $(SEMAPHORE_OPENLDAP_DOCKER) || (\
		docker pull osixia/openldap:$(OSIXIA_OPENLDAP_DOCKER_TAG) && \
		docker save osixia/openldap:$(OSIXIA_OPENLDAP_DOCKER_TAG) | gzip > openldap.tgz && \
		cache store $(SEMAPHORE_OPENLDAP_DOCKER) openldap.tgz && \
		rm -rf openldap.tgz)

	cache has_key $(SEMAPHORE_CNFLDEMOS_TOOLS_DOCKER) || (\
		docker pull cnfldemos/tools:$(CNFLDEMOS_TOOLS_DOCKER_TAG) && \
		docker save cnfldemos/tools:$(CNFLDEMOS_TOOLS_DOCKER_TAG) | gzip > cnfdemos-tools.tgz && \
		cache store $(SEMAPHORE_CNFLDEMOS_TOOLS_DOCKER) cnfdemos-tools.tgz && \
		rm -rf cnfdemos-tools.tgz)

	cache has_key $(SEMAPHORE_CONFLUENT_LOCAL_DOCKER) || (\
		docker pull confluentinc/confluent-local:$(CONFLUENT_LOCAL_DOCKER_TAG) && \
		docker save confluentinc/confluent-local:$(CONFLUENT_LOCAL_DOCKER_TAG) | gzip > confluent-local.tgz && \
		cache store $(SEMAPHORE_CONFLUENT_LOCAL_DOCKER) confluent-local.tgz && \
		rm -rf confluent-local.tgz)

.PHONY: load-cached-docker-images
load-cached-docker-images:
	cache restore $(SEMAPHORE_CP_SERVER_DOCKER)
	[ -f cp-server.tgz ] && docker load -i cp-server.tgz && rm -rf cp-server.tgz || true

	cache restore $(SEMAPHORE_CP_SCHEMA_REGISTRY_DOCKER)
	[ -f cp-schema-registry.tgz ] && docker load -i cp-schema-registry.tgz && rm -rf cp-schema-registry.tgz || true

	cache restore $(SEMAPHORE_OPENLDAP_DOCKER)
	[ -f openldap.tgz ] && docker load -i openldap.tgz && rm -rf openldap.tgz || true

	cache restore $(SEMAPHORE_CNFLDEMOS_TOOLS_DOCKER)
	[ -f cnfdemos-tools.tgz ] && docker load -i cnfdemos-tools.tgz && rm -rf cnfdemos-tools.tgz || true

	cache restore $(SEMAPHORE_CONFLUENT_LOCAL_DOCKER)
	[ -f confluent-local.tgz ] && docker load -i confluent-local.tgz && rm -rf confluent-local.tgz || true
