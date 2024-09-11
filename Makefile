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
	$(MAKE) mvn-verify

.PHONY: test-native
test-native:
	$(MAKE) mvn-verify-native-executable

.PHONY: clean
clean:
	$(MAKE) mvn-clean

.PHONY: zip-templates
zip-templates:
	(cd src/main/resources/static && zip -r templates.zip templates/)
	(cd src/test/resources/static && zip -r templates.zip templates/)

.PHONY: zip-templates-windows
zip-templates-windows:
	powershell -Command Compress-Archive -Path src/main/resources/static/templates -DestinationPath src/main/resources/static/templates.zip
	powershell -Command Compress-Archive -Path src/test/resources/static/templates -DestinationPath src/test/resources/static/templates.zip

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
	for os_arch in macos-amd64 macos-arm64 linux-amd64 linux-arm64; do \
		executable=$$(find $$EXECUTABLES_DIR -name "*-$$os_arch"); \
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

.PHONY: prepend-native-image-cmd-windows
prepend-native-image-cmd-windows:
    $$filePath = "$Env:JAVA_HOME\bin\native-image.cmd" \
    $$prependText = "call \"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat\"`r`n" \
    $$fileContent = Get-Content -Raw -Path $$filePath \
    $$newContent = $$prependText + $$fileContent \
    Set-Content -Path $$filePath -Value $$newContent \
    Write-Host "Prepended the native-image.cmd file with the Visual Studio 2022 Community Edition vcvars64.bat path." \
    Get-Content -Path $$filePath
