# Use predefine MVN or local `mvnw` if present in the repo, else fallback to globally installed `mvn`
ifeq ($(wildcard $(MVN)),)
MVN := $(CURDIR)/mvnw
endif
ifeq ($(wildcard $(MVN)),)
MVN := mvn
endif
MVN += $(MAVEN_ARGS)
MVN += $(foreach profile,$(MAVEN_PROFILES),-P$(profile))

MAVEN_SKIP_CHECKS=-DskipTests=true \
        -Dcheckstyle.skip=true \
        -Dspotbugs.skip=true \
        -Djacoco.skip=true \
        -Ddependency-check.skip=true

MAVEN_INSTALL_OPTS ?= --update-snapshots $(MAVEN_SKIP_CHECKS)
MAVEN_INSTALL_ARGS = $(MAVEN_INSTALL_OPTS) install

.PHONY: mvn-install
mvn-install:
ifneq ($(MAVEN_INSTALL_PROFILES),)
	$(MVN) $(foreach profile,$(MAVEN_INSTALL_PROFILES),-P$(profile)) $(MAVEN_INSTALL_ARGS)
else
	$(MVN) $(MAVEN_INSTALL_ARGS)
endif

.PHONY: mvn-verify
mvn-verify:
	$(MVN) $(MAVEN_VERIFY_OPTS) verify

.PHONY: mvn-clean
mvn-clean:
	$(MVN) clean

# Set the version in pom.xml to the bumped version
.PHONY: mvn-set-bumped-version
mvn-set-bumped-version:
	$(MVN) versions:set \
		-DnewVersion=$(BUMPED_CLEAN_VERSION) \
		-DgenerateBackupPoms=false
	git add --verbose $(shell find . -name pom.xml -maxdepth 2)

.PHONY: mvn-package-native
mvn-package-native:
	$(MVN) package \
		--activate-profiles native \
		-DGIT_COMMIT=$(shell git describe --always --dirty)

.PHONY: mvn-package-native-no-tests
mvn-package-native-no-tests:
	$(MVN) package $(MAVEN_SKIP_CHECKS) \
		--activate-profiles native \
		-DskipTests \
		-Dmaven.javadoc.skip=true

.PHONY: mvn-package-native-sources-only
mvn-package-native-sources-only:
	$(MVN) package $(MAVEN_SKIP_CHECKS) \
		-Dquarkus.native.enabled=true \
		-Dquarkus.native.sources-only=true \
		-DskipTests \
		-Dmaven.javadoc.skip=true

.PHONY: mvn-generate-sidecar-openapi-spec
mvn-generate-sidecar-openapi-spec:
	$(MVN) compile quarkus:build $(MAVEN_SKIP_CHECKS)

.PHONY: mvn-verify-native-executable
mvn-verify-native-executable:
	$(MVN) clean verify -Pnative

.PHONY: quarkus-dev
quarkus-dev:
	$(MVN) clean quarkus:dev

.PHONY: quarkus-test
quarkus-test:
	$(MVN) clean test-compile quarkus:test
