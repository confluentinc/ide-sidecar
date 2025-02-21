RELEASE_PRECOMMIT := mvn-set-bumped-version set-sidecar-bumped-version regenerate-openapi-specs

.PHONY: release-ci
release-ci:
ifeq ($(CI),true)
ifneq ($(RELEASE_BRANCH),$(_empty))
	$(MAKE) $(MAKE_ARGS) pre-release-check $(RELEASE_PRECOMMIT) get-release-image commit-release tag-release create-gh-release
else
# when building a PR, fail if pre-release check fails (e.g. dirty repo)
	$(MAKE) $(MAKE_ARGS) pre-release-check
endif
else
	true
endif

.PHONY: pre-release-check
pre-release-check:
	git diff --exit-code || (echo "ERROR: the repo is not supposed to have local dirty changes prior to releasing" && git status && exit 1)

.PHONY: set-sidecar-bumped-version
set-sidecar-bumped-version:
ifeq ($(CI),true)
	echo '$(BUMPED_VERSION)' > $(IDE_SIDECAR_VERSION_FILE)
	git add $(IDE_SIDECAR_VERSION_FILE)
endif

.PHONY: create-gh-release
create-gh-release:
ifeq ($(CI),true)
	gh release create $(BUMPED_VERSION) --generate-notes --latest --title "$(BUMPED_VERSION)"
endif

.PHONY: regenerate-openapi-specs
regenerate-openapi-specs:
# mainly to ensure the `info.version` is up to date with the bumped version
ifeq ($(CI),true)
	$(MAKE) $(MAKE_ARGS) mvn-generate-sidecar-openapi-spec
	git add src/generated/resources/openapi*
endif
