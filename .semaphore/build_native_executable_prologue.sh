checkout
make ci-bin-sem-cache-restore
make docker-login-ci
make load-cached-docker-images
git config remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'
git fetch --all --tags
git checkout $(sem-context get release_version)
. vault-setup
# Install SDKMAN! (https://sdkman.io/install)
curl -s "https://get.sdkman.io?rcupdate=false" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
# Install GraalVM as defined in .sdkmanrc
sdk env install
