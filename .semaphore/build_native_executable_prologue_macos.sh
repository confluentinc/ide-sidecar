checkout
make ci-bin-sem-cache-restore
make docker-login-ci
make load-cached-docker-images
git config remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'
git fetch --all --tags
git checkout $(sem-context get release_version)
. vault-get-secret vscodeextension/release
# Install SDKMAN! (https://sdkman.io/install)
# SDKMAN! requires bash 4+, but macOS comes with bash 3.2. We need to install a newer version of bash and use it to install SDKMAN!
brew install bash
curl -s "https://get.sdkman.io?rcupdate=false" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
# Install GraalVM as defined in .sdkmanrc
sdk env install
