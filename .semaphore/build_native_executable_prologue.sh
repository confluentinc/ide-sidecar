checkout
. vault-setup
# Install SDKMAN! (https://sdkman.io/install)
curl -s "https://get.sdkman.io?rcupdate=false" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
# Install GraalVM as defined in .sdkmanrc
sdk env install
make ci-bin-sem-cache-restore
