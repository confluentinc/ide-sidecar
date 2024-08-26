<!-- Consider adding [ci skip] to the PR title if the PR does not change the source code or tests. -->

## Summary of Changes

<!-- Include a high-level overview of your implementation, including any alternatives you considered and items you'll address in follow-up PRs -->


## Any additional details or context that should be provided?

<!-- Behavior before/after, more technical details/screenshots, follow-on work that should be expected, links to discussions or issues, etc -->


## Pull request checklist

Please check if your PR fulfills the following (if applicable):

- Tests:
    - [ ] Added new
    - [ ] Updated existing
    - [ ] Deleted existing
- [ ] Have you validated this change locally against a running instance of the Quarkus dev server?
    ```shell
    ./mvnw compile quarkus:dev
    ```
- [ ] Have you validated this change against a locally running native executable?
    ```shell
    make mvn-package-native && ./target/ide-sidecar-*-runner
    ```

