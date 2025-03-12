package io.confluent.idesidecar.restapi.credentials;

public record CredentialContainer(
    String name,
    Credentials credentials
) {

}
