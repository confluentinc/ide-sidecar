package io.confluent.idesidecar.restapi.testutil;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link NoAccessFilterProfile} with 1s WebClient request timeouts, so tests can stall a server
 * past the timeout without waiting long.
 */
public class ShortWebClientTimeoutProfile extends NoAccessFilterProfile {

  public static final long REQUEST_TIMEOUT_MS = 1_000;
  /** Longer than the request timeout, so a test can tell the two apart. */
  public static final long CONSUME_TIMEOUT_MS = 2_000;
  /** How long a stalled test server waits before answering, well past the request timeout. */
  public static final int STALL_MS = 4_500;
  /** The consume timeout plus generous scheduling slack, still under the stall. */
  public static final long GIVE_UP_WITHIN_MS = 4_000;

  @Override
  public Map<String, String> getConfigOverrides() {
    var overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put(
        "ide-sidecar.webclient.request-timeout-seconds", String.valueOf(REQUEST_TIMEOUT_MS / 1000)
    );
    overrides.put(
        "ide-sidecar.webclient.ccloud-consume-timeout-seconds",
        String.valueOf(CONSUME_TIMEOUT_MS / 1000)
    );
    return overrides;
  }
}
