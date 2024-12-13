package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.quarkus.runtime.annotations.RegisterForReflection;


/** Base class for all websocket message bodies. */
@RegisterForReflection
public interface MessageBody {

}
