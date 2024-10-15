package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.api.RecordsV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceBatchRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceBatchResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.smallrye.mutiny.Uni;


public class RecordsV3ApiImpl implements RecordsV3Api {
  @Override
  public Uni<ProduceResponse> produceRecord(
      String clusterId,
      String topicName,
      ProduceRequest produceRequest
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Uni<ProduceBatchResponse> produceRecordsBatch(
      String clusterId,
      String topicName,
      ProduceBatchRequest produceBatchRequest
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
