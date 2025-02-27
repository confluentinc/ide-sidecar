package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyHttpClient;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;

@ApplicationScoped
public class ConfluentCloudKafkaRestProduceProcessor extends Processor<
		KafkaRestProxyContext<ProduceRequest, ProduceResponse>,
		Future<KafkaRestProxyContext<ProduceRequest, ProduceResponse>>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Inject
	WebClientFactory webClientFactory;

	@Inject
	Vertx vertx;

	@Override
	public Future<KafkaRestProxyContext<ProduceRequest, ProduceResponse>> process(
			KafkaRestProxyContext<ProduceRequest, ProduceResponse> context
	) {
		context.setProxyRequestMethod(HttpMethod.POST);
		context.setProxyRequestAbsoluteUrl(
				context.getClusterInfo().uri() + "/kafka/v3/clusters/%s/topics/%s/records".formatted(
						context.getClusterId(),
						context.getTopicName()
				)
		);

		var connectionState = (CCloudConnectionState) context.getConnectionState();
		context.setProxyRequestHeaders(connectionState
				.getOauthContext()
				.getDataPlaneAuthenticationHeaders()
				.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
		);
		context.setProxyRequestBody(context.getRequestBody());

		var proxyHttpClient = new ProxyHttpClient<>(webClientFactory, vertx);
		return proxyHttpClient
				.send(context)
				// Parse the response from Confluent Cloud into the ProduceResponse model
				.compose(processedContext -> {
					try {
						ProduceResponse response = OBJECT_MAPPER.readValue(
								processedContext.getProxyRequestBody().toString(),
								ProduceResponse.class
						);
						context.setResponse(response);
						return Future.succeededFuture(context);
					} catch (JsonProcessingException e) {
						throw new ProcessorFailedException(
								context.failf(
										context.getProxyResponseStatusCode(),
										"Error parsing produce records response from Confluent Cloud Kafka REST: %s"
												.formatted(context.getProxyResponseBody())
								));
					}
				});
	}
}
