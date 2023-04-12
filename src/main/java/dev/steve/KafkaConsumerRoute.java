package dev.steve;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class KafkaConsumerRoute extends RouteBuilder {

    @Inject
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @Override
    public void configure() throws Exception {

        fromF("kafka:%s?brokers=%s&groupId=my-group-id&autoOffsetReset=earliest", "my-topic", kafkaBootstrapServers)
                .routeId("kafka-consumer-route")
                .log("${body}")
                .process(exchange -> {
                    String key = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
                    String value = exchange.getIn().getBody(String.class);
                    System.out.println("Received message with key: " + key + " and value: " + value);
                });
    }
}
