package dev.steve;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class KafkaConsumerRoute extends RouteBuilder {

    @Inject
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @Override
    public void configure() throws Exception {

        KafkaEndpoint kafkaEndpoint = getContext().getEndpoint("kafka:my-topic", KafkaEndpoint.class);
        kafkaEndpoint.getConfiguration().setBrokers(kafkaBootstrapServers);
        kafkaEndpoint.getConfiguration().setGroupId(UUID.randomUUID().toString()); // generate a random group.id
        kafkaEndpoint.getConfiguration().setAutoOffsetReset("earliest");


        from(kafkaEndpoint)
            .routeId("kafka-consumer-route")
            .log("${body}")
            .process(exchange -> {
                String key = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
                String value = exchange.getIn().getBody(String.class);
                System.out.println("Received message with key: " + key + " and value: " + value);
            });
    }
}
