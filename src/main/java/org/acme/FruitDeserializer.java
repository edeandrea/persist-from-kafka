package org.acme;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class FruitDeserializer extends ObjectMapperDeserializer<Fruit> {
	public FruitDeserializer() {
		super(Fruit.class);
	}
}
