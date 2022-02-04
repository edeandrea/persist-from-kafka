package org.acme;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.List;

import javax.inject.Inject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

@QuarkusTest
@QuarkusTestResource(KafkaProducerResource.class)
class FruitListenerTests {
	@InjectKafkaProducer
	KafkaProducer<String, Fruit> fruitsProducer;

	@Inject
	@Channel("fruits-persisted")
	Multi<Fruit> persistedFruitsChannel;

	@Test
	public void itemsPersist() throws InterruptedException {
		assertThat(Fruit.count().await().atMost(Duration.ofSeconds(5)))
			.isNotNull()
			.isZero();

		createSampleFruits().stream()
			.map(fruit -> new ProducerRecord<String, Fruit>("fruits", fruit))
			.forEach(fruitsProducer::send);

		// Wait for the messages to arrive
		var fruitsFromChannel = this.persistedFruitsChannel
			.select().first(2L)
			.subscribe().withSubscriber(AssertSubscriber.create(2L))
			.assertSubscribed()
			.awaitItems(2, Duration.ofMinutes(1))
			.assertCompleted()
			.getItems();

		// Check the fruits I got from the channel
		checkFruits(fruitsFromChannel);

		// Check the fruits in the database
		checkFruits(Fruit.<Fruit>listAll().await().atMost(Duration.ofSeconds(5)));

		// Cleanup
		assertThat(Fruit.deleteAll().await().atMost(Duration.ofSeconds(5)))
			.isNotNull()
			.isEqualTo(2L);

		assertThat(Fruit.count().await().atMost(Duration.ofSeconds(5)))
			.isNotNull()
			.isZero();
	}

	private static void checkFruits(List<Fruit> fruits) {
		assertThat(fruits)
			.isNotNull()
			.isNotEmpty()
			.hasSize(2)
			.extracting("name", "description")
			.containsExactlyInAnyOrder(
				tuple("Banana", "Yummy fruit"),
				tuple("Pear", "Juicy fruit")
			);

		fruits.stream()
			.map(fruit -> fruit.id)
			.forEach(fruitId ->
				assertThat(fruitId)
					.isNotNull()
					.isPositive()
			);
	}

	private static List<Fruit> createSampleFruits() {
		return List.of(
			new Fruit("Banana", "Yummy fruit"),
			new Fruit("Pear", "Juicy fruit")
		);
	}
}
