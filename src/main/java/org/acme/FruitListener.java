package org.acme;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import io.vertx.mutiny.core.eventbus.EventBus;

@ApplicationScoped
public class FruitListener {
	private static final Logger LOGGER = Logger.getLogger(FruitListener.class);

	@Inject
	EventBus eventBus;

	@Incoming("fruits")
	public void process(Fruit fruit) {
		LOGGER.infof("Got fruit from Kafka: %s", fruit);
		this.eventBus.publish("create-fruit", fruit);
	}

//	@Incoming("fruits")
//	@Outgoing("fruits-persisted")
//	@Broadcast
//	public Multi<Fruit> process(Multi<Fruit> stream) {
//		return stream
//			.invoke(fruit -> LOGGER.infof("Got fruit (before persist) -> %s", fruit))
//			.onItem().transformToUniAndConcatenate(fruit -> Panache.withTransaction(() -> Fruit.persist(fruit)).replaceWith(fruit))
//			.invoke(fruit -> LOGGER.infof("After persisted -> %s", fruit));
//	}
}
