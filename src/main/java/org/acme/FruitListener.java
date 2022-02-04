package org.acme;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import io.quarkus.hibernate.reactive.panache.Panache;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Broadcast;

@ApplicationScoped
public class FruitListener {
	private static final Logger LOGGER = Logger.getLogger(FruitListener.class);

	@Incoming("fruits")
	@Outgoing("fruits-persisted")
	@Broadcast
	public Multi<Fruit> process(Multi<Fruit> stream) {
		return stream
			.invoke(fruit -> LOGGER.infof("Got fruit (before persist) -> %s", fruit))
			.onItem().transformToUniAndConcatenate(fruit -> Panache.withTransaction(() -> Fruit.persist(fruit)).replaceWith(fruit))
			.invoke(fruit -> LOGGER.infof("After persisted -> %s", fruit));
	}
}
