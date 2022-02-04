package org.acme;

import javax.enterprise.context.ApplicationScoped;

import org.jboss.logging.Logger;

import io.quarkus.hibernate.reactive.panache.common.runtime.ReactiveTransactional;
import io.quarkus.vertx.ConsumeEvent;

import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class EventBusListener {
	private static final Logger LOGGER = Logger.getLogger(EventBusListener.class);

	@ConsumeEvent("create-fruit")
	@ReactiveTransactional
	public Uni<Void> createFruit(Fruit fruit) {
		LOGGER.infof("Persisting fruit: %s", fruit);
		return Fruit.persist(fruit)
			.replaceWith(fruit)
			.invoke(f -> LOGGER.infof("Persisted fruit: %s", f))
			.replaceWithVoid();
	}
}
