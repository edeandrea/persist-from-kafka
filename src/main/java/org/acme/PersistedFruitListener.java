package org.acme;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PersistedFruitListener {
	private static final Logger LOGGER = Logger.getLogger(PersistedFruitListener.class);

	@Incoming("fruits-persisted")
	public void process(Fruit fruit) {
		LOGGER.infof("Got persisted fruit: %s", fruit);
	}
}
