package info.archinnov.achilles.internal.persistence.operations;

import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.type.CounterBuilder.CounterImpl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterPersister {
	private static final Logger log = LoggerFactory.getLogger(CounterPersister.class);

	public void persistCounters(PersistenceContext context, List<PropertyMeta> counterMetas) {
		log.trace("Persisting counters using PersistenceContext {}", context);
		Object entity = context.getEntity();
		for (PropertyMeta counterMeta : counterMetas) {
			Object counter = counterMeta.getValueFromField(entity);
			if (counter != null) {

				final long counterDelta = retrieveCounterValue(counter);
				if (counterDelta != 0) {
					context.bindForSimpleCounterIncrement(counterMeta, counterDelta);
				}
			}
		}
	}

	public void persistClusteredCounters(PersistenceContext context) {
		log.trace("Persisting clustered counter using PersistenceContext {}", context);
		Object entity = context.getEntity();

		int nullCount = 0;
		final List<PropertyMeta> allCountersMeta = context.getAllCountersMeta();
		for (PropertyMeta counterMeta : allCountersMeta) {
			Object counter = counterMeta.getValueFromField(entity);
			if (counter != null) {

				final long counterDelta = retrieveCounterValue(counter);
				if (counterDelta != 0) {
					context.pushClusteredCounterIncrementStatement(counterMeta, counterDelta);
				}
			} else {
				nullCount++;
			}
		}

		if (nullCount == allCountersMeta.size()) {
			throw new IllegalStateException("Cannot insert clustered counter entity '" + entity
					+ "' with null clustered counter value");
		}

	}

	private long retrieveCounterValue(Object counter) {
		long counterDelta;
		if (InternalCounterImpl.class.isInstance(counter)) {
			counterDelta = ((InternalCounterImpl) counter).getInternalCounterDelta();
		} else if (CounterImpl.class.isInstance(counter)) {
			counterDelta = ((CounterImpl) counter).get();
		} else {
			throw new IllegalArgumentException(String.format("The type of counter '%s' should be '%s' or '%s'",
					counter, InternalCounterImpl.class.getCanonicalName(), CounterImpl.class.getCanonicalName()));
		}
		return counterDelta;
	}

	public void removeRelatedCounters(PersistenceContext context) {
		log.trace("Removing counter values related to entity using PersistenceContext {}", context);
		EntityMeta entityMeta = context.getEntityMeta();

		for (PropertyMeta pm : entityMeta.getAllCounterMetas()) {
			context.bindForSimpleCounterRemoval(pm);
		}
	}
}
