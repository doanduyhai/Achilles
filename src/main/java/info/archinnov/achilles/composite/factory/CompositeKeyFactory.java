package info.archinnov.achilles.composite.factory;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.validation.Validator;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeKeyFactory
{
	private static final Logger log = LoggerFactory.getLogger(CompositeKeyFactory.class);

	private CompositeHelper helper = new CompositeHelper();
	private EntityHelper entityHelper = new EntityHelper();

	@SuppressWarnings("unchecked")
	public <K, V, T> Composite createBaseComposite(PropertyMeta<K, V> propertyMeta, T keyValue)
	{
		log.trace("Creating base composite for propertyMeta {}", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		String propertyName = propertyMeta.getPropertyName();

		if (propertyMeta.isSingleKey())
		{
			Validator.validateNotNull(keyValue, "The values for the for the key of WideMap '"
					+ propertyName + "' should not be null");

			Serializer<T> keySerializer = (Serializer<T>) propertyMeta.getKeySerializer();
			composite.setComponent(0, keyValue, keySerializer, keySerializer.getComparatorType()
					.getTypeName());
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			List<Serializer<?>> componentSerializers = multiKeyProperties.getComponentSerializers();
			List<Object> keyValues = entityHelper.determineMultiKey(keyValue,
					multiKeyProperties.getComponentGetters());
			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount == valueCount, "There should be " + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			for (Object value : keyValues)
			{
				Validator.validateNotNull(value, "The values for the for the key of WideMap '"
						+ propertyName + "' should not be null");
			}

			for (int i = 0; i < srzCount; i++)
			{
				Serializer<Object> srz = (Serializer<Object>) componentSerializers.get(i);
				composite.setComponent(i, keyValues.get(i), srz, srz.getComparatorType()
						.getTypeName());
			}
		}
		return composite;
	}

	@SuppressWarnings("unchecked")
	public <K, V, T> Composite createForQuery(PropertyMeta<K, V> propertyMeta, T keyValue,
			ComponentEquality equality)
	{
		log.trace("Creating query composite for propertyMeta {}", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		String propertyName = propertyMeta.getPropertyName();

		if (propertyMeta.isSingleKey())
		{
			if (keyValue == null)
			{
				composite = null;
			}
			else
			{
				composite.addComponent(0, keyValue, equality);
			}
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			List<Serializer<?>> componentSerializers = multiKeyProperties.getComponentSerializers();
			List<Object> keyValues = entityHelper.determineMultiKey(keyValue,
					multiKeyProperties.getComponentGetters());
			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			int lastNotNullIndex = helper
					.findLastNonNullIndexForComponents(propertyName, keyValues);

			for (int i = 0; i <= lastNotNullIndex; i++)
			{
				Serializer<Object> srz = (Serializer<Object>) componentSerializers.get(i);
				Object value = keyValues.get(i);
				if (i < lastNotNullIndex)
				{
					composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
							EQUAL);
				}
				else
				{
					composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
							equality);
				}
			}
		}
		return composite;
	}

	public <K, V> Composite[] createForQuery(PropertyMeta<K, V> propertyMeta, K start,
			boolean inclusiveStart, K end, boolean inclusiveEnd, boolean reverse)
	{
		Composite[] queryComp = new Composite[2];

		ComponentEquality[] equalities = helper.determineEquality(inclusiveStart, inclusiveEnd,
				reverse);

		Composite startComp = this.createForQuery(propertyMeta, start, equalities[0]);
		Composite endComp = this.createForQuery(propertyMeta, end, equalities[1]);

		queryComp[0] = startComp;
		queryComp[1] = endComp;

		return queryComp;
	}
}
