package info.archinnov.achilles.query;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SliceQueryValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryValidator
{
	private static final Logger log = LoggerFactory.getLogger(SliceQueryValidator.class);
	private MethodInvoker invoker = new MethodInvoker();

	public <K> void validateCompoundKeys(PropertyMeta<?, ?> propertyMeta, K start, K end)
	{
		log.trace("Check compound keys {} / {}", start, end);
		if (start != null && end != null)
		{
			if (propertyMeta.isSingleKey())
			{
				Comparable<K> startComp = (Comparable<K>) start;

				Validator.validateTrue(startComp.compareTo(end) <= 0,
						"For slice query, start value should be lesser or equal to end value");
			}
			else
			{
				List<Method> componentGetters = propertyMeta
						.getMultiKeyProperties()
						.getComponentGetters();
				String propertyName = propertyMeta.getPropertyName();

				List<Object> startValues = invoker.determineMultiKeyValues(start, componentGetters);
				List<Object> endValues = invoker.determineMultiKeyValues(end, componentGetters);

				String startDescription = StringUtils.join(startValues, ",");
				String endDescription = StringUtils.join(endValues, ",");

				Validator.validateNotNull(startValues.get(0),
						"Partition key should not be null for start compound key ["
								+ startDescription + "]");

				Validator.validateNotNull(endValues.get(0),
						"Partition key should not be null for end compound key [" + endDescription
								+ "]");

				Validator.validateTrue(startValues.get(0).equals(endValues.get(0)),
						"Partition key should be equals for start and end compound keys : [["
								+ startDescription + "],[" + endDescription + "]]");

				int startIndex = findLastNonNullIndexForComponents(propertyName, startValues);
				int endIndex = findLastNonNullIndexForComponents(propertyName, endValues);

				Validator.validateTrue(Math.abs(startIndex - endIndex) <= 1, "Start compound key ["
						+ startDescription + "] and end compound key [" + endDescription
						+ "] are not valid for slice query");

				if (startIndex == endIndex && startIndex > 0)
				{
					for (int i = 0; i < startIndex; i++)
					{
						Validator.validateTrue(startValues.get(i).equals(endValues.get(i)), "[" + i
								+ "]th component of compound keys [[" + startDescription + "],["
								+ endDescription + "] should be equal");
					}

					Comparable<Object> lastStartComponent = (Comparable<Object>) startValues
							.get(startIndex);
					Comparable<Object> lastEndComponent = (Comparable<Object>) endValues
							.get(startIndex);
					Validator
							.validateTrue(
									lastStartComponent.compareTo(lastEndComponent) < 0,
									"For slice query, last component of start compound key ["
											+ startDescription
											+ "] should be strictly lesser to last component of end compound key ["
											+ endDescription + "]");
				}
				else
				{
					for (int i = 0; i <= Math.min(startIndex, endIndex); i++)
					{
						Validator.validateTrue(startValues.get(i).equals(endValues.get(i)), "[" + i
								+ "]th component of compound keys [[" + startDescription + "],["
								+ endDescription + "] should be equal");
					}

				}
			}
		}
		else if (start == null && end == null)
		{
			throw new AchillesException(
					"Start and end compound keys for Slice Query should not be both null");
		}
	}

	public int findLastNonNullIndexForComponents(String propertyName, List<Object> components)
	{
		boolean nullFlag = false;
		int lastNotNullIndex = 0;
		for (Object component : components)
		{
			if (component != null)
			{
				if (nullFlag)
				{
					throw new IllegalArgumentException(
							"There should not be any null value between two non-null components for compound key '"
									+ propertyName + "'");
				}
				lastNotNullIndex++;
			}
			else
			{
				nullFlag = true;
			}
		}
		lastNotNullIndex--;

		log.trace("Last non null index for components of property {} : {}", propertyName,
				lastNotNullIndex);
		return lastNotNullIndex;
	}
}
