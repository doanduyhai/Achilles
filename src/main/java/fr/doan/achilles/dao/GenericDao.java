package fr.doan.achilles.dao;

import static fr.doan.achilles.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.metadata.PropertyType.START_EAGER;
import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;

import java.util.List;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.validation.Validator;

public class GenericDao<K> extends AbstractDao<K, Composite, Object>
{

	private Composite startCompositeForEagerFetch;
	private Composite endCompositeForEagerFetch;

	protected GenericDao() {
		initComposites();
	}

	public GenericDao(Keyspace keyspace, Serializer<K> keySrz, String cf) {
		super(keyspace);
		initComposites();
		Validator.validateNotBlank(cf, "columnFamily");
		Validator.validateNotNull(keySrz, "keySerializer for columnFamily ='" + columnFamily + "'");
		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = OBJECT_SRZ;
	}

	private void initComposites()
	{
		startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(), ComponentEquality.GREATER_THAN_EQUAL);
	}

	public Mutator<K> buildMutator()
	{
		return HFactory.createMutator(this.keyspace, this.keySerializer);
	}

	public Composite buildCompositeForProperty(String propertyName, PropertyType type, int hashOrPosition)
	{
		Composite composite = new Composite();
		composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	public Composite buildCompositeComparatorStart(String propertyName, PropertyType type)
	{
		Composite composite = new Composite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);

		return composite;
	}

	public Composite buildCompositeComparatorEnd(String propertyName, PropertyType type)
	{
		Composite composite = new Composite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.GREATER_THAN_EQUAL);

		return composite;
	}

	public Composite buildCompositeComparatorStart(String propertyName, PropertyType type, int hashOrPosition, boolean exclusive)
	{
		Composite composite = new Composite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		if (exclusive)
		{
			composite.addComponent(2, hashOrPosition, ComponentEquality.GREATER_THAN_EQUAL);
		}
		else
		{
			composite.addComponent(2, hashOrPosition, ComponentEquality.EQUAL);
		}

		return composite;
	}

	public Composite buildCompositeComparatorEnd(String propertyName, PropertyType type, int hashOrPosition, boolean exclusive)
	{
		Composite composite = new Composite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		if (exclusive)
		{
			composite.addComponent(2, hashOrPosition, ComponentEquality.LESS_THAN_EQUAL);
		}
		else
		{
			composite.addComponent(2, hashOrPosition, ComponentEquality.EQUAL);
		}

		return composite;
	}

	public List<Pair<Composite, Object>> eagerFetchEntity(K key)
	{

		return this.findColumnsRange(key, startCompositeForEagerFetch, endCompositeForEagerFetch, false, Integer.MAX_VALUE);
	}
}
