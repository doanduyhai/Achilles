package fr.doan.achilles.dao;

import static fr.doan.achilles.entity.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.entity.metadata.PropertyType.START_EAGER;
import static fr.doan.achilles.serializer.Utils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;

import java.util.List;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.validation.Validator;

public class GenericDao<K> extends AbstractDao<K, DynamicComposite, Object>
{

	private DynamicComposite startCompositeForEagerFetch;
	private DynamicComposite endCompositeForEagerFetch;

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
		columnNameSerializer = DYNA_COMP_SRZ;
		valueSerializer = OBJECT_SRZ;
	}

	private void initComposites()
	{
		startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);
	}

	public Mutator<K> buildMutator()
	{
		return HFactory.createMutator(this.keyspace, this.keySerializer);
	}

	public List<Pair<DynamicComposite, Object>> eagerFetchEntity(K key)
	{

		return this.findColumnsRange(key, startCompositeForEagerFetch, endCompositeForEagerFetch,
				false, Integer.MAX_VALUE);
	}
}
