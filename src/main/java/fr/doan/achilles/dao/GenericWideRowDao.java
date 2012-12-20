package fr.doan.achilles.dao;

import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

/**
 * GenericWideRowDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class GenericWideRowDao<K, N> extends AbstractDao<K, N, Object>
{
	public GenericWideRowDao(Keyspace keyspace, Serializer<K> keySrz, Serializer<N> nameSrz,
			String cf)
	{
		super(keyspace);
		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = nameSrz;
		valueSerializer = OBJECT_SRZ;
	}
}
