package fr.doan.achilles.dao;

import static fr.doan.achilles.serializer.Utils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;

/**
 * GenericWideRowDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class GenericWideRowDao<K> extends AbstractDao<K, Composite, Object>
{

	public GenericWideRowDao(Keyspace keyspace, Serializer<K> keySrz, String cf) {
		super(keyspace);
		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = OBJECT_SRZ;
	}
}
