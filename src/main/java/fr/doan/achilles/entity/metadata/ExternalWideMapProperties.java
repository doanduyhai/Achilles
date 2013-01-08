package fr.doan.achilles.entity.metadata;

import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.dao.GenericWideRowDao;

/**
 * ExternalWideMapProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class ExternalWideMapProperties<ID>
{
	private String externalColumnFamilyName;
	private GenericWideRowDao<ID, ?> externalWideMapDao;
	private Serializer<ID> idSerializer;

	public ExternalWideMapProperties() {}

	public ExternalWideMapProperties(String externalColumnFamilyName, GenericWideRowDao<ID, ?> dao,
			Serializer<ID> idSerializer)
	{
		this.externalColumnFamilyName = externalColumnFamilyName;
		this.externalWideMapDao = dao;
		this.idSerializer = idSerializer;
	}

	public String getExternalColumnFamilyName()
	{
		return externalColumnFamilyName;
	}

	public GenericWideRowDao<ID, ?> getExternalWideMapDao()
	{
		return externalWideMapDao;
	}

	public void setExternalWideMapDao(GenericWideRowDao<ID, ?> externalWideMapDao)
	{
		this.externalWideMapDao = externalWideMapDao;
	}

	public Serializer<ID> getIdSerializer()
	{
		return idSerializer;
	}
}
