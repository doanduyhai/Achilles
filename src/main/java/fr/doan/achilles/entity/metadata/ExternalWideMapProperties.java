package fr.doan.achilles.entity.metadata;

import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.dao.GenericCompositeDao;

/**
 * ExternalWideMapProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class ExternalWideMapProperties<ID>
{
	private String externalColumnFamilyName;
	private GenericCompositeDao<ID, ?> externalWideMapDao;
	private Serializer<ID> idSerializer;

	public ExternalWideMapProperties() {}

	public ExternalWideMapProperties(String externalColumnFamilyName,
			GenericCompositeDao<ID, ?> dao, Serializer<ID> idSerializer)
	{
		this.externalColumnFamilyName = externalColumnFamilyName;
		this.externalWideMapDao = dao;
		this.idSerializer = idSerializer;
	}

	public String getExternalColumnFamilyName()
	{
		return externalColumnFamilyName;
	}

	public GenericCompositeDao<ID, ?> getExternalWideMapDao()
	{
		return externalWideMapDao;
	}

	public void setExternalWideMapDao(GenericCompositeDao<ID, ?> externalWideMapDao)
	{
		this.externalWideMapDao = externalWideMapDao;
	}

	public Serializer<ID> getIdSerializer()
	{
		return idSerializer;
	}
}
