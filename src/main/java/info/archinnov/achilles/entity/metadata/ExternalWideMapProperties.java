package info.archinnov.achilles.entity.metadata;

import me.prettyprint.hector.api.Serializer;

/**
 * ExternalWideMapProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class ExternalWideMapProperties<ID>
{
	private String externalColumnFamilyName;
	private Serializer<ID> idSerializer;

	public ExternalWideMapProperties() {}

	public ExternalWideMapProperties(String externalColumnFamilyName, Serializer<ID> idSerializer) {
		this.externalColumnFamilyName = externalColumnFamilyName;
		this.idSerializer = idSerializer;
	}

	public String getExternalColumnFamilyName()
	{
		return externalColumnFamilyName;
	}

	public Serializer<ID> getIdSerializer()
	{
		return idSerializer;
	}
}
