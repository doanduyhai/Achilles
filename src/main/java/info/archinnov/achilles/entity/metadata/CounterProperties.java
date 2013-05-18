package info.archinnov.achilles.entity.metadata;

/**
 * CounterProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterProperties
{
	private String fqcn;
	private PropertyMeta<?, ?> idMeta;

	public CounterProperties(String fqcn) {
		this.fqcn = fqcn;
	}

	public CounterProperties(String fqcn, PropertyMeta<?, ?> idMeta) {
		this.fqcn = fqcn;
		this.idMeta = idMeta;
	}

	public String getFqcn()
	{
		return fqcn;
	}

	public PropertyMeta<?, ?> getIdMeta()
	{
		return idMeta;
	}

	public void setIdMeta(PropertyMeta<?, ?> idMeta)
	{
		this.idMeta = idMeta;
	}

	@Override
	public String toString()
	{
		return "CounterProperties [fqcn=" + fqcn + ", idMeta=" + idMeta + "]";
	}
}
