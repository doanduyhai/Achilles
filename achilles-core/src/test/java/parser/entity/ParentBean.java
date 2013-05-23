package parser.entity;

import javax.persistence.Column;

/**
 * ParentBean
 * 
 * @author DuyHai DOAN
 * 
 */
public class ParentBean extends GrandParentBean
{

	@Column
	private String name;

	@Column
	private String address;

	private String unmapped;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getAddress()
	{
		return address;
	}

	public void setAddress(String address)
	{
		this.address = address;
	}

	public String getUnmapped()
	{
		return unmapped;
	}

	public void setUnmapped(String unmapped)
	{
		this.unmapped = unmapped;
	}
}
