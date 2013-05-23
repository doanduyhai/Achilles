package integration.tests.entity;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import info.archinnov.achilles.annotations.Consistency;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * BeanWithWriteOneAndReadLocalQuorumConsistency
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Consistency(read = LOCAL_QUORUM, write = ONE)
public class BeanWithWriteOneAndReadLocalQuorumConsistency implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String firstname;

	@Column
	private String lastname;

	public BeanWithWriteOneAndReadLocalQuorumConsistency() {}

	public BeanWithWriteOneAndReadLocalQuorumConsistency(Long id, String firstname, String lastname)
	{
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
	}

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public String getFirstname()
	{
		return firstname;
	}

	public void setFirstname(String firstname)
	{
		this.firstname = firstname;
	}

	public String getLastname()
	{
		return lastname;
	}

	public void setLastname(String lastname)
	{
		this.lastname = lastname;
	}
}
