package integration.tests.entity;

import java.io.Serializable;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import fr.doan.achilles.entity.type.WideMap;

@Table
public class User implements Serializable
{

	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String firstname;

	@Column
	private String lastname;

	@ManyToMany(cascade = CascadeType.ALL)
	@JoinColumn
	private WideMap<Integer, Tweet> tweets;

	@ManyToMany(cascade = CascadeType.MERGE)
	@JoinColumn
	private WideMap<Long, Tweet> timeline;

	@ManyToMany(cascade = CascadeType.PERSIST)
	@JoinColumn(table = "retweets_cf")
	private WideMap<Integer, Tweet> retweets;

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

	public WideMap<Integer, Tweet> getTweets()
	{
		return tweets;
	}

	public WideMap<Long, Tweet> getTimeline()
	{
		return timeline;
	}

	public WideMap<Integer, Tweet> getRetweets()
	{
		return retweets;
	}
}
