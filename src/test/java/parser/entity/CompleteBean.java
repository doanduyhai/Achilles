package parser.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

@Table
public class CompleteBean implements Serializable
{
	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Column(name = "age_in_year")
	private Long age;

	@Column
	private List<String> friends;

	@Column
	private Set<String> followers;

	@Column
	private Map<Integer, String> preferences;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public Long getAge()
	{
		return age;
	}

	public void setAge(Long age)
	{
		this.age = age;
	}

	public List<String> getFriends()
	{
		return friends;
	}

	public void setFriends(List<String> friends)
	{
		this.friends = friends;
	}

	public Set<String> getFollowers()
	{
		return followers;
	}

	public void setFollowers(Set<String> followers)
	{
		this.followers = followers;
	}

	public Map<Integer, String> getPreferences()
	{
		return preferences;
	}

	public void setPreferences(Map<Integer, String> preferences)
	{
		this.preferences = preferences;
	}

}
