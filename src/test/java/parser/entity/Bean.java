package parser.entity;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.MERGE;
import static javax.persistence.CascadeType.PERSIST;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.entity.type.WideMap;

/**
 * Bean
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
public class Bean implements Serializable
{
	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Column(name = "age_in_year")
	private Long age;

	@Lazy
	@Column
	private List<String> friends;

	@Column
	private Set<String> followers;

	@Column
	private Map<Integer, String> preferences;

	@ManyToOne(cascade = ALL)
	@JoinColumn
	private UserBean creator;

	@ManyToMany(cascade =
	{
			PERSIST,
			MERGE
	})
	@JoinColumn(name = "linked_users")
	private WideMap<String, UserBean> users;

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

	public UserBean getCreator()
	{
		return creator;
	}

	public void setCreator(UserBean creator)
	{
		this.creator = creator;
	}

	public WideMap<String, UserBean> getUsers()
	{
		return users;
	}
}
