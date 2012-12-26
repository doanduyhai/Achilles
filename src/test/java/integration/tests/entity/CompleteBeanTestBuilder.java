package integration.tests.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;

public class CompleteBeanTestBuilder
{

	private Long id;

	private String name;

	private String label;

	private Long age;

	private List<String> friends = new ArrayList<String>();

	private Set<String> followers = new HashSet<String>();

	private Map<Integer, String> preferences = new HashMap<Integer, String>();

	public static CompleteBeanTestBuilder builder()
	{
		return new CompleteBeanTestBuilder();
	}

	public CompleteBean buid()
	{
		CompleteBean bean = new CompleteBean();

		bean.setId(id);
		bean.setName(name);
		bean.setLabel(label);
		bean.setAge(age);
		bean.setFriends(friends);
		bean.setFollowers(followers);
		bean.setPreferences(preferences);
		return bean;
	}

	public CompleteBeanTestBuilder id(Long id)
	{
		this.id = id;
		return this;
	}

	public CompleteBeanTestBuilder randomId()
	{
		this.id = RandomUtils.nextLong();
		return this;
	}

	public CompleteBeanTestBuilder name(String name)
	{
		this.name = name;
		return this;
	}

	public CompleteBeanTestBuilder label(String label)
	{
		this.label = label;
		return this;
	}

	public CompleteBeanTestBuilder age(Long age)
	{
		this.age = age;
		return this;
	}

	public CompleteBeanTestBuilder addFriend(String friend)
	{
		this.friends.add(friend);
		return this;
	}

	public CompleteBeanTestBuilder addFriends(String... friends)
	{
		this.friends.addAll(Arrays.asList(friends));
		return this;
	}

	public CompleteBeanTestBuilder addFollower(String follower)
	{
		this.followers.add(follower);
		return this;
	}

	public CompleteBeanTestBuilder addFollowers(String... followers)
	{
		this.followers.addAll(Arrays.asList(followers));
		return this;
	}

	public CompleteBeanTestBuilder addPreference(Integer key, String value)
	{
		this.preferences.put(key, value);
		return this;
	}
}
