package testBuilders;

import integration.tests.entity.User;

import org.apache.commons.lang.math.RandomUtils;

/**
 * UserTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class UserTestBuilder
{

	private Long id;

	private String firstname;

	private String lastname;

	public static UserTestBuilder user()
	{
		return new UserTestBuilder();
	}

	public User buid()
	{
		User user = new User();

		user.setId(id);
		user.setFirstname(firstname);
		user.setLastname(lastname);
		return user;
	}

	public UserTestBuilder id(Long id)
	{
		this.id = id;
		return this;
	}

	public UserTestBuilder randomId()
	{
		this.id = RandomUtils.nextLong();
		return this;
	}

	public UserTestBuilder firstname(String firstname)
	{
		this.firstname = firstname;
		return this;
	}

	public UserTestBuilder lastname(String lastname)
	{
		this.lastname = lastname;
		return this;
	}
}
