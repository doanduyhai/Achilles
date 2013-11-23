/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.test.builders;

import org.apache.commons.lang.math.RandomUtils;
import info.archinnov.achilles.test.integration.entity.User;

public class UserTestBuilder {

	private Long id;

	private String firstname;

	private String lastname;

	public static UserTestBuilder user() {
		return new UserTestBuilder();
	}

	public User buid() {
		User user = new User();

		user.setId(id);
		user.setFirstname(firstname);
		user.setLastname(lastname);
		return user;
	}

	public UserTestBuilder id(Long id) {
		this.id = id;
		return this;
	}

	public UserTestBuilder randomId() {
		this.id = RandomUtils.nextLong();
		return this;
	}

	public UserTestBuilder firstname(String firstname) {
		this.firstname = firstname;
		return this;
	}

	public UserTestBuilder lastname(String lastname) {
		this.lastname = lastname;
		return this;
	}
}
