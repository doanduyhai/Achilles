/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.test.integration.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.math.RandomUtils;
import info.archinnov.achilles.type.Counter;

public class CompleteBeanTestBuilder {

	private Long id;

	private String name;

	private String label;

	private Long age;

	private Tweet welcomeTweet;

	private Counter version;

	private List<String> friends = new ArrayList<String>();

	private Set<String> followers = new HashSet<String>();

	private Map<Integer, String> preferences = new HashMap<Integer, String>();

	public static CompleteBeanTestBuilder builder() {
		return new CompleteBeanTestBuilder();
	}

	public CompleteBean buid() {
		CompleteBean bean = new CompleteBean();

		bean.setId(id);
		bean.setName(name);
		bean.setLabel(label);
		bean.setAge(age);
		bean.setVersion(version);
		bean.setFriends(friends);
		bean.setFollowers(followers);
		bean.setPreferences(preferences);
		bean.setWelcomeTweet(welcomeTweet);
		return bean;
	}

	public CompleteBeanTestBuilder id(Long id) {
		this.id = id;
		return this;
	}

	public CompleteBeanTestBuilder randomId() {
		this.id = RandomUtils.nextLong();
		return this;
	}

	public CompleteBeanTestBuilder name(String name) {
		this.name = name;
		return this;
	}

	public CompleteBeanTestBuilder label(String label) {
		this.label = label;
		return this;
	}

	public CompleteBeanTestBuilder age(Long age) {
		this.age = age;
		return this;
	}

	public CompleteBeanTestBuilder welcomeTweet(Tweet welcomeTweet) {
		this.welcomeTweet = welcomeTweet;
		return this;
	}

	public CompleteBeanTestBuilder version(Counter version) {
		this.version = version;
		return this;
	}

	public CompleteBeanTestBuilder addFriend(String friend) {
		this.friends.add(friend);
		return this;
	}

	public CompleteBeanTestBuilder addFriends(String... friends) {
		this.friends.addAll(Arrays.asList(friends));
		return this;
	}

	public CompleteBeanTestBuilder addFollower(String follower) {
		this.followers.add(follower);
		return this;
	}

	public CompleteBeanTestBuilder addFollowers(String... followers) {
		this.followers.addAll(Arrays.asList(followers));
		return this;
	}

	public CompleteBeanTestBuilder addPreference(Integer key, String value) {
		this.preferences.put(key, value);
		return this;
	}
}
