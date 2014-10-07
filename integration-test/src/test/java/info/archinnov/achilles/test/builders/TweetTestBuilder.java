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
package info.archinnov.achilles.test.builders;

import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;

import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;

public class TweetTestBuilder {

	private UUID id;

	private User creator;

	private String content;

	public static TweetTestBuilder tweet() {
		return new TweetTestBuilder();
	}

	public Tweet buid() {
		Tweet tweet = new Tweet();

		tweet.setId(id);
		tweet.setCreator(creator);
		tweet.setContent(content);
		return tweet;
	}

	public TweetTestBuilder id(UUID id) {
		this.id = id;
		return this;
	}

	public TweetTestBuilder randomId() {
		this.id = new UUID(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextLong(0,Long.MAX_VALUE));
		return this;
	}

	public TweetTestBuilder content(String content) {
		this.content = content;
		return this;
	}

	public TweetTestBuilder creator(User creator) {
		this.creator = creator;
		return this;
	}
}
