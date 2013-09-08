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
package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;

import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class ClusteredTweetId {
	@Column(name = "user_id")
	private Long userId;

	@Column(name = "tweet_id")
	private UUID tweetId;

	@Column(name = "creation_date")
	private Date creationDate;

	@JsonCreator
	public ClusteredTweetId(@Order(1) @JsonProperty("userId") Long userId,
			@Order(2) @JsonProperty("tweetId") UUID tweetId,
			@Order(3) @JsonProperty("creationDate") Date creationDate) {
		this.userId = userId;
		this.tweetId = tweetId;
		this.creationDate = creationDate;
	}

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public UUID getTweetId() {
		return tweetId;
	}

	public void setTweetId(UUID tweetId) {
		this.tweetId = tweetId;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}
}
