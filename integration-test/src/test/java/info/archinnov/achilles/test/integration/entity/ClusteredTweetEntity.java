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

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.CompoundPrimaryKey;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.type.Counter;

@Entity
public class ClusteredTweetEntity {
	@CompoundPrimaryKey
	private ClusteredTweetId id;

	@Column
	private String content;

	@Column(name = "original_author_id")
	private Long originalAuthorId;

	@Column(name = "is_a_retweet")
	private Boolean isARetweet;

	@Column
	private Counter retweetCount;

	@Column
	private Counter favoriteCount;

	public ClusteredTweetEntity() {
	}

	public ClusteredTweetEntity(ClusteredTweetId id, String content, Long originalAuthorId, Boolean isARetweet) {
		this.id = id;
		this.content = content;
		this.originalAuthorId = originalAuthorId;
		this.isARetweet = isARetweet;
	}

	public ClusteredTweetId getId() {
		return id;
	}

	public void setId(ClusteredTweetId id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Long getOriginalAuthorId() {
		return originalAuthorId;
	}

	public void setOriginalAuthorId(Long originalAuthorId) {
		this.originalAuthorId = originalAuthorId;
	}

	public Boolean getIsARetweet() {
		return isARetweet;
	}

	public void setIsARetweet(Boolean isARetweet) {
		this.isARetweet = isARetweet;
	}

	public Counter getRetweetCount() {
		return retweetCount;
	}

	public Counter getFavoriteCount() {
		return favoriteCount;
	}

	public void setRetweetCount(Counter retweetCount) {
		this.retweetCount = retweetCount;
	}

	public void setFavoriteCount(Counter favoriteCount) {
		this.favoriteCount = favoriteCount;
	}
}
