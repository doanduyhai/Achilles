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
package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.test.mapping.entity.UserBean;

import javax.persistence.CascadeType;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;

@Entity
public class ClusteredEntityWithJoin {

	@EmbeddedId
	private CompoundKey id;

	@JoinColumn
	@ManyToMany(cascade = CascadeType.ALL)
	private UserBean friend;

	public CompoundKey getId() {
		return id;
	}

	public void setId(CompoundKey id) {
		this.id = id;
	}

	public UserBean getFriend() {
		return friend;
	}

	public void setFriend(UserBean friends) {
		this.friend = friends;
	}

}
