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

import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Order;

@Entity
public class ValuelessClusteredEntity {

	@EmbeddedId
	private CompoundKey id;

	public ValuelessClusteredEntity() {
	}

	public ValuelessClusteredEntity(CompoundKey id) {
		this.id = id;
	}

	public CompoundKey getId() {
		return id;
	}

	public void setId(CompoundKey id) {
		this.id = id;
	}

	public static class CompoundKey {
		@Order(1)
		private Long id;

		@Order(2)
		private String name;

		public CompoundKey() {
		}

		public CompoundKey(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
