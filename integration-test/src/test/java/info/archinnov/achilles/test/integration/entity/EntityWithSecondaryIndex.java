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

import info.archinnov.achilles.annotations.*;

@Entity
public class EntityWithSecondaryIndex {

	@CompoundPrimaryKey
	private EmbeddedKey id;

	@Column(name = "\"myLabel\"")
	@Index
	private String label;

	@Column
	@Index
	private Integer number;

	public EntityWithSecondaryIndex() {
	}

	public EntityWithSecondaryIndex(Long id, Integer rank, String label, Integer number) {
		this.id = new EmbeddedKey(id, rank);
		this.label = label;
		this.number = number;
	}

	public EmbeddedKey getId() {
		return id;
	}

	public void setId(EmbeddedKey id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	public static class EmbeddedKey {

		@PartitionKey
		private Long id;

        @ClusteringColumn
		private Integer rank;

		public EmbeddedKey() {
		}

		public EmbeddedKey(Long id, Integer rank) {
			this.id = id;
			this.rank = rank;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public Integer getRank() {
			return rank;
		}

		public void setRank(Integer rank) {
			this.rank = rank;
		}
	}
}
