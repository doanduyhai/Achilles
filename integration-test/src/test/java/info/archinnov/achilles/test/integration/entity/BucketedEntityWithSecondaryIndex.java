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

public class BucketedEntityWithSecondaryIndex {

	@EmbeddedId
	private EmbeddedKey id;

	@Column
	@Index
	private String label;

	public BucketedEntityWithSecondaryIndex() {
	}

	public BucketedEntityWithSecondaryIndex(Long id, String type, Integer rank, String label) {
		this.id = new EmbeddedKey(id, type, rank);
		this.label = label;
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

	public static class EmbeddedKey {

		@PartitionKey(1)
		private Long id;

		@PartitionKey(2)
		private String type;

        @ClusteringColumn
		private Integer rank;

		public EmbeddedKey() {
		}

		public EmbeddedKey(Long id, String type, Integer rank) {
			this.id = id;
			this.type = type;
			this.rank = rank;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public Integer getRank() {
			return rank;
		}

		public void setRank(Integer rank) {
			this.rank = rank;
		}
	}
}
