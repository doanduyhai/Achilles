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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTimeUUID.TABLE_NAME;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.TimeUUID;

import java.util.UUID;

@Entity(table = TABLE_NAME)
public class ClusteredEntityWithTimeUUID {

	public static final String TABLE_NAME = "clustered_with_time_uuid";

	@EmbeddedId
	private ClusteredKey id;

	@Column
	private String value;

	public ClusteredEntityWithTimeUUID() {
	}

	public ClusteredEntityWithTimeUUID(Long id, UUID date, String value) {
		this.id = new ClusteredKey(id, date);
		this.value = value;
	}

	public ClusteredKey getId() {
		return id;
	}

	public void setId(ClusteredKey id) {
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public static class ClusteredKey {
		@Column
		@Order(1)
		private Long id;

		@TimeUUID
		@Column
		@Order(2)
		private UUID date;

		public ClusteredKey() {
		}

		public ClusteredKey(Long id, UUID date) {
			this.id = id;
			this.date = date;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public UUID getDate() {
			return date;
		}

		public void setDate(UUID date) {
			this.date = date;
		}
	}
}
