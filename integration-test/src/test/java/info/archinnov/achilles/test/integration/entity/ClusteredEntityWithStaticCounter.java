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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticCounter.TABLE_NAME;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.type.Counter;

@Entity(table = TABLE_NAME)
public class ClusteredEntityWithStaticCounter {

	public static final String TABLE_NAME = "clustered_with_static_counter";

	@EmbeddedId
	private ClusteredKeyForCounter id;

    @Column(staticColumn = true)
    private Counter version;

	@Column
	private Counter count;


    public ClusteredEntityWithStaticCounter(ClusteredKeyForCounter id, Counter version, Counter count) {
		this.id = id;
        this.version = version;
        this.count = count;
    }

	public ClusteredKeyForCounter getId() {
		return id;
	}

	public void setId(ClusteredKeyForCounter id) {
		this.id = id;
	}

	public Counter getCount() {
		return count;
	}

	public void setCount(Counter count) {
		this.count = count;
	}

    public Counter getVersion() {
        return version;
    }

    public void setVersion(Counter version) {
        this.version = version;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClusteredEntityWithStaticCounter other = (ClusteredEntityWithStaticCounter) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	public static class ClusteredKeyForCounter {
		@Column
		@Order(1)
		private Long id;

		@Column
		@Order(2)
		private String name;

		public ClusteredKeyForCounter() {
		}

		public ClusteredKeyForCounter(Long id, String name) {
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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ClusteredKeyForCounter other = (ClusteredKeyForCounter) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

	}
}
