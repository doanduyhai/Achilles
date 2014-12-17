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
import info.archinnov.achilles.internal.metadata.holder.PropertyType;

import java.nio.ByteBuffer;
import java.util.Objects;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityForTranscoding.TABLE_NAME;

@Entity(table = TABLE_NAME)
public class ClusteredEntityForTranscoding {

	public static final String TABLE_NAME = "clustered_for_transcoding";

	@EmbeddedId
	private EmbeddedKey id;

	public ClusteredEntityForTranscoding() {
	}

	public ClusteredEntityForTranscoding(Long id, PropertyType type, Integer year, ByteBuffer bytes) {
		this.id = new EmbeddedKey(id, type, year, bytes);
	}

	public EmbeddedKey getId() {
		return id;
	}

	public void setId(EmbeddedKey id) {
		this.id = id;
	}

	public static class EmbeddedKey {

		@PartitionKey(1)
		private long id;

		@PartitionKey(2)
		private PropertyType type;

        @ClusteringColumn(1)
		private Integer year;

        @ClusteringColumn(2)
        private ByteBuffer bytes;

		public EmbeddedKey() {
		}

		public EmbeddedKey(long id, PropertyType type, Integer year, ByteBuffer bytes) {
			this.id = id;
			this.type = type;
			this.year = year;
            this.bytes = bytes;
        }

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public PropertyType getType() {
			return type;
		}

		public void setType(PropertyType type) {
			this.type = type;
		}

        public Integer getYear() {
            return year;
        }

        public void setYear(Integer year) {
            this.year = year;
        }

        public ByteBuffer getBytes() {
            return bytes;
        }

        public void setBytes(ByteBuffer bytes) {
            this.bytes = bytes;
        }

        @Override
		public int hashCode() {
			return Objects.hash(this.id, this.type, this.year, this.bytes);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			EmbeddedKey other = (EmbeddedKey) obj;

            return Objects.equals(this.id, other.id) &&
                    Objects.equals(this.type, other.type) &&
                    Objects.equals(this.year, other.year) &&
                    Objects.equals(this.bytes, other.bytes);
        }
	}
}
