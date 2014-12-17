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

import java.nio.ByteBuffer;
import java.util.Objects;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithPrimitiveAndSubTypes.TABLE_NAME;

@Entity(table = TABLE_NAME)
public class ClusteredEntityWithPrimitiveAndSubTypes {

	public static final String TABLE_NAME = "clustered_with_primitive__and_sub_types";

	@EmbeddedId
	private ClusteredKey id;


	public ClusteredKey getId() {
		return id;
	}

	public void setId(ClusteredKey id) {
		this.id = id;
	}


	public ClusteredEntityWithPrimitiveAndSubTypes() {
	}

	public ClusteredEntityWithPrimitiveAndSubTypes(Long id, int bucket, String date) {
		this.id = new ClusteredKey(id, bucket, ByteBuffer.wrap(date.getBytes()));
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.id);
    }

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClusteredEntityWithPrimitiveAndSubTypes other = (ClusteredEntityWithPrimitiveAndSubTypes) obj;

        return Objects.equals(this.id, other.id) ;
    }

	public static class ClusteredKey {
		@PartitionKey(1)
		private Long id;

        @PartitionKey(2)
		private int bucket;

        @ClusteringColumn
        private ByteBuffer date;

		public ClusteredKey() {
		}

		public ClusteredKey(Long id, int bucket, ByteBuffer date) {
			this.id = id;
            this.bucket = bucket;
            this.date = date;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public int getBucket() {
            return bucket;
        }

        public void setBucket(int bucket) {
            this.bucket = bucket;
        }

        public ByteBuffer getDate() {
            return date;
        }

        public void setDate(ByteBuffer date) {
            this.date = date;
        }

        @Override
		public int hashCode() {
            return Objects.hash(this.id, this.bucket, this.date);
        }

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ClusteredKey other = (ClusteredKey) obj;

            return Objects.equals(this.id, other.id) &&
                    Objects.equals(this.bucket, other.bucket) &&
                    Objects.equals(this.date, other.date);
        }

	}
}
