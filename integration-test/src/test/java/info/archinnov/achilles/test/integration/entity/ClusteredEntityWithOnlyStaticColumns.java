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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithOnlyStaticColumns.TABLE_NAME;

import info.archinnov.achilles.annotations.*;

@Entity(table = TABLE_NAME)
public class ClusteredEntityWithOnlyStaticColumns {

	public static final String TABLE_NAME = "clustered_with_only_static_columns";

	@EmbeddedId
	private ClusteredOnlyStaticColumnsKey id;

	@Column(staticColumn = true)
	private String city;

    @Column(staticColumn = true)
    private String street;

    public ClusteredEntityWithOnlyStaticColumns() {
    }

    public ClusteredEntityWithOnlyStaticColumns(ClusteredOnlyStaticColumnsKey id, String city, String street) {
		this.id = id;
        this.city = city;
        this.street = street;
    }

	public ClusteredOnlyStaticColumnsKey getId() {
		return id;
	}

	public void setId(ClusteredOnlyStaticColumnsKey id) {
		this.id = id;
	}

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public static class ClusteredOnlyStaticColumnsKey {
		@PartitionKey
		private Long id;

        @ClusteringColumn
		private String location;

		public ClusteredOnlyStaticColumnsKey() {
		}

		public ClusteredOnlyStaticColumnsKey(Long id, String location) {
			this.id = id;
			this.location = location;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ClusteredOnlyStaticColumnsKey that = (ClusteredOnlyStaticColumnsKey) o;

            if (!id.equals(that.id)) {
                return false;
            }
            if (!location.equals(that.location)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + location.hashCode();
            return result;
        }
    }
}
