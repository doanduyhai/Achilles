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
package info.archinnov.achilles.internal.persistence.metadata;

import com.google.common.base.Objects;

public class CounterProperties {
	private String fqcn;
	private PropertyMeta idMeta;

	public CounterProperties(String fqcn) {
		this.fqcn = fqcn;
	}

	public CounterProperties(String fqcn, PropertyMeta idMeta) {
		this.fqcn = fqcn;
		this.idMeta = idMeta;
	}

	public String getFqcn() {
		return fqcn;
	}

	public PropertyMeta getIdMeta() {
		return idMeta;
	}

	public void setIdMeta(PropertyMeta idMeta) {
		this.idMeta = idMeta;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("fqcn", fqcn).add("idMeta", idMeta).toString();
	}
}
