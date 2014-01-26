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

package info.archinnov.achilles.internal.metadata.holder;

import com.google.common.base.Objects;

public class IndexProperties {

	private String name;
	private String propertyName;

	public IndexProperties(String name) {
		this.name = name;
	}

	public IndexProperties(String name, String propertyName) {
		this.name = name;
		this.propertyName = propertyName;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyMeta(String propertyName) {
		this.propertyName = propertyName;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("name", name).add("propertyName", propertyName).toString();
	}
}
