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
package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.entity.metadata.util.CascadeMergeFilter;
import info.archinnov.achilles.entity.metadata.util.CascadePersistFilter;
import info.archinnov.achilles.entity.metadata.util.CascadeRefreshFilter;
import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Objects;

public class JoinProperties {
	public static CascadePersistFilter hasCascadePersist = new CascadePersistFilter();
	public static CascadeMergeFilter hasCascadeMerge = new CascadeMergeFilter();
	public static CascadeRefreshFilter hasCascadeRefresh = new CascadeRefreshFilter();

	private EntityMeta entityMeta;
	private Set<CascadeType> cascadeTypes = new LinkedHashSet<CascadeType>();

	public EntityMeta getEntityMeta() {
		return entityMeta;
	}

	public void setEntityMeta(EntityMeta entityMeta) {
		this.entityMeta = entityMeta;
	}

	public Set<CascadeType> getCascadeTypes() {
		return cascadeTypes;
	}

	public void setCascadeTypes(Set<CascadeType> cascadeTypes) {
		this.cascadeTypes = cascadeTypes;
	}

	public void addCascadeType(CascadeType cascadeType) {
		this.cascadeTypes.add(cascadeType);
	}

	public void addCascadeType(Collection<CascadeType> cascadeTypesCollection) {
		if (cascadeTypesCollection.contains(CascadeType.REMOVE)) {
			throw new AchillesBeanMappingException(
					"CascadeType.REMOVE is not supported for join columns");
		}
		this.cascadeTypes.addAll(cascadeTypesCollection);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass())
				.add("entityMeta", entityMeta)
				.add("cascadeTypes", StringUtils.join(cascadeTypes, ","))
				.toString();
	}
}
