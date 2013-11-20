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
package info.archinnov.achilles.entity.parsing;

import info.archinnov.achilles.annotations.Entity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityExplorer {
	private static final Logger log = LoggerFactory.getLogger(EntityExplorer.class);

	public List<Class<?>> discoverEntities(List<String> packageNames) {
		log.debug("Discovery of Achilles entity classes in packages {}", StringUtils.join(packageNames, ","));

		Set<Class<?>> candidateClasses = new HashSet<Class<?>>();
		Reflections reflections = new Reflections(packageNames);
		candidateClasses.addAll(reflections.getTypesAnnotatedWith(Entity.class));
		return new ArrayList<Class<?>>(candidateClasses);
	}

}
