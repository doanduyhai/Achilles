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
package info.archinnov.achilles.internal.proxy.wrapper.builder;

import java.util.Map;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.proxy.wrapper.MapEntryWrapper;

public class MapEntryWrapperBuilder extends AbstractWrapperBuilder<MapEntryWrapperBuilder> {
	private final Map.Entry<Object, Object> target;

	public MapEntryWrapperBuilder(PersistenceContext context, Map.Entry<Object, Object> target) {
		super.context = context;
		this.target = target;
	}

	public static MapEntryWrapperBuilder builder(PersistenceContext context, Map.Entry<Object, Object> target) {
		return new MapEntryWrapperBuilder(context, target);
	}

	public MapEntryWrapper build() {
		MapEntryWrapper wrapper = new MapEntryWrapper(this.target);
		super.build(wrapper);
		return wrapper;
	}

}
