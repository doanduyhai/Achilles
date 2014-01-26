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

import java.util.Map.Entry;
import java.util.Set;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.proxy.wrapper.EntrySetWrapper;

public class EntrySetWrapperBuilder extends AbstractWrapperBuilder<EntrySetWrapperBuilder> {
	private Set<Entry<Object, Object>> target;

	public static EntrySetWrapperBuilder builder(PersistenceContext context, Set<Entry<Object, Object>> target) {
		return new EntrySetWrapperBuilder(context, target);
	}

	public EntrySetWrapperBuilder(PersistenceContext context, Set<Entry<Object, Object>> target) {
		super.context = context;
		this.target = target;
	}

	public EntrySetWrapper build() {
		EntrySetWrapper entrySetWrapper = new EntrySetWrapper(this.target);
		super.build(entrySetWrapper);
		return entrySetWrapper;
	}
}
