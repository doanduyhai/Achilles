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
package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.KeySetWrapper;

import java.util.Set;

public class KeySetWrapperBuilder extends
		AbstractWrapperBuilder<KeySetWrapperBuilder> {
	private Set<Object> target;

	public KeySetWrapperBuilder(PersistenceContext context, Set<Object> target) {
		super.context = context;
		this.target = target;
	}

	public static KeySetWrapperBuilder builder(PersistenceContext context,
			Set<Object> target) {
		return new KeySetWrapperBuilder(context, target);
	}

	public KeySetWrapper build() {
		KeySetWrapper keySetWrapper = new KeySetWrapper(this.target);
		super.build(keySetWrapper);
		return keySetWrapper;
	}

}
