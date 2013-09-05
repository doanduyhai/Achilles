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

import static org.fest.assertions.api.Assertions.assertThat;

import javax.persistence.CascadeType;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class JoinPropertiesTest {
	@Test
	public void should_to_string() throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClassName("className");

		JoinProperties props = new JoinProperties();
		props.setEntityMeta(entityMeta);
		props.addCascadeType(CascadeType.MERGE);
		props.addCascadeType(CascadeType.PERSIST);

		StringBuilder toString = new StringBuilder();
		toString.append("JoinProperties [entityMeta=className, ");
		toString.append("cascadeTypes=[")
				.append(StringUtils.join(props.getCascadeTypes(), ","))
				.append("]]");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}
}
