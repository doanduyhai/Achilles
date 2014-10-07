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
package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithAllTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.net.InetAddresses;

public class SupportedTypesIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(EntityWithAllTypes.TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_persist_and_find_all_types() throws Exception {
		// Given
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		byte[] bytes = "toto".getBytes(Charsets.UTF_8);
		Date now = new Date();
		InetAddress inetAddress = InetAddresses.forString("192.168.0.1");

		EntityWithAllTypes entity = new EntityWithAllTypes();
		entity.setId(id);

		entity.setPrimitiveByte((byte) 7);
		entity.setObjectByte((byte) 7);
		entity.setByteArray(bytes);
		entity.setByteBuffer(ByteBuffer.wrap(bytes));

		entity.setPrimitiveBool(true);
		entity.setObjectBool(true);

		entity.setDate(now);

		entity.setPrimitiveDouble(1.0d);
		entity.setObjectDouble(1.0d);
		entity.setBigDecimal(new BigDecimal(1.11));

		entity.setPrimitiveFloat(1.0f);
		entity.setObjectFloat(1.0f);

		entity.setInetAddress(inetAddress);

		entity.setBigInt(new BigInteger("10"));
		entity.setPrimitiveInt(10);
		entity.setObjectInt(10);

		entity.setPrimitiveLong(10L);

		// When
		manager.insert(entity);
		EntityWithAllTypes found = manager.find(EntityWithAllTypes.class, id);

		// Then
		assertThat(found.getPrimitiveByte()).isEqualTo((byte) 7);
		assertThat(found.getObjectByte()).isEqualTo((byte) 7);
		assertThat(found.getByteArray()).isEqualTo(bytes);
		assertThat(found.getByteBuffer()).isEqualTo(ByteBuffer.wrap(bytes));

		assertThat(found.isPrimitiveBool()).isTrue();
		assertThat(found.getObjectBool()).isTrue();

		assertThat(found.getDate()).isEqualTo(now);

		assertThat(found.getPrimitiveDouble()).isEqualTo(1.0d);
		assertThat(found.getObjectDouble()).isEqualTo(1.0d);
		assertThat(found.getBigDecimal()).isEqualTo(new BigDecimal(1.11));

		assertThat(found.getPrimitiveFloat()).isEqualTo(1.0f);
		assertThat(found.getObjectFloat()).isEqualTo(1.0f);

		assertThat(found.getInetAddress()).isEqualTo(inetAddress);

		assertThat(found.getBigInt()).isEqualTo(new BigInteger("10"));
		assertThat(found.getPrimitiveInt()).isEqualTo(10);
		assertThat(found.getObjectInt()).isEqualTo(10);

		assertThat(found.getPrimitiveLong()).isEqualTo(10L);
	}
}
