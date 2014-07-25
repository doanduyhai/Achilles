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

import static info.archinnov.achilles.test.integration.entity.EntityWithAllTypes.TABLE_NAME;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

@Entity(table = TABLE_NAME)
public class EntityWithAllTypes {

	public static final String TABLE_NAME = "entity_with_all_types";

	@Id
	private Long id;

	@Column
	private byte primitiveByte;

	@Column
	private Byte objectByte;

	@Column
	private byte[] byteArray;

	@Column
	private ByteBuffer byteBuffer;

	@Column
	private boolean primitiveBool;

	@Column
	private Boolean objectBool;

	@Column
	private Date date;

	@Column
	private double primitiveDouble;

	@Column
	private Double objectDouble;

	@Column
	private BigDecimal bigDecimal;

	@Column
	private float primitiveFloat;

	@Column
	private Float objectFloat;

	@Column
	private InetAddress inetAddress;

	@Column
	private BigInteger bigInt;

	@Column
	private int primitiveInt;

	@Column
	private Integer objectInt;

	@Column
	private long primitiveLong;

    public EntityWithAllTypes() {
    }

    public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public byte getPrimitiveByte() {
		return primitiveByte;
	}

	public void setPrimitiveByte(byte primitiveByte) {
		this.primitiveByte = primitiveByte;
	}

	public Byte getObjectByte() {
		return objectByte;
	}

	public void setObjectByte(Byte objectByte) {
		this.objectByte = objectByte;
	}

	public byte[] getByteArray() {
		return byteArray;
	}

	public void setByteArray(byte[] byteArray) {
		this.byteArray = byteArray;
	}

	public ByteBuffer getByteBuffer() {
		return byteBuffer;
	}

	public void setByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}

	public boolean isPrimitiveBool() {
		return primitiveBool;
	}

	public void setPrimitiveBool(boolean primitiveBool) {
		this.primitiveBool = primitiveBool;
	}

	public Boolean getObjectBool() {
		return objectBool;
	}

	public void setObjectBool(Boolean objectBool) {
		this.objectBool = objectBool;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public double getPrimitiveDouble() {
		return primitiveDouble;
	}

	public void setPrimitiveDouble(double primitiveDouble) {
		this.primitiveDouble = primitiveDouble;
	}

	public Double getObjectDouble() {
		return objectDouble;
	}

	public void setObjectDouble(Double objectDouble) {
		this.objectDouble = objectDouble;
	}

	public BigDecimal getBigDecimal() {
		return bigDecimal;
	}

	public void setBigDecimal(BigDecimal bigDecimal) {
		this.bigDecimal = bigDecimal;
	}

	public float getPrimitiveFloat() {
		return primitiveFloat;
	}

	public void setPrimitiveFloat(float primitiveFloat) {
		this.primitiveFloat = primitiveFloat;
	}

	public Float getObjectFloat() {
		return objectFloat;
	}

	public void setObjectFloat(Float objectFloat) {
		this.objectFloat = objectFloat;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public BigInteger getBigInt() {
		return bigInt;
	}

	public void setBigInt(BigInteger bigInt) {
		this.bigInt = bigInt;
	}

	public int getPrimitiveInt() {
		return primitiveInt;
	}

	public void setPrimitiveInt(int primitiveInt) {
		this.primitiveInt = primitiveInt;
	}

	public Integer getObjectInt() {
		return objectInt;
	}

	public void setObjectInt(Integer objectInt) {
		this.objectInt = objectInt;
	}

	public long getPrimitiveLong() {
		return primitiveLong;
	}

	public void setPrimitiveLong(long primitiveLong) {
		this.primitiveLong = primitiveLong;
	}
}
