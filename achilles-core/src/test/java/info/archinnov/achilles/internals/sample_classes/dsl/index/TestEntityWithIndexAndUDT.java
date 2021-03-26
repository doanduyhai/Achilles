/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.sample_classes.dsl.index;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;

@APUnitTest
@Table(keyspace = "manager_codegen", table = "index_and_udt")
public class TestEntityWithIndexAndUDT {

  @PartitionKey
  private Long id;

  @Index
  @Column
  private String indexedText;

  @Column
  @Frozen
  private TestUDT udt;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getIndexedText() {
    return indexedText;
  }

  public void setIndexedText(String indexedText) {
    this.indexedText = indexedText;
  }

  public TestUDT getUdt() {
    return udt;
  }

  public void setUdt(TestUDT udt) {
    this.udt = udt;
  }
}
