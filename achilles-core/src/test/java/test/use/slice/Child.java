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

package test.use.slice;

public class Child<CLASS> extends SimpleRoot<CLASS,Child<CLASS>> {

    public Child(Class<CLASS> clazz) {
        super(clazz);
    }

    public Child<CLASS> doSomething() {
        return Child.this;
    }

    public Child<CLASS> thenSomething() {
        return Child.this;
    }


    @Override
    protected Child<CLASS> getThis() {
        return Child.this;
    }
}
