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

package info.archinnov.achilles.internals.metamodel.columns;

import java.util.List;

public class ComputedColumnInfo extends ColumnInfo {

    public final String functionName;
    public final String alias;
    public final List<String> functionArgs;
    public final Class<?> cqlClass;

    public ComputedColumnInfo(String functionName, String alias, List<String> functionArgs, Class<?> cqlClass) {
        super(false);
        this.functionName = functionName;
        this.alias = alias;
        this.functionArgs = functionArgs;
        this.cqlClass = cqlClass;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ComputedColumnInfo{");
        sb.append("functionName='").append(functionName).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", functionArgs=").append(functionArgs);
        sb.append(", cqlClass=").append(cqlClass);
        sb.append('}');
        return sb.toString();
    }
}
