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

package info.archinnov.achilles.internal.table;

import java.util.List;

import com.datastax.driver.core.*;
import com.google.common.collect.ComparisonChain;

public class ColumnMetaDataComparator {

    private static final CodecRegistry CODEC_REGISTRY = new CodecRegistry();

    public boolean isEqual(ColumnMetadata source, ColumnMetadata target) {
        boolean isEqual;
        final String sourceName = source.getName();
        final AbstractTableMetadata sourceTable = source.getParent();
        final DataType sourceType = source.getType();
        final Class<?> sourceClass = CODEC_REGISTRY.codecFor(sourceType).getJavaType().getRawType();
        final List<DataType> sourceTypeParams = sourceType.getTypeArguments();

        final String targetName = target.getName();
        final AbstractTableMetadata targetTable = target.getParent();
        final DataType targetType = target.getType();
        final Class<?> targetClass = CODEC_REGISTRY.codecFor(targetType).getJavaType().getRawType();
        final List<DataType> targetTypeParams = targetType.getTypeArguments();

        final boolean isPartiallyEqual = ComparisonChain.start()
                                           .compare(sourceName, targetName)
                                           .compare(sourceTable.getName(), targetTable.getName())
                                           .compare(sourceType.getName(), targetType.getName())
                                           .compareFalseFirst(sourceType.isCollection(), targetType.isCollection())
                                           .result() == 0;
        final boolean isSameClass = sourceClass.equals(targetClass);
        final boolean bothHaveTypeParameters = (sourceTypeParams != null && targetTypeParams != null) || (sourceTypeParams == null && targetTypeParams == null);

        isEqual = isPartiallyEqual && isSameClass && bothHaveTypeParameters;
        if(isEqual && sourceTypeParams != null) {
            isEqual = (sourceTypeParams.size() == targetTypeParams.size());
            if(isEqual) {
                for(int i=0; i<sourceTypeParams.size(); i++) {
                    final DataType sourceParamType = sourceTypeParams.get(i);
                    final DataType targetParamType = targetTypeParams.get(i);

                    final boolean sameParamType = ComparisonChain.start()
                            .compare(sourceParamType.getName(),targetParamType.getName())
                            .result() == 0;
                    if(!sameParamType) {
                        return false;
                    }
                }
            }
        }
        return isEqual;
    }

    public static enum Singleton {
        INSTANCE;

        private final ColumnMetaDataComparator instance = new ColumnMetaDataComparator();

        public ColumnMetaDataComparator get() {
            return instance;
        }
    }
}
