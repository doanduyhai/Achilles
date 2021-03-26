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

package info.archinnov.achilles.internals.parser.validator.cassandra3_0;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.parser.FieldParser;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;

public class BeanValidator3_0 extends BeanValidator {

    @Override
    public void validateViewsAgainstBaseTable(AptUtils aptUtils, List<EntityMetaCodeGen.EntityMetaSignature> viewSignatures, List<EntityMetaCodeGen.EntityMetaSignature> entitySignatures) {

        final Map<TypeName, List<FieldParser.FieldMetaSignature>> entitySignaturesMap = entitySignatures
                .stream()
                .collect(toMap(meta -> meta.entityRawClass, meta -> meta.fieldMetaSignatures));

        for (EntityMetaCodeGen.EntityMetaSignature view : viewSignatures) {
            final TypeName viewBaseClass = view.viewBaseClass.get();
            final List<FieldParser.FieldMetaSignature> entityParsingResults = entitySignaturesMap.get(viewBaseClass);
            aptUtils.validateTrue(entityParsingResults != null,
                    "Cannot find base entity class '%s' for view class '%s'", viewBaseClass, view.entityRawClass);

            // Validate all view columns are in base and have correct name & types
            for (FieldParser.FieldMetaSignature vpr : view.fieldMetaSignatures) {
                final long count = entityParsingResults.stream().filter(epr -> epr.equalsTo(vpr)).count();
                aptUtils.validateTrue(count == 1, "Cannot find any match in base table for field '%s' in view class '%s'",
                        vpr.toStringForViewCheck(), view.entityRawClass);
            }

            final List<FieldParser.FieldMetaSignature> viewPKColumns = view.fieldMetaSignatures
                    .stream()
                    .filter(vpr -> vpr.context.columnType == ColumnType.PARTITION || vpr.context.columnType == ColumnType.CLUSTERING)
                    .collect(toList());

            final List<FieldParser.FieldMetaSignature> basePKColumns = entityParsingResults
                    .stream()
                    .filter(vpr -> vpr.context.columnType == ColumnType.PARTITION || vpr.context.columnType == ColumnType.CLUSTERING)
                    .collect(toList());

            final List<FieldParser.FieldMetaSignature> baseCollectionColumns = entityParsingResults
                    .stream()
                    .filter(vpr -> vpr.targetType instanceof ParameterizedTypeName)
                    .filter(vpr -> {
                        final ClassName rawType = ((ParameterizedTypeName) vpr.targetType).rawType;
                        return rawType.equals(TypeUtils.LIST) || rawType.equals(TypeUtils.SET) ||
                                rawType.equals(TypeUtils.MAP) || rawType.equals(TypeUtils.JAVA_DRIVER_UDT_VALUE_TYPE);
                    })
                    .collect(toList());


            // Validate all base PK columns are in view PK columns
            for (FieldParser.FieldMetaSignature epr : basePKColumns) {
                final long count = viewPKColumns.stream().filter(vpr -> vpr.equalsTo(epr)).count();
                aptUtils.validateTrue(count == 1, "Primary key column '%s' in base class %s is not found in view class '%s' as primary key column",
                        epr.toStringForViewCheck(), epr.context.entityRawType, view.entityRawClass);
            }

            // Validate collections in base should be in view
            for (FieldParser.FieldMetaSignature epr : baseCollectionColumns) {
                final long count = view.fieldMetaSignatures.stream().filter(vpr -> vpr.equalsTo(epr)).count();
                aptUtils.validateTrue(count == 1, "Collection/UDT column '%s' in base class %s is not found in view class '%s'. It should be included in the view",
                        epr.toStringForViewCheck(), epr.context.entityRawType, view.entityRawClass);
            }

            // Validate max 1 non-PK column from base in view PK
            final List<FieldParser.FieldMetaSignature> viewPKColumnNotInBase = new ArrayList<>();
            for (FieldParser.FieldMetaSignature vpr : viewPKColumns) {
                final long count = basePKColumns.stream().filter(epr -> epr.equalsTo(vpr)).count();
                if(count == 0) viewPKColumnNotInBase.add(vpr);
            }
            aptUtils.validateTrue(viewPKColumnNotInBase.size() <= 1, "There should be maximum 1 column in the view %s primary key " +
                            "that is NOT a primary column of the base class '%s'. We have %s", view.entityRawClass, viewBaseClass,
                    viewPKColumnNotInBase.stream().map(x -> x.toStringForViewCheck()).collect(toList()));

        }
    }

    @Override
    public void validateNoStaticColumnsForView(AptUtils aptUtils, TypeName rawClassType, List<FieldParser.FieldMetaSignature> parsingResults) {
        final boolean hasStatic = parsingResults
                .stream()
                .filter(x -> (x.context.columnType == ColumnType.STATIC || x.context.columnType == ColumnType.STATIC_COUNTER))
                .count() > 0;


        aptUtils.validateFalse(hasStatic, "The class '%s' cannot have static columns because it is a materialized view", rawClassType);
    }

}
