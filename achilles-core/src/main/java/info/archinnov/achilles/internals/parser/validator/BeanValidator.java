/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

package info.archinnov.achilles.internals.parser.validator;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.*;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.type.tuples.Tuple2;

public class BeanValidator {

    static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            ("add,allow,alter,and,any,apply,asc,authorize,batch,begin,by,columnfamily,create,delete,desc,drop,each_quorum,from,grant,in,index,inet,infinity," +
                    "insert,into,keyspace,keyspaces,limit,local_one,local_quorum,modify,nan,norecursive,of,on,order,password,primary,quorum,rename,revoke,schema," +
                    "select,set,table,three,to,token,truncate,two,unlogged,update,use,using,where,with")
                    .split(","));

    public static void validateIsAConcreteNonFinalClass(AptUtils aptUtils, TypeElement typeElement) {
        final Name name = typeElement.getQualifiedName();
        aptUtils.validateTrue(typeElement.getKind() == ElementKind.CLASS, "Bean type '%s' should be a class", name);
        final Set<Modifier> modifiers = typeElement.getModifiers();
        aptUtils.validateFalse(modifiers.contains(Modifier.ABSTRACT), "Bean type '%s' should not be abstract", name);
        aptUtils.validateFalse(modifiers.contains(Modifier.FINAL), "Bean type '%s' should not be final", name);
    }

    public static void validateHasPublicConstructor(AptUtils aptUtils, TypeName typeName, TypeElement typeElement) {
        final long constructorCount = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                .stream()
                .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                .filter(x -> x.getParameters().size() == 0) //No arg constructor
                .count();
        aptUtils.validateTrue(constructorCount == 1, "Bean type '%s' should have a public constructor", typeName);
    }

    public static void validateNoDuplicateNames(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        Map<String, String> mapping = new HashMap<>();
        parsingResults
                .stream()
                .map(x -> x.context)
                .forEach(context -> {
                    final String fieldName = context.fieldName;
                    final String cqlColumn = context.cqlColumn;
                    if (mapping.containsKey(fieldName)) {
                        aptUtils.printError("The class '%s' already contains a field with name '%s'", rawClassType, fieldName);
                    } else if (mapping.containsValue(cqlColumn)) {
                        aptUtils.printError("The class '%s' already contains a cql column with name '%s'", rawClassType, cqlColumn);
                    } else {
                        mapping.put(fieldName, cqlColumn);
                    }
                });
    }

    public static void validateCqlColumnNotReservedWords(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        parsingResults
                .stream()
                .map(x -> Tuple2.of(x.context.cqlColumn, x.context.fieldName))
                .forEach(x -> aptUtils.validateFalse(RESERVED_KEYWORDS.contains(x._1().toLowerCase()),
                        "The cql column '%s' on field '%s' of class '%s' is a CQL reserved word and cannot be used",
                        x._1(), x._2(), rawClassType));
    }

    public static void validateStaticColumns(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        final boolean hasStatic = parsingResults
                .stream()
                .filter(x -> (x.context.columnType == ColumnType.STATIC || x.context.columnType == ColumnType.STATIC_COUNTER))
                .count() > 0;

        final boolean hasClustering = parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.CLUSTERING)
                .count() > 0;
        if (hasStatic) {
            aptUtils.validateTrue(hasClustering,
                    "The class '%s' cannot have static columns without at least 1 clustering column", rawClassType);
        }
    }

    public static void validateNoStaticColumnsForView(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        final boolean hasStatic = parsingResults
                .stream()
                .filter(x -> (x.context.columnType == ColumnType.STATIC || x.context.columnType == ColumnType.STATIC_COUNTER))
                .count() > 0;


        aptUtils.validateFalse(hasStatic, "The class '%s' cannot have static columns because it is a materialized view", rawClassType);
    }

    public static void validateHasPartitionKey(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        final boolean hasPartitionKey = parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .count() > 0;

        aptUtils.validateTrue(hasPartitionKey,
                "The class '%s' should have at least 1 partition key (@PartitionKey)", rawClassType);
    }

    public static boolean isCounterTable(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        final boolean hasCounter = parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COUNTER)
                .count() > 0;

        final boolean hasStaticCounter = parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.STATIC_COUNTER)
                .count() > 0;

        final boolean hasNormal = parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.NORMAL)
                .count() > 0;

        if (hasCounter || hasStaticCounter) {
            aptUtils.validateFalse(hasNormal, "Class '%s' should not mix counter and normal columns", rawClassType);
        }
        return hasCounter || hasStaticCounter;
    }

    public static void validateComputed(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        List<String> fieldNames = parsingResults
                .stream()
                .map(x -> x.context.fieldName)
                .collect(toList());

        final Set<String> aliases = new HashSet<>();

        parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .map(x -> Tuple2.of(x.context.fieldName, ((ComputedColumnInfo) x.context.columnInfo).alias))
                .forEach(x -> {
                    aptUtils.validateFalse(aliases.contains(x._2()),
                            "Alias '%s' in @Computed annotation on field '%s' is already used by another @Computed field",
                            x._2(), x._1());
                    if (!aliases.contains(x._2())) aliases.add(x._2());
                });


        parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .forEach(x -> {
                    final ComputedColumnInfo columnInfo = (ComputedColumnInfo) x.context.columnInfo;
                    for (String column : columnInfo.functionArgs) {
                        aptUtils.validateTrue(fieldNames.contains(column),
                                "Target field '%s' in @Computed annotation of field '%s' of class '%s' does not exist",
                                column, x.context.fieldName, rawClassType);
                    }
                });
    }

    public static void validateViewsAgainstBaseTable(AptUtils aptUtils, List<EntityMetaSignature> viewSignatures, List<EntityMetaSignature> entitySignatures) {

        final Map<TypeName, List<FieldMetaSignature>> entitySignaturesMap = entitySignatures
                .stream()
                .collect(toMap(meta -> meta.entityRawClass, meta -> meta.parsingResults));

        for (EntityMetaSignature view : viewSignatures) {
            final TypeName viewBaseClass = view.viewBaseClass.get();
            final List<FieldMetaSignature> entityParsingResults = entitySignaturesMap.get(viewBaseClass);
            aptUtils.validateTrue(entityParsingResults != null,
                "Cannot find base entity class '%s' for view class '%s'", viewBaseClass, view.entityRawClass);

            // Validate all view columns are in base and have correct name & types
            for (FieldMetaSignature vpr : view.parsingResults) {
                final long count = entityParsingResults.stream().filter(epr -> epr.equalsTo(vpr)).count();
                aptUtils.validateTrue(count == 1, "Cannot find any match in base table for field '%s' in view class '%s'",
                  vpr.toStringForViewCheck(), view.entityRawClass);
            }

            final List<FieldMetaSignature> viewPKColumns = view.parsingResults
                    .stream()
                    .filter(vpr -> vpr.context.columnType == ColumnType.PARTITION || vpr.context.columnType == ColumnType.CLUSTERING)
                    .collect(toList());

            final List<FieldMetaSignature> basePKColumns = entityParsingResults
                    .stream()
                    .filter(vpr -> vpr.context.columnType == ColumnType.PARTITION || vpr.context.columnType == ColumnType.CLUSTERING)
                    .collect(toList());

            final List<FieldMetaSignature> baseCollectionColumns = entityParsingResults
                    .stream()
                    .filter(vpr -> vpr.targetType instanceof ParameterizedTypeName)
                    .filter(vpr -> {
                        final ClassName rawType = ((ParameterizedTypeName) vpr.targetType).rawType;
                        return rawType.equals(TypeUtils.LIST) || rawType.equals(TypeUtils.SET) ||
                                rawType.equals(TypeUtils.MAP) || rawType.equals(TypeUtils.JAVA_DRIVER_UDT_VALUE_TYPE);
                    })
                    .collect(toList());


            // Validate all base PK columns are in view PK columns
            for (FieldMetaSignature epr : basePKColumns) {
                final long count = viewPKColumns.stream().filter(vpr -> vpr.equalsTo(epr)).count();
                aptUtils.validateTrue(count == 1, "Primary key column '%s' in base class %s is not found in view class '%s' as primary key column",
                        epr.toStringForViewCheck(), epr.context.entityRawType, view.entityRawClass);
            }

            // Validate collections in base should be in view
            for (FieldMetaSignature epr : baseCollectionColumns) {
                final long count = view.parsingResults.stream().filter(vpr -> vpr.equalsTo(epr)).count();
                aptUtils.validateTrue(count == 1, "Collection/UDT column '%s' in base class %s is not found in view class '%s'. It should be included in the view",
                        epr.toStringForViewCheck(), epr.context.entityRawType, view.entityRawClass);
            }

            // Validate max 1 non-PK column from base in view PK
            final List<FieldMetaSignature> viewPKColumnNotInBase = new ArrayList<>();
            for (FieldMetaSignature vpr : viewPKColumns) {
                final long count = basePKColumns.stream().filter(epr -> epr.equalsTo(vpr)).count();
                if(count == 0) viewPKColumnNotInBase.add(vpr);
            }
            aptUtils.validateTrue(viewPKColumnNotInBase.size() <= 1, "There should be maximum 1 column in the view %s primary key " +
                "that is NOT a primary column of the base class '%s'. We have %s", view.entityRawClass, viewBaseClass,
                    viewPKColumnNotInBase.stream().map(x -> x.toStringForViewCheck()).collect(toList()));

        }

    }

}
