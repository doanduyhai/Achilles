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

package info.archinnov.achilles.internals.parser.validator;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.util.*;
import javax.lang.model.element.*;
import javax.lang.model.util.ElementFilter;

import org.apache.commons.collections.map.HashedMap;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.annotations.Immutable;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.ConstructorInfo;
import info.archinnov.achilles.type.tuples.Tuple2;

public abstract class BeanValidator {

    public final List<String> RESERVED_KEYWORDS = Arrays.asList(
            ("add,allow,alter,and,any,apply,asc,authorize,batch,begin,by,columnfamily,create,delete,desc,drop,each_quorum,from,grant,in,index,inet,infinity," +
                    "insert,into,keyspace,keyspaces,limit,local_one,local_quorum,modify,nan,norecursive,of,on,order,primary,quorum,rename,revoke,schema," +
                    "select,set,table,three,to,token,truncate,two,unlogged,update,use,using,where,with")
                    .split(","));

    public void validateEntityNames(AptUtils aptUtils, List<TypeElement> entityTypes) {
        Map<String, String> entities = new HashedMap();
        for (TypeElement entityType : entityTypes) {
            final String className = entityType.getSimpleName().toString();
            final String FQCN = entityType.getQualifiedName().toString();
            if (entities.containsKey(className)) {
                final String existingFQCN = entities.get(className);
                aptUtils.printError("%s and %s both share the same class name, it is forbidden by Achilles",
                        FQCN, existingFQCN);
                throw new IllegalStateException(format("%s and %s both share the same class name, it is forbidden by Achilles",
                        FQCN, existingFQCN));
            } else {
                entities.put(className, FQCN);
            }
        }
    }

    public void validateIsAConcreteClass(AptUtils aptUtils, TypeElement typeElement) {
        final Name name = typeElement.getQualifiedName();
        aptUtils.validateTrue(typeElement.getKind() == ElementKind.CLASS, "Bean type '%s' should be a class", name);
        final Set<Modifier> modifiers = typeElement.getModifiers();
        aptUtils.validateFalse(modifiers.contains(Modifier.ABSTRACT), "Bean type '%s' should not be abstract", name);
    }

    public ConstructorInfo validateConstructor(AptUtils aptUtils, TypeName typeName, TypeElement typeElement) {

        final Optional<Immutable> immutable = aptUtils.getAnnotationOnClass(typeElement, Immutable.class);

        final long customConstructorCount = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                .stream()
                .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                .filter(x -> x.getParameters().size() > 0) // multi arg(s) constructor
                .count();

        final long defaultConstructorCount = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                .stream()
                .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                .filter(x -> x.getParameters().size() == 0) //No arg constructor
                .count();

        final long entityCreatorConstructorCount = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                .stream()
                .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                .filter(x -> x.getParameters().size() > 0) // multi arg(s) constructor
                .filter(x -> x.getAnnotation(EntityCreator.class) != null) //has @EntityCreator annotation
                .count();


        aptUtils.validateTrue((immutable.isPresent() && customConstructorCount == 1 && defaultConstructorCount == 0) ||
                (entityCreatorConstructorCount == 1) ||
                (defaultConstructorCount == 1), "Entity type '%s' should:\n\t" +
                "1. either be annotated with @Immutable, have NO default constructor and EXACTLY 1 custom constructor\n\t" +
                "2. or have EXACTLY 1 custom constructor annotated with @EntityCreator and optionally one default constructor and multiple other custom constructors\n\t" +
                "3. or have EXACTLY 1 default constructor and one or more custom constructor(s) (nominal scenario)\n", typeName);

        if(immutable.isPresent() && customConstructorCount == 1 && defaultConstructorCount == 0) {
            final ExecutableElement constructor = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                    .stream()
                    .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                    .filter(x -> x.getParameters().size() > 0) // multi arg(s) constructor
                    .findFirst().get();
            return ConstructorInfo.immutable(constructor);
        } else if (entityCreatorConstructorCount == 1) {
            final ExecutableElement constructor = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                    .stream()
                    .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                    .filter(x -> x.getParameters().size() > 0) // multi arg(s) constructor
                    .filter(x -> x.getAnnotation(EntityCreator.class) != null) //has @EntityCreator annotation
                    .findFirst().get();

            return ConstructorInfo.entityCreator(constructor);
        } else if (defaultConstructorCount == 1) {
            final ExecutableElement constructor = ElementFilter.constructorsIn(typeElement.getEnclosedElements())
                    .stream()
                    .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                    .filter(x -> x.getParameters().size() == 0) //No arg constructor
                    .findFirst().get();
            return ConstructorInfo.defaultConstructor(constructor);
        } else {
            aptUtils.printError("Cannot determine appropriate constructor type for entity type '%s'", typeName);
            throw new IllegalStateException(format("Cannot determine appropriate constructor type for entity type '%s'", typeElement));
        }
    }

    public void validateNoDuplicateNames(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
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

    public void validateCqlColumnNotReservedWords(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        parsingResults
                .stream()
                .map(x -> Tuple2.of(x.context.cqlColumn, x.context.fieldName))
                .forEach(x -> aptUtils.validateFalse(RESERVED_KEYWORDS.contains(x._1().toLowerCase()),
                        "The cql column '%s' on field '%s' of class '%s' is a CQL reserved word and cannot be used",
                        x._1(), x._2(), rawClassType));
    }

    public void validateStaticColumns(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
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

    public void validateNoStaticColumnsForView(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        // No op
    }

    public void validateHasPartitionKey(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
        final boolean hasPartitionKey = parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .count() > 0;

        aptUtils.validateTrue(hasPartitionKey,
                "The class '%s' should have at least 1 partition key (@PartitionKey)", rawClassType);
    }

    public boolean isCounterTable(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
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

    public void validateComputed(AptUtils aptUtils, TypeName rawClassType, List<FieldMetaSignature> parsingResults) {
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

    public void validateViewsAgainstBaseTable(AptUtils aptUtils, List<EntityMetaSignature> viewSignatures, List<EntityMetaSignature> entitySignatures) {
        // No op by default
    }

}
