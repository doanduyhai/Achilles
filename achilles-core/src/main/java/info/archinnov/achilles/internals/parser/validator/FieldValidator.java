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

import static info.archinnov.achilles.internals.apt.AptUtils.containsAnnotation;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.summingInt;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.metamodel.columns.KeyColumnInfo;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.CodecContext;
import info.archinnov.achilles.internals.parser.context.FieldParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.type.tuples.Tuple2;


public abstract class FieldValidator {

    public abstract List<TypeName> getAllowedTypes();

    public abstract void validateCompatibleIndexAnnotationsOnField(GlobalParsingContext context, AptUtils aptUtils,
                                                                   String fieldName, TypeName rawEntityClass,
                                                                   Optional<Index> index, Optional<SASI> sasi,
                                                                   Optional<DSE_Search> dseSearch);

    public abstract void validateSASIIndex(AptUtils aptUtils, FieldMetaSignature fieldMetaSignature);
    public abstract void validateDSESearchIndex(AptUtils aptUtils, FieldMetaSignature fieldMetaSignature);

    public void checkNoMutuallyExclusiveAnnotations(AptUtils aptUtils, String fieldName, TypeName rawEntityClass,
                                                            List<Optional<? extends Annotation>> annotations) {
        annotations
            .stream()
            .filter(annotation -> annotation.isPresent())
            .forEach(annotation -> {
                final ArrayList<Optional<? extends Annotation>> shifted = new ArrayList<>(annotations);
                shifted.remove(annotation);
                shifted
                    .stream()
                    .filter(shiftedAnnotation -> shiftedAnnotation.isPresent())
                    .forEach(shiftedAnnotation -> {
                        final String annot1 = "@" + annotation.get().annotationType().getSimpleName();
                        final String annot2 = "@" + shiftedAnnotation.get().annotationType().getSimpleName();
                        aptUtils.printError("Field '%s' in class '%s' cannot have both %s AND %s annotations", fieldName, rawEntityClass,
                                annot1, annot2);
            });
        });
    }

    public void checkNoMutuallyExclusiveCodecAnnotations(AptUtils aptUtils, String fieldName, Name rawEntityClass,
                                                            List<? extends Annotation> annotations) {
        final ArrayList<Annotation> shifted = new ArrayList<>(annotations);
        final Annotation first = shifted.remove(0);
        shifted.add(first);

        for(int i=0; i<annotations.size(); i++) {
            final Annotation annotation = annotations.get(i);
            final Annotation shiftedAnnotation = shifted.get(i);
            if (annotation != null && shiftedAnnotation != null) {
                final String annot1 = "@" + annotation.annotationType().getSimpleName();
                final String annot2 = "@" + shiftedAnnotation.annotationType().getSimpleName();
                aptUtils.printError("Cannot have both %s and % annotation on the same field '%s' in class '%s'",
                        fieldName, rawEntityClass, annot1, annot2);
            }
        }
    }

    public void checkNoMutuallyExclusiveCodecAnnotations(AptUtils aptUtils, String fieldName, Name rawEntityClass,
                                                                 Annotation left, List<? extends Annotation> right) {
        if (left != null) {
            for(int i=0; i<right.size(); i++) {
                final Annotation annotation = right.get(i);
                if (annotation != null) {
                    final String annot1 = "@" + annotation.getClass().getSimpleName();
                    final String annot2 = "@" + left.getClass().getSimpleName();
                    aptUtils.printError("Cannot have both %s and %s annotation on the same field '%s' in class '%s'",
                            fieldName, rawEntityClass, annot1, annot2);
                }
            }
        }
    }

    public void validateCompatibleColumnAnnotationsOnField(AptUtils aptUtils, String fieldName, TypeName rawEntityClass,
                                                                  Optional<PartitionKey> partitionKey, Optional<ClusteringColumn> clusteringColumn,
                                                                  Optional<Static> staticColumn, Optional<Computed> computed,
                                                                  Optional<Counter> counter) {

        checkNoMutuallyExclusiveAnnotations(aptUtils, fieldName, rawEntityClass, asList(partitionKey, clusteringColumn, staticColumn, computed));
        checkNoMutuallyExclusiveAnnotations(aptUtils, fieldName, rawEntityClass, asList(computed, counter));
    }


    public void validateCompatibleCodecAnnotationsOnField(AptUtils aptUtils, String fieldName, Name className,
                                                                 Frozen frozen, JSON json, Enumerated enumerated, Codec codec,
                                                                 RuntimeCodec runtimeCodec,
                                                                 Computed computed, Counter counter,
                                                                 TimeUUID timeUUID, ASCII ascii) {

        checkNoMutuallyExclusiveCodecAnnotations(aptUtils, fieldName, className, asList(json, codec, runtimeCodec, enumerated, frozen));

        checkNoMutuallyExclusiveCodecAnnotations(aptUtils, fieldName, className, computed, asList(frozen, json, enumerated));
        checkNoMutuallyExclusiveCodecAnnotations(aptUtils, fieldName, className, counter, asList(frozen, json, enumerated, computed));
        checkNoMutuallyExclusiveCodecAnnotations(aptUtils, fieldName, className, timeUUID, asList(frozen, json, enumerated, codec, runtimeCodec, computed, counter, ascii));
        checkNoMutuallyExclusiveCodecAnnotations(aptUtils, fieldName, className, ascii, asList(frozen, json, enumerated, codec, runtimeCodec, computed, counter, timeUUID));

    }

    public void validateAllowedFrozen(boolean isFrozen, AptUtils aptUtils, VariableElement elm, String fieldName, TypeName rawClass) {
        if (isFrozen) {
            aptUtils.validateTrue(aptUtils.isCompositeTypeForCassandra(elm.asType()),
                    "@Frozen annotation on field '%s' of class '%s' is only allowed for collections and UDT",
                    fieldName, rawClass);
        }
    }

    public void validateAllowedType(AptUtils aptUtils, TypeName rawTargetType, FieldParsingContext context) {
        aptUtils.validateTrue(getAllowedTypes().contains(rawTargetType),
                "Impossible to parse type '%s' from field '%s' of class '%s'. It should be a supported type",
                rawTargetType.toString(), context.fieldName, context.className);
    }

    public void validateCounter(AptUtils aptUtils, TypeName targetType, Set<Class<? extends Annotation>> annotations, FieldParsingContext context) {
        if (containsAnnotation(annotations, Counter.class)) {
            aptUtils.validateTrue(targetType.box().equals(TypeName.LONG.box()),
                    "Field '%s' of class '%s' annotated with @Counter should be of type Long/long",
                    context.fieldName, context.className);
        }
    }

    public void validateCorrectKeysOrder(AptUtils aptUtils, TypeName rawClassName, List<Tuple2<String, KeyColumnInfo>> keyTuples, String type) {
        /**
         * Math formula : sum of N consecutive integers = N * (N+1)/2
         */
        int checkForKeyOrdering = (keyTuples.size() * (keyTuples.size() + 1)) / 2;
        final Integer sumOfOrders = keyTuples.stream()
                .map(x -> x._2())
                .collect(summingInt(x -> x.order()));
        aptUtils.validateTrue(checkForKeyOrdering == sumOfOrders, "The %s ordering is wrong in class '%s'", type, rawClassName);
    }

    public CodecContext validateCodec(AptUtils aptUtils, TypeValidator typeValidator, CodecContext codecContext, TypeName sourceType,
                                             Optional<TypeName> cqlClass, boolean isCounter) {
        final String codecClass = codecContext.codecType.toString();

        aptUtils.validateTrue(sourceType.box().equals(codecContext.sourceType.box()), "Codec '%s' source type '%s' should match current object type '%s'",
                codecClass, codecContext.sourceType, sourceType.toString());
        if (cqlClass.isPresent()) {
            aptUtils.validateTrue(codecContext.targetType.box().equals(cqlClass.get().box()), "Codec '%s' target type '%s' should match computed CQL type '%s'",
                    codecClass, codecContext.targetType, cqlClass.get());
        }
        if (isCounter) {
            aptUtils.validateTrue(codecContext.targetType.box().equals(TypeName.LONG.box()),
                    "Codec '%s' target type '%s' should be Long/long because the column is annotated with @Counter",
                    codecClass, codecContext.targetType);
        }
        typeValidator.validateAllowedTypes(aptUtils, sourceType, codecContext.targetType);

        return codecContext;
    }
}
