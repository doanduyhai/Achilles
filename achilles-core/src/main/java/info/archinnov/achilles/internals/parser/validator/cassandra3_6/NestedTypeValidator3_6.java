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

package info.archinnov.achilles.internals.parser.validator.cassandra3_6;

import static info.archinnov.achilles.internals.apt.AptUtils.containsAnnotation;
import static info.archinnov.achilles.internals.apt.AptUtils.getShortname;
import static info.archinnov.achilles.internals.parser.TypeUtils.getRawType;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.AnnotationTree;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.NestedTypeValidator2_1;
import info.archinnov.achilles.type.tuples.Tuple;

public class NestedTypeValidator3_6 extends NestedTypeValidator2_1 {

    @Override
    public void validate(AptUtils aptUtils, AnnotationTree annotationTree, String fieldName, TypeName rawClass) {
        validateIndexAnnotation(aptUtils, annotationTree, fieldName, rawClass);

        final TypeMirror currentType = aptUtils.erasure(annotationTree.getCurrentType());

        if (aptUtils.isAssignableFrom(Tuple.class, currentType) || containsAnnotation(annotationTree, JSON.class)) {
            // Do not validate nested types for Tuples because
            // they are @Frozen by default
            // Do not validate nested types for JSON transformation
            return;
        } else if (aptUtils.isAssignableFrom(List.class, currentType)
                || aptUtils.isAssignableFrom(Set.class, currentType)) {
            validateNestedType(aptUtils, annotationTree, fieldName, rawClass);
        } else if (aptUtils.isAssignableFrom(Map.class, currentType)) {
            validateMapKeys(aptUtils, annotationTree, fieldName, rawClass);
            AnnotationTree next = annotationTree;
            for (int i = 0; i < 2; i++) {
                next = validateNestedType(aptUtils, next, fieldName, rawClass);
            }
        }
    }

    @Override
    public void validateUDT(AptUtils aptUtils, UDTMetaSignature udtMetaSignature, String fieldName, TypeName rawClass) {
        super.validateUDT(aptUtils, udtMetaSignature, fieldName, rawClass);
        if (!udtMetaSignature.isFrozen) {
            udtMetaSignature.fieldMetaSignatures
                    .stream()
                    .filter(x -> x.isCollection())
                    .forEach(x -> aptUtils.validateTrue(x.context.columnInfo.frozen,
                            "Collection type %s of field %s.%s should has @Frozen annotation because %s.%s is a non-frozen UDT",
                            getShortname(getRawType(x.sourceType)), getShortname(rawClass), fieldName + "." + x.context.fieldName,
                            getShortname(rawClass), fieldName));

        }
    }
}