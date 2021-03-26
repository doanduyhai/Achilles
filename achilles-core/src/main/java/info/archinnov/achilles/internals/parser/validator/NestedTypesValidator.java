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

import java.util.Map;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.AnnotationTree;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;

public abstract class NestedTypesValidator {

    public abstract void validate(AptUtils aptUtils, AnnotationTree annotationTree, String fieldName, TypeName rawClass);
    public abstract void validateUDT(AptUtils aptUtils, UDTMetaSignature udtMetaSignature, String fieldName, TypeName rawClass);

    public void validateMapKeys(AptUtils aptUtils, AnnotationTree annotationTree, String fieldName, TypeName rawClass) {
        final TypeMirror currentType = aptUtils.erasure(annotationTree.getCurrentType());
        if (aptUtils.isAssignableFrom(Map.class, currentType)) {
            final AnnotationTree next = annotationTree.next();
            final TypeMirror mapKey = next.getCurrentType();
            if (aptUtils.isCompositeTypeForCassandra(mapKey) && !containsAnnotation(next, JSON.class)) {
                aptUtils.validateTrue(containsAnnotation(next, Frozen.class),
                        "Map key of type collection/UDT '%s' in '%s' of class '%s' should be annotated with @Frozen",
                        mapKey, fieldName, rawClass);
            }
        }
        if (annotationTree.hasNext()) {
            validateMapKeys(aptUtils, annotationTree.next(), fieldName, rawClass);
        }
    }

    public void validateIndexAnnotation(AptUtils aptUtils, AnnotationTree annotationTree, String fieldName, TypeName rawClass) {
        if (annotationTree.depth() > 2) {
            aptUtils.validateFalse(containsAnnotation(annotationTree, Index.class),
                    "@Index annotation cannot be nested for depth > 2 for field '%s' of class '%s'",
                    fieldName, rawClass);
        }
        if (annotationTree.hasNext()) {
            validateIndexAnnotation(aptUtils, annotationTree.next(), fieldName, rawClass);
        }
    }
}
