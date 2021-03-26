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

package info.archinnov.achilles.internals.parser;

import static info.archinnov.achilles.internals.parser.TypeUtils.getRawType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.processing.RoundEnvironment;

import com.google.auto.common.MoreElements;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.CodecRegistry;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.CodecFactory.CodecInfo;
import info.archinnov.achilles.internals.parser.context.FieldParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;

public class CodecRegistryParser extends AbstractBeanParser{

    private final CodecFactory codecFactory;

    public CodecRegistryParser(AptUtils aptUtils) {
        super(aptUtils);
        codecFactory = new CodecFactory(aptUtils);
    }

    public void parseCodecs(RoundEnvironment roundEnv, GlobalParsingContext parsingContext) {
        Map<TypeName, CodecInfo> map = new HashMap<>();
        roundEnv.getElementsAnnotatedWith(CodecRegistry.class)
            .stream()
            .map(MoreElements::asType)
            .forEach(typeElm -> {
                final String className = typeElm.getQualifiedName().toString();
                extractCandidateFields(typeElm, FieldFilter.CODEC_RELATED_ANNOTATIONS).forEach(varElm -> {
                    final TypeName typeName = getRawType(TypeName.get(varElm.asType()));
                    final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, parsingContext, varElm);
                    final FieldParsingContext fieldParsingContext = FieldParsingContext
                            .forConfig(parsingContext, typeElm, typeName, className, varElm.getSimpleName().toString());

                    final CodecInfo codec = codecFactory.createCodec(typeName, annotationTree, fieldParsingContext, Optional.empty());
                    if (map.containsKey(codec.sourceType)) {
                        aptUtils.printError("There is already a codec for source type %s in the class %s",
                                codec.sourceType, className);
                    } else {
                        map.put(codec.sourceType, codec);
                    }
                });
            });
        parsingContext.codecRegistry.putAll(map);
    }

}
