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

package info.archinnov.achilles.internals.codegen;

import java.util.Collections;
import java.util.List;
import javax.lang.model.element.TypeElement;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.EntityParser;
import info.archinnov.achilles.internals.parser.FieldParser;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public interface TypeParsingResultConsumer {

    static List<FieldParser.FieldMetaSignature> getTypeParsingResults(AptUtils aptUtils, TypeElement typeElement,
                                                                      GlobalParsingContext context) {
        final EntityParser parser = new EntityParser(aptUtils);
        final FieldParser fieldParser = new FieldParser(aptUtils);
        return parser.parseFields(typeElement, fieldParser, Collections.emptyList(), context);
    }

    static List<FieldParser.FieldMetaSignature> getTypeParsingResults(AptUtils aptUtils, TypeElement typeElement,
                                                                      List<AccessorsExclusionContext> exclusionContexts,
                                                                      GlobalParsingContext context) {
        final EntityParser parser = new EntityParser(aptUtils);
        final FieldParser fieldParser = new FieldParser(aptUtils);
        return parser.parseFields(typeElement, fieldParser, exclusionContexts, context);
    }
}
