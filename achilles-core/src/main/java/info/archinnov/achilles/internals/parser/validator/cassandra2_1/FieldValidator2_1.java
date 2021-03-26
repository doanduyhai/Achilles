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

package info.archinnov.achilles.internals.parser.validator.cassandra2_1;

import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.DSE_SEARCH;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.SASI_INDEX;
import static java.util.Arrays.asList;

import java.util.List;
import java.util.Optional;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.DSE_Search;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.SASI;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;

public class FieldValidator2_1 extends FieldValidator {

    @Override
    public List<TypeName> getAllowedTypes() {
        return TypeUtils.ALLOWED_TYPES_2_1;
    }

    @Override
    public void validateCompatibleIndexAnnotationsOnField(GlobalParsingContext context, AptUtils aptUtils, String fieldName, TypeName rawEntityClass,
                                                          Optional<Index> index, Optional<SASI> sasi, Optional<DSE_Search> dseSearch) {

        if (sasi.isPresent()) {
            aptUtils.validateTrue(context.supportsFeature(SASI_INDEX),
                    "@SASI annotation is not allowed if using Cassandra version %s", context.cassandraVersion.version());
        }

        if (dseSearch.isPresent()) {
            aptUtils.validateTrue(context.supportsFeature(DSE_SEARCH),
                    "@DSE_Search annotation is not allowed if using Cassandra version %s. " +
                    "Consider setting your Cassandra version to DSE_X_X", context.cassandraVersion.version());
        }

        checkNoMutuallyExclusiveAnnotations(aptUtils, fieldName, rawEntityClass, asList(index, dseSearch));
    }

    @Override
    public void validateSASIIndex(AptUtils aptUtils, FieldMetaSignature fieldMetaSignature) {
        //NO Op
    }

    @Override
    public void validateDSESearchIndex(AptUtils aptUtils, FieldMetaSignature fieldMetaSignature) {
        //NO Op
    }
}
