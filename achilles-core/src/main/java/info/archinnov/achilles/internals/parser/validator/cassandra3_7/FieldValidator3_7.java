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

package info.archinnov.achilles.internals.parser.validator.cassandra3_7;

import static info.archinnov.achilles.annotations.SASI.Analyzer.NO_OP_ANALYZER;
import static info.archinnov.achilles.annotations.SASI.IndexMode.SPARSE;
import static info.archinnov.achilles.internals.parser.TypeUtils.STRING;
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
import info.archinnov.achilles.internals.parser.context.SASIInfoContext;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.FieldValidator2_1;

public class FieldValidator3_7 extends FieldValidator2_1 {

    @Override
    public void validateCompatibleIndexAnnotationsOnField(GlobalParsingContext context, AptUtils aptUtils, String fieldName, TypeName rawEntityClass,
                                                          Optional<Index> index, Optional<SASI> sasi, Optional<DSE_Search> dseSearch) {

        super.validateCompatibleIndexAnnotationsOnField(context, aptUtils, fieldName, rawEntityClass, index, sasi, dseSearch);
        checkNoMutuallyExclusiveAnnotations(aptUtils, fieldName, rawEntityClass, asList(index, sasi, dseSearch));
    }

    @Override
    public void validateSASIIndex(AptUtils aptUtils, FieldMetaSignature fieldMetaSignature) {
        SASIInfoContext sasiInfoContext = fieldMetaSignature.context.indexInfo.sasiInfoContext.get();
        final String fieldName = fieldMetaSignature.context.fieldName;
        final TypeName entityRawType = fieldMetaSignature.context.entityRawType;

        aptUtils.validateFalse(fieldMetaSignature.isCollection(),
                "The target type %s of field %s from entity %s is a collection (list/set/map). " +
                "@SASI is not allowed because collections are not (yet) supported",
                fieldMetaSignature.targetType.toString(), fieldName, entityRawType.toString());

        aptUtils.validateFalse(fieldMetaSignature.isUDT(),
                "The target type %s of field %s from entity %s is an UDT. " +
                "@SASI is not allowed because UDT are not supported",
                fieldMetaSignature.targetType.toString(), fieldName, entityRawType.toString());

        if (sasiInfoContext.analyzed == true) {
            aptUtils.validateTrue(fieldMetaSignature.targetType.equals(STRING),
                    "The target type %s of field %s from entity %s is not text/ascii so " +
                    "@SASI option 'analyzed' should be false AND 'analyzerClass' should be NO_OP_ANALYZER",
                    fieldMetaSignature.targetType.toString(), fieldName, entityRawType.toString());

            aptUtils.validateFalse(sasiInfoContext.indexMode.equals(SPARSE),
                    "The @SASI option 'indexMode' for field %s from entity %s cannot be SPARSE " +
                    "because @SASI option 'analyzed' = true",
                    fieldName, entityRawType.toString());

            aptUtils.validateFalse(sasiInfoContext.analyzerClass.equals(NO_OP_ANALYZER),
                    "The @SASI option 'analyzerClass' for field %s from entity %s cannot be " +
                    "NO_OP_ANALYZER because @SASI option 'analyzed' = true",
                    fieldName, entityRawType);

        }

        if(!sasiInfoContext.analyzerClass.equals(NO_OP_ANALYZER)) {
            aptUtils.validateTrue(fieldMetaSignature.targetType.equals(STRING),
                    "The target type %s of field %s from entity %s should be text/ascii because " +
                    "@SASI option 'analyzerClass' is %s",
                    fieldMetaSignature.targetType.toString(), fieldName,
                    entityRawType.toString(), sasiInfoContext.analyzerClass.name());

            aptUtils.validateFalse(sasiInfoContext.indexMode.equals(SPARSE),
                    "The @SASI option 'indexMode' for field %s from entity %s cannot be SPARSE " +
                    "because @SASI option 'analyzerClass' = %s",
                    fieldName, entityRawType.toString(), sasiInfoContext.analyzerClass.name());

            aptUtils.validateTrue(sasiInfoContext.analyzed,
                    "The @SASI option 'analyzed' for field %s from entity %s cannot be false " +
                    "because @SASI option 'analyzerClass' = %s",
                    fieldName, entityRawType, sasiInfoContext.analyzerClass.name());

        }

        if (!sasiInfoContext.normalization.equals(SASI.Normalization.NONE)) {
            aptUtils.validateTrue(fieldMetaSignature.targetType.equals(STRING),
                    "The target type %s of field %s from entity %s should be text/ascii because " +
                    "@SASI option 'normalization' is %s",
                    fieldMetaSignature.targetType.toString(), fieldName,
                    entityRawType.toString(), sasiInfoContext.normalization.name());

            aptUtils.validateFalse(sasiInfoContext.indexMode.equals(SPARSE),
                    "The @SASI option 'indexMode' for field %s from entity %s cannot be SPARSE " +
                    "because @SASI option 'normalization' = %s",
                    fieldName, entityRawType.toString(), sasiInfoContext.normalization.name());

            aptUtils.validateTrue(sasiInfoContext.analyzed,
                    "The @SASI option 'analyzed' for field %s from entity %s cannot be false " +
                    "because @SASI option 'normalization' = %s",
                    fieldName, entityRawType, sasiInfoContext.normalization.name());

            aptUtils.validateFalse(sasiInfoContext.analyzerClass.equals(NO_OP_ANALYZER),
                    "The @SASI option 'analyzerClass' for field %s from entity %s cannot be " +
                    "NO_OP_ANALYZER because @SASI option 'normalization' = %s",
                    fieldName, entityRawType, sasiInfoContext.normalization.name());
        }

        if (sasiInfoContext.enableStemming || sasiInfoContext.skipStopWords) {
            aptUtils.validateTrue(fieldMetaSignature.targetType.equals(STRING),
                    "The target type %s of field %s from entity %s should be text/ascii because " +
                    "@SASI options 'enableStemming'/'skipStopWords' are true",
                    fieldMetaSignature.targetType.toString(), fieldName,
                    entityRawType.toString());

            aptUtils.validateFalse(sasiInfoContext.indexMode.equals(SPARSE),
                    "The @SASI option 'indexMode' for field %s from entity %s cannot be SPARSE " +
                    "because @SASI options 'enableStemming'/'skipStopWords' are true",
                    fieldName, entityRawType.toString());

            aptUtils.validateTrue(sasiInfoContext.analyzed,
                    "The @SASI option 'analyzed' for field %s from entity %s cannot be false " +
                    "because @SASI options 'enableStemming'/'skipStopWords' are true",
                    fieldName, entityRawType);

            aptUtils.validateTrue(sasiInfoContext.analyzerClass.equals(SASI.Analyzer.STANDARD_ANALYZER),
                    "The @SASI option 'analyzerClass' for field %s from entity %s should be " +
                    "STANDARD_ANALYZER because @SASI options 'enableStemming'/'skipStopWords' are true",
                    fieldName, entityRawType);
        }

        if (sasiInfoContext.indexMode == SPARSE) {
            aptUtils.validateFalse(fieldMetaSignature.targetType.equals(STRING),
                "The @SASI 'indexMode' SPARSE is incompatible with data type %s for field %s of entity %s",
                fieldMetaSignature.targetType, fieldName, entityRawType);
        }
    }
}
