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

package info.archinnov.achilles.internals.sample_classes.config;

import info.archinnov.achilles.annotations.Codec;
import info.archinnov.achilles.annotations.CodecRegistry;
import info.archinnov.achilles.internals.sample_classes.codecs.SimpleLongWrapperCodec;
import info.archinnov.achilles.internals.sample_classes.codecs.SimpleLongWrapperCodec2;
import info.archinnov.achilles.internals.sample_classes.types.SimpleLongWrapper;

@CodecRegistry
public class TestCodecRegistryWrong {

    private @Codec(SimpleLongWrapperCodec.class) SimpleLongWrapper longWrapper;

    @Codec(SimpleLongWrapperCodec2.class)
    private SimpleLongWrapper stringWrapper;

}
