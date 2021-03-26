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

package info.archinnov.achilles.schema;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.reflections.vfs.Vfs;

import com.google.common.collect.Lists;

/**
 * Class taken from here: https://gist.github.com/nonrational/287ed109bb0852f982e8
 * Inspired heavily by
 *
 * <link>https://git-wip-us.apache.org/repos/asf?p=isis.git;a=blob;f=core/applib/src/main/java/org/apache/isis/applib/services/classdiscovery/
 * ClassDiscoveryServiceUsingReflections.java;h=283f053ddb15bfe32f111d88891602820854415e;hb=283f053ddb15bfe32f111d88891602820854415e</link>
 */
public class ReflectionsHelper {

    /**
     * OSX contains file:// resources on the classpath including .mar and .jnilib files.
     *
     * Reflections use of Vfs doesn't recognize these URLs and logs warns when it sees them. By registering those file endings, we supress the warns.
     */
    public static void registerUrlTypes(String...typesToSkip) {

        final List<Vfs.UrlType> urlTypes = Lists.newArrayList();

        // include a list of file extensions / filenames to be recognized
        urlTypes.add(new EmptyIfFileEndingsUrlType(typesToSkip));

        urlTypes.addAll(Arrays.asList(Vfs.DefaultUrlTypes.values()));

        Vfs.setDefaultURLTypes(urlTypes);
    }

    private static class EmptyIfFileEndingsUrlType implements Vfs.UrlType {

        private final List<String> fileEndings;

        private EmptyIfFileEndingsUrlType(final String... fileEndings) {

            this.fileEndings = Lists.newArrayList(fileEndings);
        }

        public boolean matches(URL url) {

            final String protocol = url.getProtocol();
            final String externalForm = url.toExternalForm();
            if (!protocol.equals("file")) {
                return false;
            }
            for (String fileEnding : fileEndings) {
                if (externalForm.endsWith(fileEnding))
                    return true;
            }
            return false;
        }

        public Vfs.Dir createDir(final URL url) throws Exception {

            return emptyVfsDir(url);
        }

        private static Vfs.Dir emptyVfsDir(final URL url) {

            return new Vfs.Dir() {
                @Override
                public String getPath() {

                    return url.toExternalForm();
                }

                @Override
                public Iterable<Vfs.File> getFiles() {

                    return Collections.emptyList();
                }

                @Override
                public void close() {

                }
            };
        }
    }
}
