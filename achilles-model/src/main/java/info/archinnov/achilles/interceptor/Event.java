/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.interceptor;

/**
 * <p/>
 * Events representing entity lifecycle. Their name is self-explanatory.
 * <p/>
 * Each event is bound to one or several methods of the <strong>PersistenceManager</strong> object
 * <br>
 * <table border="1">
 * <thead><tr>
 * <th>Operation</th>
 * <th>Possible Events</th>
 * </tr></thead>
 * <tbody>
 * <tr>
 * <td>persist()</td>
 * <td>PRE_PERSIST/POST_PERSIST</td>
 * </tr>
 * <tr>
 * <td>update()</td>
 * <td>PRE_UPDATE/POST_UPDATE</td>
 * </tr>
 * <tr>
 * <td>remove()</td>
 * <td>PRE_REMOVE/POST_REMOVE</td>
 * </tr>
 * <tr>
 * <td>find()</td>
 * <td>POST_LOAD</td>
 * </tr>
 * <tr>
 * <td>sliceQuery()</td>
 * <td>POST_LOAD</td>
 * </tr>
 * <tr>
 * <td>typedQuery()</td>
 * <td>POST_LOAD</td>
 * </tr>
 * <tr>
 * <td>rawTypedQuery()</td>
 * <td>POST_LOAD</td>
 * </tr>
 * </tbody>
 * </table>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Interceptors" target="_blank">Interceptors</a>
 */
public enum Event {
    PRE_PERSIST, POST_PERSIST, PRE_UPDATE, POST_UPDATE, PRE_REMOVE, POST_REMOVE, POST_LOAD;
}
