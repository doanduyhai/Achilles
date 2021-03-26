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

package info.archinnov.achilles.internals.dsl;

import static info.archinnov.achilles.type.lightweighttransaction.LWTResultListener.LWTResult.LWTOperation.INSERT;
import static info.archinnov.achilles.type.lightweighttransaction.LWTResultListener.LWTResult.LWTOperation.UPDATE;
import static java.lang.String.format;

import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener.LWTResult;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener.LWTResult.LWTOperation;

public class LWTHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(LWTHelper.class);

    private static final String IF_NOT_EXIST_CLAUSE = " IF NOT EXISTS";
    private static final String IF_CLAUSE = " IF ";

    private static void notifyLWTError(Optional<List<LWTResultListener>> lwtResultListeners, LWTResult lwtResult) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Maybe notify listener of LWT error : %s",
                    lwtResult));
        }

        if (lwtResultListeners.isPresent()) {
            for (LWTResultListener listener : lwtResultListeners.get()) {
                listener.onError(lwtResult);
            }
        } else {
            throw new AchillesLightWeightTransactionException(lwtResult);
        }
    }

    private static void notifyCASSuccess(Optional<List<LWTResultListener>> lwtResultListeners) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Maybe notify listener of LWT success"));
        }
        lwtResultListeners.ifPresent(list -> list.forEach(listener -> listener.onSuccess()));
    }

    private static boolean isLWTOperation(String queryString) {
        return queryString.contains(IF_CLAUSE);
    }

    private static boolean isLWTInsert(String queryString) {
        return queryString.contains(IF_NOT_EXIST_CLAUSE);
    }

    public static ResultSet triggerLWTListeners(Optional<List<LWTResultListener>> lwtResultListeners, ResultSet resultSet, String queryString) {
        if (isLWTOperation(queryString)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Trigger LWT listeners for query : %s", queryString));
            }

            final Row lwtResult = resultSet.one();
            if (!resultSet.wasApplied()) {
                TreeMap<String, Object> currentValues = new TreeMap<>();
                for (ColumnDefinitions.Definition columnDef : lwtResult.getColumnDefinitions()) {
                    final String columnDefName = columnDef.getName();
                    Object columnValue = lwtResult.getObject(columnDefName);
                    currentValues.put(columnDefName, columnValue);
                }

                LWTOperation lwtOperation = UPDATE;
                if (isLWTInsert(queryString)) {
                    lwtOperation = INSERT;
                }
                notifyLWTError(lwtResultListeners, new LWTResult(lwtOperation, TypedMap.fromMap(currentValues)));
            } else {
                notifyCASSuccess(lwtResultListeners);
            }
        }
        return resultSet;
    }
}
