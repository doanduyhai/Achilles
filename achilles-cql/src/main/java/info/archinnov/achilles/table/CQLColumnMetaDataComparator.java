package info.archinnov.achilles.table;

import java.util.List;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ComparisonChain;

public class CQLColumnMetaDataComparator {

    public boolean isEqual(ColumnMetadata source, ColumnMetadata target) {
        boolean isEqual = false;
        final String sourceName = source.getName();
        final TableMetadata sourceTable = source.getTable();
        final DataType sourceType = source.getType();
        final Class<?> sourceClass = sourceType.asJavaClass();
        final List<DataType> sourceTypeParams = sourceType.getTypeArguments();

        final String targetName = target.getName();
        final TableMetadata targetTable = target.getTable();
        final DataType targetType = target.getType();
        final Class<?> targetClass = targetType.asJavaClass();
        final List<DataType> targetTypeParams = targetType.getTypeArguments();

        final boolean isPartiallyEqual = ComparisonChain.start()
                                           .compare(sourceName, targetName)
                                           .compare(sourceTable.getName(), targetTable.getName())
                                           .compare(sourceType.getName(), targetType.getName())
                                           .compareFalseFirst(sourceType.isCollection(), targetType.isCollection())
                                           .result() == 0;
        final boolean isSameClass = sourceClass.equals(targetClass);
        final boolean bothHaveTypeParameters = (sourceTypeParams != null && targetTypeParams != null) || (sourceTypeParams == null && targetTypeParams == null);

        isEqual = isPartiallyEqual && isSameClass && bothHaveTypeParameters;
        if(isEqual && sourceTypeParams != null) {
            isEqual = (sourceTypeParams.size() == targetTypeParams.size());
            if(isEqual) {
                for(int i=0; i<sourceTypeParams.size(); i++) {
                    final DataType sourceParamType = sourceTypeParams.get(i);
                    final DataType targetParamType = targetTypeParams.get(i);

                    final boolean sameParamType = ComparisonChain.start()
                            .compare(sourceParamType.getName(),targetParamType.getName())
                            .result() == 0;
                    if(!sameParamType) {
                        return false;
                    }
                }
            }
        }
        return isEqual;
    }
}
