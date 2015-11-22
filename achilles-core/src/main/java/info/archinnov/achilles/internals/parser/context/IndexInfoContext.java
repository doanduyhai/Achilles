package info.archinnov.achilles.internals.parser.context;

import static org.apache.commons.lang3.StringUtils.isBlank;

import javax.lang.model.element.VariableElement;

public class IndexInfoContext {

    public final String indexName;
    public final String indexClassName;
    public final String indexOptions;

    public IndexInfoContext(String indexName, String indexClassName, String indexOptions) {
        this.indexName = indexName;
        this.indexClassName = indexClassName;
        this.indexOptions = indexOptions;
    }

    public IndexInfoContext computeIndexName(VariableElement elm, EntityParsingContext context) {
        String newIndexName = isBlank(indexName)
                ? context.namingStrategy.apply(elm.getSimpleName().toString() + "_index")
                : indexName;
        return new IndexInfoContext(newIndexName, indexClassName, indexOptions);
    }
}
