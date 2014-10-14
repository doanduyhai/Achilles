package info.archinnov.achilles.query.cql;

import com.datastax.driver.core.Row;
import info.archinnov.achilles.type.TypedMap;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TypedMapIterator implements Iterator<TypedMap> {

    private final Iterator<Row> sourceIterator;
    NativeQueryMapper mapper = NativeQueryMapper.Singleton.INSTANCE.get();

    TypedMapIterator(Iterator<Row> sourceIterator) {
        this.sourceIterator = sourceIterator;
    }

    @Override
    public boolean hasNext() {
        return sourceIterator.hasNext();
    }

    @Override
    public TypedMap next() {
        if(!sourceIterator.hasNext()) throw new NoSuchElementException("No more data. You should call 'hasNext' first to ensure there is remaining data to fetch");
        return mapper.mapRow(sourceIterator.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove element from a TypedMapIterator");
    }
}
