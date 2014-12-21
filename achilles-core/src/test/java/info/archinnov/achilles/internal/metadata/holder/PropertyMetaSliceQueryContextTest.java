package info.archinnov.achilles.internal.metadata.holder;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaSliceQueryContextTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock
    private CompoundPKProperties compoundPKProperties;


    @Test
    public void should_instantiate_compound_pk_with_partition_components() throws Exception {
        //Given
        CompleteBean pk = new CompleteBean();
        List<Field> fields = Arrays.asList(CompleteBean.class.getDeclaredField("id"), CompleteBean.class.getDeclaredField("name"));
        List<Object> components = Arrays.<Object>asList(10L, "DuyHai");
        when(meta.<CompleteBean>getValueClass()).thenReturn(CompleteBean.class);
        when(meta.forValues().instantiate()).thenReturn(pk);
        when(meta.getCompoundPKProperties().getPartitionComponents().getComponentFields()).thenReturn(fields);

        PropertyMetaSliceQueryContext view = new PropertyMetaSliceQueryContext(meta);

        //When
        final Object actual = view.instantiateEmbeddedIdWithPartitionComponents(components);

        //Then
        assertThat(actual).isSameAs(pk);
        assertThat(pk.getId()).isEqualTo(10L);
        assertThat(pk.getName()).isEqualTo("DuyHai");
    }

}