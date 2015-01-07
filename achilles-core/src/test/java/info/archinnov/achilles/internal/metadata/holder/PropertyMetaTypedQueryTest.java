package info.archinnov.achilles.internal.metadata.holder;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTypedQueryTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    private PropertyMetaTypedQuery view;

    @Before
    public void setUp() {
        view = new PropertyMetaTypedQuery(meta);
    }

    @Test
    public void should_validate_typed_query_for_compound_pk() throws Exception {
        //Given
        when(meta.structure().isCompoundPK()).thenReturn(true);
        when(meta.getCompoundPKProperties().getCQLComponentNames()).thenReturn(asList("id", "name"));
        when(meta.<CompleteBean>getCqlValueClass()).thenReturn(CompleteBean.class);

        //When
        view.validateTypedQuery("Select id, name From table");

        //Then
    }

    @Test
    public void should_validate_typed_query_for_simple_id() throws Exception {
        //Given
        when(meta.structure().isCompoundPK()).thenReturn(false);
        when(meta.getCQLColumnName()).thenReturn("id");
        when(meta.<CompleteBean>getValueClass()).thenReturn(CompleteBean.class);

        //When
        view.validateTypedQuery("Select id From table");

        //Then
    }
}