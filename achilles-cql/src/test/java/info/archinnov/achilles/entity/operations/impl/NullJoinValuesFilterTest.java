package info.archinnov.achilles.entity.operations.impl;

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.MethodInvoker;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import testBuilders.PropertyMetaTestBuilder;
import com.google.common.collect.Collections2;

public class NullJoinValuesFilterTest {

    private NullJoinValuesFilter filter = new NullJoinValuesFilter();

    @Mock
    private MethodInvoker invoker;

    private CompleteBean entity = new CompleteBean();

    @Before
    public void setUp()
    {
    }

    @Test
    public void should_return_list_when_join_value_exist() throws Exception
    {
        PropertyMeta<?, ?> joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .build();
        UserBean user = new UserBean();

        Pair<List<?>, PropertyMeta<?, ?>> pair = Pair.<List<?>, PropertyMeta<?, ?>> create(Arrays.asList(user),
                joinSimpleMeta);
        Collection<Pair<List<?>, PropertyMeta<?, ?>>> filtered = Collections2.filter(
                Arrays.asList(pair), filter);

        assertThat(filtered).hasSize(1);

    }

    @Test
    public void should_filter_out_empty_join_values_list() throws Exception
    {
        PropertyMeta<?, ?> pm = new PropertyMeta<Void, String>();
        UserBean user = new UserBean();

        Pair<List<?>, PropertyMeta<?, ?>> pair1 = Pair.<List<?>, PropertyMeta<?, ?>> create(
                Arrays.asList(), pm);
        Pair<List<?>, PropertyMeta<?, ?>> pair2 = Pair.<List<?>, PropertyMeta<?, ?>> create(
                Arrays.asList(user), pm);

        List<Pair<List<?>, PropertyMeta<?, ?>>> list = Arrays.asList(pair1, pair2);

        Collection<Pair<List<?>, PropertyMeta<?, ?>>> filtered = Collections2.filter(list,
                filter);

        assertThat(filtered).containsOnly(pair2);

    }
}
