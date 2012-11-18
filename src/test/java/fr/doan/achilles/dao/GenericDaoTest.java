package fr.doan.achilles.dao;

import static fr.doan.achilles.metadata.PropertyType.LIST;
import static fr.doan.achilles.metadata.PropertyType.MAP;
import static fr.doan.achilles.metadata.PropertyType.SET;
import static fr.doan.achilles.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class GenericDaoTest {

    @InjectMocks
    private final GenericDao<Long> dao = new GenericDao<Long>();

    @Mock
    private ExecutingKeyspace keyspace;

    @Mock
    private final Serializer<Long> serializer = Utils.LONG_SRZ;

    @Test
    public void should_build_mutator() throws Exception {

        Mutator<Long> mutator = dao.buildMutator();
        assertThat(mutator).isNotNull();
    }

    @Test
    public void should_build_composite_for_simple_property() throws Exception {

        Composite comp = dao.buildCompositeForProperty("name", SIMPLE, 0);

        assertThat(comp.getComponent(0).getValue()).isEqualTo(SIMPLE.flag());
        assertThat(comp.getComponent(1).getValue()).isEqualTo("name");
        assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
    }

    @Test
    public void should_build_composite_for_list_property() throws Exception {

        Composite comp = dao.buildCompositeForProperty("friends", LIST, 0);

        assertThat(comp.getComponent(0).getValue()).isEqualTo(LIST.flag());
        assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
        assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
    }

    @Test
    public void should_build_composite_for_set_property() throws Exception {

        Composite comp = dao.buildCompositeForProperty("followers", SET, 12345);

        assertThat(comp.getComponent(0).getValue()).isEqualTo(SET.flag());
        assertThat(comp.getComponent(1).getValue()).isEqualTo("followers");
        assertThat(comp.getComponent(2).getValue()).isEqualTo(12345);
    }

    @Test
    public void should_build_composite_for_map_property() throws Exception {

        Composite comp = dao.buildCompositeForProperty("preferences", MAP, -123933);

        assertThat(comp.getComponent(0).getValue()).isEqualTo(MAP.flag());
        assertThat(comp.getComponent(1).getValue()).isEqualTo("preferences");
        assertThat(comp.getComponent(2).getValue()).isEqualTo(-123933);
    }

}
