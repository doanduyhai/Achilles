package info.archinnov.achilles.internal.metadata.transcoding.codec;

import static org.fest.assertions.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JSONCodecTest {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void should_encode_and_decode() throws Exception {
        //Given
        JSONCodec<MyPojo> codec = JSONCodec.create(mapper, MyPojo.class);

        //When
        String encoded = codec.encode(new MyPojo(10L, "name"));
        MyPojo decoded = codec.decode("{\"id\":11,\"name\":\"Test\"}");

        //Then
        assertThat(encoded).isEqualTo("{\"id\":10,\"name\":\"name\"}");
        assertThat(decoded.getId()).isEqualTo(11L);
        assertThat(decoded.getName()).isEqualTo("Test");
    }

    @JsonAutoDetect
    public static class MyPojo {

        private Long id;
        private String name;

        public MyPojo() {
        }

        public MyPojo(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}