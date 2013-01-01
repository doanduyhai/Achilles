package integration.tests.entity;

import java.util.UUID;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

public class TweetTestBuilder {

    private UUID id;

    private User creator;

    private String content;

    public static TweetTestBuilder tweet() {
        return new TweetTestBuilder();
    }

    public Tweet buid() {
        Tweet tweet = new Tweet();

        tweet.setId(id);
        tweet.setCreator(creator);
        tweet.setContent(content);
        return tweet;
    }

    public TweetTestBuilder id(UUID id) {
        this.id = id;
        return this;
    }

    public TweetTestBuilder randomId() {
        this.id = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        return this;
    }

    public TweetTestBuilder content(String content) {
        this.content = content;
        return this;
    }

    public TweetTestBuilder creator(User creator) {
        this.creator = creator;
        return this;
    }
}
