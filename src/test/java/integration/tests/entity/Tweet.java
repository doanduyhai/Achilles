package integration.tests.entity;

import java.util.UUID;
import javax.persistence.Table;
import mapping.entity.UserBean;
import fr.doan.achilles.entity.type.WideMap;

@Table
public class Tweet {

    private UUID id;

    private UserBean creator;

    private String content;

    private WideMap<Long, UserBean> inTimelineOfUsers;

}
