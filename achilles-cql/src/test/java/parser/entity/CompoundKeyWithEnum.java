package parser.entity;

import info.archinnov.achilles.annotations.MultiKey;
import info.archinnov.achilles.annotations.Order;

@MultiKey
public class CompoundKeyWithEnum
{

    @Order(1)
    private Long userId;

    @Order(2)
    private Gender gender;

    public CompoundKeyWithEnum() {
    }

    public CompoundKeyWithEnum(Long userId, Gender gender) {
        this.userId = userId;
        this.gender = gender;
    }

    public Long getUserId()
    {
        return userId;
    }

    public void setUserId(Long userId)
    {
        this.userId = userId;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public static enum Gender
    {
        MALE, FEMALE, TRANS
    }
}
