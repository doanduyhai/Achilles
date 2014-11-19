/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.test.integration.entity;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.type.Counter;

@Entity(table = CompleteBean.TABLE_NAME, comment = "Table for testing main operations: 'insert', 'update', 'delete', 'find'")
public class CompleteBean {

    public static final String TABLE_NAME = "CompleteBean";
    @Id
    private Long id;

    @Column
    @Index
    private String name;

    @Column
    private String label;

    @Column(name = "age_in_years")
    private Long age;

    @EmptyCollectionIfNull
    @Column
    private List<String> friends;

    @Column
    private Set<String> followers;

    @Column
    private Map<Integer, String> preferences;

    @Column
    @JSON
    private Tweet welcomeTweet;

    @Column
    private Counter version;

    @Column
    @JSON
    private List<Tweet> favoriteTweets;

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

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<String> getFriends() {
        return friends;
    }

    public void setFriends(List<String> friends) {
        this.friends = friends;
    }

    public Set<String> getFollowers() {
        return followers;
    }

    public void setFollowers(Set<String> followers) {
        this.followers = followers;
    }

    public Map<Integer, String> getPreferences() {
        return preferences;
    }

    public void setPreferences(Map<Integer, String> preferences) {
        this.preferences = preferences;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }

    public Tweet getWelcomeTweet() {
        return welcomeTweet;
    }

    public void setWelcomeTweet(Tweet welcomeTweet) {
        this.welcomeTweet = welcomeTweet;
    }

    public Counter getVersion() {
        return version;
    }

    public void setVersion(Counter version) {
        this.version = version;
    }

    public List<Tweet> getFavoriteTweets() {
        return favoriteTweets;
    }

    public void setFavoriteTweets(List<Tweet> favoriteTweets) {
        this.favoriteTweets = favoriteTweets;
    }
}
