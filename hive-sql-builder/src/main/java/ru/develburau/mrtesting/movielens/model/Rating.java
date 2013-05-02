package ru.develburau.mrtesting.movielens.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.jooq.Field;
import org.jooq.impl.Factory;

/**
 * User: sergey.sheypak
 * Date: 02.05.13
 * Time: 14:32
 */
public class Rating {

    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public Rating() {
    }

    public Rating(int userId, int movieId, float rating, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public float getRating() {
        return rating;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder().append(userId).append(movieId).build();
    }

    @Override
    public boolean equals(Object outer){
        if(this == outer){
            return true;
        }
        if(outer instanceof Rating){
            Rating other = (Rating)outer;
            return new EqualsBuilder().append(userId, other.userId)
                                      .append(movieId, other.movieId)
                                      .append(rating, other.rating)
                                      .append(timestamp, this.timestamp)
                                      .isEquals();
        }

        return false;
    }

    public static enum SQL{

            USER_ID("userid", Integer.class )
        ,   MOVIE_ID("movieid", Integer.class)
        ,   RATING("rating", Float.class)
        ,   TS("ts", Long.class)
        ;

        private final String fieldName;

        private final Class<?> fieldType;

        SQL(String fieldName, Class<?> fieldType){
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }

        Field field(){
            return Factory.fieldByName(fieldType, fieldName);
        }

        String fieldName(){
            return fieldName;
        }
    }
}
