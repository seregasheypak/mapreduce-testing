package ru.develburau.mrtesting.movielens.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * User: sergey.sheypak
 * Date: 02.05.13
 * Time: 14:32
 */
public class Rating {

    private static final String RATINGS = "ratings";

    public static final Field<Integer>  USER_ID =   DSL.fieldByName(Integer.class,  RATINGS, "user_id");
    public static final Field<Integer>  MOVIE_ID=   DSL.fieldByName(Integer.class,  RATINGS, "movie_id");
    public static final Field<Double>    RATING =    DSL.fieldByName(Double.class,    RATINGS, "rating");
    public static final Field<Long>     TSTAMP =    DSL.fieldByName(Long.class,     RATINGS, "tstamp");

    public static final Table<?> TABLE = DSL.tableByName(RATINGS);

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
}
