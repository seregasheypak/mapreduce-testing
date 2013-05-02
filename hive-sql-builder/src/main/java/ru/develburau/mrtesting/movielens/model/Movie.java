package ru.develburau.mrtesting.movielens.model;

import org.jooq.Field;
import org.jooq.impl.Factory;

import java.util.List;

/**
 * User: sergey.sheypak
 * Date: 25.04.13
 * Time: 22:46
 */
public class Movie {

    private int id;
    private String title;
    private List<String> genres;

    public Movie() {
    }

    public Movie(int id, String title, List<String> genres) {
        this.id = id;
        this.title = title;
        this.genres = genres;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getGenres() {
        return genres;
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }

    public int hashCode(){
        return Integer.valueOf(id).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Movie movie = (Movie) o;

        if (id != movie.id) return false;

        return true;
    }

    public static enum SQL{

            USER_ID("id", Integer.class )
        ,   TITLE("title", String.class)
        ,   GENRES("genres", String.class)
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
