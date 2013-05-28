package ru.develburau.mrtesting.movielens.model;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.Arrays;
import java.util.List;

/**
 * User: sergey.sheypak
 * Date: 25.04.13
 * Time: 22:46
 */
public class Movie {

    private static final String MOVIES = "movies";

    public static final Field<Integer> ID      = DSL.fieldByName(Integer.class, MOVIES, "id");
    public static final Field<String>  TITLE   = DSL.fieldByName(String.class,  MOVIES, "title");
    public static final Field<String>  GENRES  = DSL.fieldByName(String.class,  MOVIES, "genres");


    public static final Table<?> TABLE = DSL.tableByName(MOVIES);

    private static final String GENRES_SEPARATOR = "\\|";
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

    public void setGenres(String genres) {
        if(genres!=null && !genres.isEmpty()){
            this.genres = Arrays.asList(genres.split(GENRES_SEPARATOR));
        }
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
}
