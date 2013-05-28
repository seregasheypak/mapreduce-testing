package ru.develburau.mrtesting.movielens.sql.integration;

import org.jooq.Record;
import org.jooq.impl.DSL;
import org.testng.annotations.Test;
import ru.develburau.mrtesting.movielens.model.Movie;
import ru.develburau.mrtesting.movielens.sql.HiveConnector;
import ru.develburau.mrtesting.movielens.sql.HiveDSL;
import ru.develburau.mrtesting.movielens.sql.integration.base.MovieLensBaseTest;

import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * User: sergey.sheypak
 * Date: 04.05.13
 * Time: 15:20
 */
public class CountMoviesTest extends MovieLensBaseTest {

    @Test(groups = INTEGRATION, enabled = false)
    public void countMovies() throws SQLException {
        Connection connection = HiveConnector.INSTANCE.getConnection();
        HiveDSL hiveDSL = new HiveDSL(connection);
        Record rowCount = hiveDSL.get().select(DSL.count().as("moviescount")).from(Movie.TABLE).fetchOne();
        Integer count = rowCount.getValue(DSL.fieldByName(Integer.class, "moviescount"));
        assertThat("It's supposed that dataset on movielens is not changed.", count, equalTo(10681));
    }
}
