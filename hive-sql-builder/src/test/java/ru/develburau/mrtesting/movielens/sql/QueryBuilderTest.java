package ru.develburau.mrtesting.movielens.sql;

import org.jooq.impl.DSL;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import ru.develburau.mrtesting.movielens.model.Movie;
import ru.develburau.mrtesting.movielens.model.Rating;

import java.sql.Connection;

/**
 * User: sergey.sheypak
 * Date: 02.05.13
 * Time: 17:31
 */
public class QueryBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueryBuilderTest.class);

    @Test
    public void testSqlWithoutBindingSelectFromMoviesWithLimit(){
        Connection connection = Mockito.mock(Connection.class);
        HiveDSL hiveDSL = new HiveDSL(connection);
        String sql = hiveDSL.get().select(Movie.TITLE)
                                  .from(Movie.TABLE)
                                  .orderBy(Movie.TITLE.asc())
                                  .limit(10)
                                  .getSQL();
        LOG.debug("select query:[{}]", sql);
    }

    @Test
    public void testSqlSelectFromRatingsJoinWithMoviesWithLimit(){
        Connection connection = Mockito.mock(Connection.class);
        HiveDSL hiveDSL = new HiveDSL(connection);
        String sql = hiveDSL.get()
                     .select(Movie.TITLE, DSL.sum(Rating.RATING).as("sumrating"))
                     .from(Rating.TABLE)
                     .join(Movie.TABLE).on(Movie.ID.eq(Rating.MOVIE_ID))
                     .groupBy(Rating.MOVIE_ID)
                     .orderBy(DSL.field("sumrating").desc())
                     .limit(10)
                     .getSQL();
        LOG.debug("select query:[{}]", sql);
    }
}
