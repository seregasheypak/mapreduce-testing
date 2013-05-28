package ru.develburau.mrtesting.movielens.sql.integration;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import ru.develburau.mrtesting.movielens.model.Movie;
import ru.develburau.mrtesting.movielens.model.Rating;
import ru.develburau.mrtesting.movielens.sql.HiveConnector;
import ru.develburau.mrtesting.movielens.sql.HiveDSL;
import ru.develburau.mrtesting.movielens.sql.integration.base.MovieLensBaseTest;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * User: sergey.sheypak
 * Date: 05.05.13
 * Time: 15:34
 */
public class MoviesWithHighestRatingInTotalTest extends MovieLensBaseTest {

    private final Logger LOG = LoggerFactory.getLogger(MoviesWithHighestRatingInTotalTest.class);

    @Test(groups = INTEGRATION, enabled = false)
    public void selectMoviesWithHighestRatingInTotal() throws SQLException {
        Connection connection = HiveConnector.INSTANCE.getConnection();
        HiveDSL hiveDSL = new HiveDSL(connection);
        //List<MovieRatingBean> result =
        Result<Record2<String, BigDecimal>> result =
        hiveDSL.get().select(Movie.TITLE, DSL.sum(Rating.RATING).as("sumrating"))
                     .from(Rating.TABLE)
                     .join(Movie.TABLE).on(Movie.ID.eq(Rating.MOVIE_ID))
                     .groupBy(Rating.MOVIE_ID, Movie.TITLE)
                     .orderBy(DSL.fieldByName("sumrating").desc())
                     .limit(10)
                     .fetch();//.into(MovieRatingBean.class);
          for(Record2<String, BigDecimal> r2 : result){
            LOG.debug("{}---{}", r2.getValue(1), r2.getValue(2));
          }
//        for(MovieRatingBean movieRatingBean : result){
//            LOG.debug("{}", movieRatingBean);
//        }
    }

    class MovieRatingBean{
        private final String title;
        private final BigDecimal sumrating;

        MovieRatingBean(String title, BigDecimal sumrating) {
            this.title = title;
            this.sumrating = sumrating;
        }

        String getTitle() {
            return title;
        }

        BigDecimal getSumrating() {
            return sumrating;
        }

        @Override
        public String toString(){
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }
}
