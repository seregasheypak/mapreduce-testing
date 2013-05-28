package ru.develburau.mrtesting.movielens.sql;

import org.jooq.ExecuteContext;
import org.jooq.impl.DefaultExecuteListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * User: sergey.sheypak
 * Date: 03.05.13
 * Time: 18:09
 */
public class HiveLimitExecuteListener extends DefaultExecuteListener {

    private static final Logger LOG = LoggerFactory.getLogger(HiveLimitExecuteListener.class);

    @Override
    public void renderEnd(ExecuteContext ctx){
        if(ctx.sql().contains("offset ?")){
            ctx.sql(ctx.sql().replace("offset ?", ""));
        }
    }

    @Override
    public void bindStart(ExecuteContext ctx){
        LOG.debug("query[{}]", ctx.sql());
        LOG.debug("bindValues[{}]", ctx.query().getBindValues());
//        if(ctx.sql().contains("limit ?")){
//            List<Object> bindValues = ctx.query().getBindValues();
//            Connection connection = ctx.connection();
//            PreparedStatement preparedStatement = connection.prepareStatement(ctx.sql());
//
//
//        }
    }
}
