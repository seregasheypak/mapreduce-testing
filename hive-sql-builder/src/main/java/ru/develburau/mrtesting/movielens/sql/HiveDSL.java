package ru.develburau.mrtesting.movielens.sql;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.impl.DefaultExecuteListenerProvider;

import java.sql.Connection;

/**
 * User: sergey.sheypak
 * Date: 03.05.13
 * Time: 18:05
 */
public class HiveDSL {
    private final DSLContext dsl;

    public HiveDSL(Connection connection){
        Configuration configuration = new DefaultConfiguration();
        configuration.set(SQLDialect.MYSQL);

        configuration.set(new DefaultConnectionProvider(connection));

        Settings settings = new Settings();
        settings.setRenderNameStyle(RenderNameStyle.AS_IS);
        configuration.set(settings);
        configuration.set(new DefaultExecuteListenerProvider(new HiveLimitExecuteListener()));
        dsl = DSL.using(configuration);
    }

    public DSLContext get(){
        return dsl;
    }
}
