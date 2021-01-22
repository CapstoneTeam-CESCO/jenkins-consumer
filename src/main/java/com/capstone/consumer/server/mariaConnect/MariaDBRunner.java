package com.capstone.consumer.server.mariaConnect;

import com.capstone.consumer.server.common.LogUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Component
public class MariaDBRunner implements ApplicationRunner {

    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(ApplicationArguments args) throws Exception{
        LogUtil.traceLog.info(("MariaDB Connection..."));
        try(Connection connetion = dataSource.getConnection()){
            String dburl =  connetion.getMetaData().getURL();
            LogUtil.traceLog.info("Connected. Database URL: {}",dburl);
            Statement statement = connetion.createStatement();
            //String sql = "CREATE TABLE TESTING2(ID INTEGER NOT NULL, name VARCHAR(200))";
            //statement.executeUpdate(sql);
        }

        jdbcTemplate.execute("INSERT INTO TESTING VALUES (1,'Dong')");
    }

}
