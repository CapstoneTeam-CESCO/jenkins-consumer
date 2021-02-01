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
        catch (Exception e){
            e.printStackTrace();
        }

//        jdbcTemplate.execute("INSERT INTO TESTING VALUES (1,'Dong')");
    }

//    TODO: 디비에 맞는 INSERT 구현

//     try {
//         String SQL = "INSERT INTO Notices(device_id,type,contents,is_readed,created_at) VALUES (?,?,?,?,?)";
//
//         pstmt = conn.prepareStatement(SQL);
//         // pstmt.setInt(1,);  // id
//         pstmt.setNull(1,Types.INTEGER); // device_id
//         pstmt.setNull(2,Types.VARCHAR); // type
//         pstmt.setString(3,contents); // contents
//         pstmt.setBoolean(4,false); // is_readed
//         pstmt.setNull(5, Types.DATE); //created_at
//
//        int r = pstmt.executeUpdate();
//        System.out.println("변경된 row : "+ r);
//        pstmt.close();
//    } catch(SQLException e) {
//         LogUtil.traceLog.error(String.valueOf(e));
//     }

}
