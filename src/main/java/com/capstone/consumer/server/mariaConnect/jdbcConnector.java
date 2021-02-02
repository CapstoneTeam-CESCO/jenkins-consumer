//package com.capstone.consumer.server.mariaConnect;
//
//import com.capstone.consumer.server.common.LogUtil;
//
//import java.sql.*;
//
//
//public class jdbcConnector{
//    private static final String DB_DRIVER_CLASS = "org.mariadb.jdbc.Driver";
//    private static final String DB_URL = "jdbc:mariadb://127.0.0.1:3306/pest";
//    private static final String DB_USERNAME = "root";
//    private static final String DB_PASSWORD = "root";
//
//    private static Connection conn;
//    private static PreparedStatement pstmt;
//
//    public static void connectDB() {
//        try {
//            Class.forName(DB_DRIVER_CLASS);
//            conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
//            System.out.println("연결성공");
//        } catch (ClassNotFoundException e) {
//            // TODO Auto-generated catch block
//            System.out.println("드라이브 로딩 실패");
//        } catch (SQLException e) {
//            // TODO Auto-generated catch block
//            System.out.println("DB 연결 실패");
//        }
//    }
//
//    public static void insert(String contents) throws SQLException {
//
//
//    }
//}