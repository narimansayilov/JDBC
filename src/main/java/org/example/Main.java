package org.example;


import java.sql.*;
import java.util.Scanner;

public class Main {
    private static final Scanner scanner = new Scanner(System.in);

    static String url = "jdbc:postgresql://localhost:5432/postgres";
    static String name = "postgres";
    static String password = "12345";

    public static void main(String[] args) {

        System.out.println("1 : Create new table.");
        System.out.println("2 : Add data to the table.");
        System.out.println("3 : Data searcher based on id");
        System.out.println("4 : Any sql query");

        System.out.println("Enter one of the above commands to work with the database: ");

        int command = scanner.nextInt();

        switch (command) {
            case 1 -> {
                System.out.println("Enter sql query to create table: ");
                scanner.nextLine();
                String query = scanner.nextLine();
                createNewTable(query);
            }
            case 2 -> {
                System.out.println("---Before adding data to the table, preview the columns!---");
                System.out.println("Enter table name: ");
                scanner.nextLine();
                String tableName = scanner.nextLine();
                String[] result = showTheColumnNamesAndGetData(tableName);
                addDataToTable(tableName,result[0], result[1]);
            }
            case 3 -> {
                System.out.println("To search on tables, enter the table name and user id: ");
                System.out.print("Table name: ");
                String tableName = scanner.next();
                System.out.print("User id: ");
                int id = scanner.nextInt();
                searchId(tableName, id);
            }
            case 4 -> {
                System.out.println("Enter sql query: ");
                scanner.nextLine();
                String query = scanner.nextLine();
                sqlQuery(query);
            }
            default -> System.out.println("The command you entered does not exist!");
        }
    }

    private static void createNewTable(String query) {
        try (Connection connection = DriverManager.getConnection(url, name, password)) {
            Statement statement = connection.createStatement();

            statement.executeUpdate(query);
            System.out.println("Table created!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addDataToTable(String tableName, String columnNames, String values) {
        String query = "insert into " + tableName + "(" + columnNames + ") values(" + values + ")";
        try (Connection connection = DriverManager.getConnection(url, name, password)){
            Statement statement = connection.createStatement();
            int rowsAffected = statement.executeUpdate(query);
            System.out.println(rowsAffected + " row(s) inserted successfully.");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private static String[] showTheColumnNamesAndGetData(String tableName){
        String[] result = new String[2];
        try(Connection connection = DriverManager.getConnection(url, name, password)){
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, null, tableName, null);

            String columnNames = "";
            String values = "";

            System.out.println("Column names and their data types in the " + tableName + " table: and enter value");
            while (resultSet.next()){
                String columnName = resultSet.getString("column_name");
                int dataTypeCode = resultSet.getInt("data_type");
                String dataType = getDataTypeString(dataTypeCode);
                System.out.print(columnName + " (" + dataType + ") : ");

                String data = scanner.nextLine();

                columnNames += columnName + ", ";
                if ("varchar".equals(dataType) || "date".equals(dataType) || "timestamp".equals(dataType)){
                    values += "'" + data + "', ";
                }
                else {
                    values += data + ", ";
                }
            }
            result[0] = columnNames.substring(0, columnNames.length() - 2);
            result[1] = values.substring(0, values.length() - 2);

        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    private static String getDataTypeString(int dataTypeCode){
        switch (dataTypeCode){
            case Types.INTEGER -> {
                return "int";
            }
            case Types.VARCHAR -> {
                return "varchar";
            }
            case Types.DATE -> {
                return "date";
            }
            case Types.TIMESTAMP -> {
                return "timestamp";
            }
            default -> {
                return "unknown";
            }
        }
    }

    private static void searchId(String tableName, int id){
        String query = "select * from " + tableName + " where id=" + id;
        try (Connection connection = DriverManager.getConnection(url, name, password)) {

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    System.out.println(columnName + " = " + columnValue);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void sqlQuery(String query) {
        try (Connection connection = DriverManager.getConnection(url, name, password)) {

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String value = resultSet.getString(i);
                    System.out.println(columnName + ": " + value);
                }
                System.out.println("------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



