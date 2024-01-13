package org.hellcat;

import java.sql.*;
import java.util.ArrayList;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class Enkidu {
    Connection dbCon;
    Statement dbStatement;
    FileInputStream csvFile;
    String tblName;
    int LineNumber = 1;
    ArrayList<String> columns = new ArrayList<>();
    ArrayList<String> columnTypes = new ArrayList<>();
    ArrayList<String> row = new ArrayList<>();
    ToolArguments.Delimiter delimiter;
    char columnQuote;

    public Enkidu(ToolArguments args, String tblName) throws RuntimeException
    {
        try {
            csvFile = new FileInputStream(args.CSVPath);
            dbCon = Enkidu.createConnection(args);
        }catch (FileNotFoundException ex){
            throw new RuntimeException("Unable to Open the CSV File: "+ex.getMessage());
        }catch (ClassNotFoundException ex){
            throw new RuntimeException("Unable to Load Database Drivers: "+ex.getMessage());
        }catch (SQLException ex){
            throw new RuntimeException("Unable to Open a Database Connection"+ex.getMessage());
        }
        try {
            dbStatement = dbCon.createStatement();
        }catch (SQLException ex){
            throw new RuntimeException("Unable to Open a Statement to execute SQL queries"+ex.getMessage());
        }

        delimiter = args.delimiter;
        switch(args.dbType){
            case MSSQL -> columnQuote = '"';
            case MYSQL -> columnQuote = '`';
            default -> {
                return;
            }
        }

        this.tblName = tblName;

        GetColumns();
        try {
        ReadRow();
        } catch (EOFException ex) {
            throw new RuntimeException("File doesn't have any data: "+ ex.getMessage());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }

        if(columns.size() != row.size()) throw  new RuntimeException(String.format("Line %d has Either Too Few or Too many Attributes", LineNumber));

        DetermineColumnTypes();
        try {
            CreateTable(tblName);
        }
        catch (SQLException ex){
            throw new RuntimeException("Failed to Create Table into the Database: "+ex.getMessage());
        }

        try {
            InsertRow();
            System.out.println("Successfully Inserted Row into the Database");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void CreateTable(String tblName) throws SQLException {
        StringBuilder queryBuilder = new StringBuilder(String.format("CREATE TABLE %s( %s%s%s %s%n", tblName, columnQuote, columns.getFirst(), columnQuote, columnTypes.getFirst()));
        for (int i = 1; i < columnTypes.size(); i++) {
            queryBuilder.append(", ")
                    .append(columnQuote)
                    .append(columns.get(i))
                    .append(columnQuote)
                    .append(" ")
                    .append(columnTypes.get(i));
        }
        queryBuilder.append(");");

        String query = queryBuilder.toString();
        dbStatement.executeUpdate(query);
    }

    private void GetColumns() throws RuntimeException {
        try {
            StringBuilder colname = new StringBuilder();
            int character;
            boolean openQuote = false;
            while ((character = csvFile.read()) != 10) {
                if (character == 34) {
                    openQuote = !openQuote;
                    continue;
                }
                if (character == delimiter.val && !openQuote) {
                    if (colname.isEmpty()) colname.append("Col").append(columns.size() - 1);

                    columns.add(colname.toString());
                    colname = new StringBuilder();
                    continue;
                }

                if (character == 39) colname.append("'");

                colname.append((char) character);
            }

            if (!colname.isEmpty()) {
                columns.add(colname.toString());
            }

            LineNumber++;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected Error while trying to parse column names: "+ ex.getMessage());
        }
    }

    private void DetermineColumnTypes(){
        for (String ele : row) {
            if (Parsable.toInt(ele))
                columnTypes.add("int");
            else if (Parsable.toFloat(ele))
                columnTypes.add("float");
            else
                columnTypes.add("text");
        }

    }


    public void ReadRow() throws EOFException, RuntimeException {

        LineNumber++;
        try {
            row = new ArrayList<>();

            int character;
            boolean openQuote = false;
            StringBuilder colVal = new StringBuilder();
            while ((character = csvFile.read()) != 10 ) {
                if (character == -1) {
                    throw new EOFException("End of File");
                }

                if (character == 34) {
                    openQuote = !openQuote;
                    continue;
                }
                if (character == delimiter.val && !openQuote) {
                    row.add(colVal.toString());
                    colVal = new StringBuilder();
                    continue;
                }

                if (character == 39) colVal.append("'");

                colVal.append((char) character);
            }
            if (!colVal.isEmpty()) row.add(colVal.toString());
        }catch (EOFException ex){
            throw ex;
        }
        catch (Exception ex) {
            throw new RuntimeException("Unexpected Error while trying to parse row: "+ex.getMessage());
        }
    }

    private void InsertRow() throws SQLException, RuntimeException {
        StringBuilder queryTable = new StringBuilder("INSERT INTO " + tblName + "(");
        StringBuilder queryValues = new StringBuilder("VALUES(");
        for (int i = 0; i < row.size(); i++) {
            if (row.get(i).isEmpty()) continue;

            queryTable.append(columnQuote).append(columns.get(i)).append(columnQuote).append(",");
            if (columnTypes.get(i).equals("text")) {
                queryValues.append("'").append(row.get(i)).append("',");
            } else {
                queryValues.append(row.get(i)).append(",");
            }
        }
        queryTable.deleteCharAt(queryTable.length() - 1);
        queryTable.append(") ");
        queryValues.deleteCharAt(queryValues.length() - 1);
        queryValues.append(");");

        try {
            dbStatement.executeUpdate(queryTable.toString() + queryValues);
        }catch (SQLException ex){
            throw new SQLException("Failed to Insert Lineno "+LineNumber+" Into the Table. Skipping...\n" + ex.getMessage());
        }
    }

    public void InsertData() throws EOFException, SQLException {
        ReadRow();
        InsertRow();
    }

    public static Connection createConnection(ToolArguments arguments) throws ClassNotFoundException, SQLException,RuntimeException {
        Connection con;
        switch (arguments.dbType){
            case MSSQL -> {
                String DbURL = String.format("jdbc:sqlserver://%s;database=%s;trustServerCertificate=true", arguments.dbIP, arguments.dbName);

                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                con = DriverManager.getConnection(DbURL, arguments.dbUsername, arguments.dbPassword);
            }
            case MYSQL ->  {
                String DbURL = String.format("jdbc:mysql://%s/%s?user=%s&password=%s", arguments.dbIP, arguments.dbName, arguments.dbUsername, arguments.dbPassword);
                Class.forName("com.mysql.cj.jdbc.Driver");
                con = DriverManager.getConnection(DbURL);
            }
            case POSTGRESQL -> {
                String DbURL = String.format("jdbc:postgresql://%s/%s?user=%s&password=%s", arguments.dbIP, arguments.dbName, arguments.dbUsername, arguments.dbPassword);

                Class.forName("org.postgresql.Driver");
                con = DriverManager.getConnection(DbURL);
            }
            default -> throw new RuntimeException("Unknown Database Type");
        }
        return con;
    }
}

class Parsable {
    public static boolean toFloat(String input) {
        try {
            Float.parseFloat(input);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static boolean toInt(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}