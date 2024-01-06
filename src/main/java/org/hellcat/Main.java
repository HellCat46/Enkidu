package org.hellcat;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.Scanner;

public class Main {
    static Connection dbCon;
    static FileInputStream csvFile;
    static boolean EOF = false;
    public static void main(String[] args)  {

        // Get Arguments Provided by user
        final ToolArguments Arguments = ParseArguments(args);


        Statement statement;
        try {
            statement = dbCon.createStatement();
        } catch (SQLException ex) {
            System.out.println("Database Connection was closed \n" + ex);
            return;
        }

        // Open a Read Stream the CSV File
        int LineNumber = 1;
        try{
            csvFile = new FileInputStream(Arguments.CSVPath);
        }catch (IOException ex){
            System.out.println("Errors while trying to Read the CSV File\n"+ex);
            return;
        }


        // Parses Out all the Column Name in the CSV File
        Optional<ArrayList<String>> readRes = ReadColumnNames(Arguments.delimiter);
        if(readRes.isEmpty()){
            System.out.println("Unable to Read File At Line"+ LineNumber);
            return;
        }
        ArrayList<String> columns = readRes.get();
        if(columns.isEmpty()){
            System.out.println("Error: Unable to parse any columns from CSV. Make sure you have selected the correct Delimiter");
            return;
        }
        LineNumber++;

        // Parses Out First Row Elements to Determine Column's Datatype and to obviously get first row
        readRes = ReadLine(Arguments.delimiter);
        if(readRes.isEmpty()){
            System.out.println("Unable to Read File Line");
            return;
        }
        ArrayList<String> RowElements = readRes.get();
        if(RowElements.size() != columns.size()){
            System.out.println("Line "+LineNumber+":Either Too Few or Too many Attributes.");
            return;
        }
        LineNumber++;

        // Determines Column's Datatype
        ArrayList<String> ColumnTypes = new ArrayList<>();
        for(String ele : RowElements){
            if(Parsable.toInt(ele))
                ColumnTypes.add("int");
            else if (Parsable.toFloat(ele))
                ColumnTypes.add("float");
            else
                ColumnTypes.add("text");
        }

        Scanner sc = new Scanner(System.in);
        String name;
        while (true) {
            System.out.print("Enter Name for New Table: ");
            name = sc.next();
            String CreateTableQuery = BuildDbCreateQuery(name, columns, ColumnTypes);

            try {
                statement.executeUpdate(CreateTableQuery);
                System.out.println("Successfully Created Table for the CSV");
                break;
            } catch (SQLException ex) {
                System.out.println(ex.toString());
            }
        }

        try {
            Optional<ArrayList<String >> res;
            while (!EOF){
                String InsertDataQuery = BuildDbInsertQuery(name, columns, RowElements, ColumnTypes);
                statement.executeUpdate(InsertDataQuery);

                res = ReadLine(Arguments.delimiter);
                if(res.isEmpty()) System.out.println("Unable to Read Line No "+(LineNumber+1)+". Skipping...");
                else{
                    RowElements = res.get();
                    System.out.println("Successfully Read Line No "+(++LineNumber));
                }
            }

        }catch (SQLException ex){
            System.out.println("Unable to Insert Data Into the Table"+ex);
        }
    }

    public static String BuildDbCreateQuery(String tableName, ArrayList<String> columns, ArrayList<String> ColumnTypes){

        StringBuilder query = new StringBuilder("CREATE TABLE "+tableName+"(\""+columns.getFirst()+"\" "+ColumnTypes.getFirst());
        for(int i =1; i< ColumnTypes.size(); i++){
            query.append(", \"")
                    .append(columns.get(i))
                    .append("\" ")
                    .append(ColumnTypes.get(i));
        }
        query.append(");");

        return query.toString();
    }

    public static String BuildDbInsertQuery(String tableName, ArrayList<String> Columns, ArrayList<String> Row, ArrayList<String> ColumnTypes) {
        StringBuilder queryTable = new StringBuilder("INSERT INTO "+tableName+"(");
        StringBuilder queryValues = new StringBuilder("VALUES(");
        for(int i =0; i< Row.size(); i++){
            if(Row.get(i).isEmpty()) continue;

            queryTable.append("\"").append(Columns.get(i)).append("\"").append(",");
            if(ColumnTypes.get(i).equals("text") ){
                queryValues.append("'").append(Row.get(i)).append("',");
            }else {
                queryValues.append(Row.get(i)).append(",");
            }
        }
        queryTable.deleteCharAt(queryTable.length()-1);
        queryTable.append(") ");
        queryValues.deleteCharAt(queryValues.length()-1);
        queryValues.append(");");
        return queryTable.toString() + queryValues;
    }
    private static Optional<ArrayList<String>> ReadColumnNames(ToolArguments.Delimiter delimiter){
        try {
            ArrayList<String> list = new ArrayList<>();
            StringBuilder colname = new StringBuilder();

            int character;
            boolean openQuote = false;
            while ((character = csvFile.read()) != 10){
                if(character == -1){
                    EOF = true;
                    return Optional.empty();
                }

                if(character == 34){
                    openQuote = !openQuote;
                    continue;
                }
                if(character == delimiter.val && !openQuote){
                    if(colname.isEmpty()) colname.append("Col").append(list.size()-1);

                    list.add(colname.toString());
                    colname = new StringBuilder();
                    continue;
                }
                colname.append((char) character);
            }

            if(!colname.isEmpty()){
                list.add(colname.toString());
            }
            return Optional.of(list);
        }catch (Exception ex){
            return Optional.empty();
        }
    }

    private static Optional<ArrayList<String>> ReadLine(ToolArguments.Delimiter delimiter){
        try {
            ArrayList<String> list = new ArrayList<>();
            StringBuilder colname = new StringBuilder();

            int character;
            boolean openQuote = false;
            while ((character = csvFile.read()) != 10){

                if(character == 34){
                    openQuote = !openQuote;
                    continue;
                }
                if(character == delimiter.val && !openQuote){
                    list.add(colname.toString());
                    colname = new StringBuilder();
                    continue;
                }
                colname.append((char) character);
            }
            if(!colname.isEmpty()){
                list.add(colname.toString());
            }
            return Optional.of(list);
        }catch (Exception ex){
            return Optional.empty();
        }
    }
    private static ToolArguments ParseArguments(String[] args) {
        ToolArguments arguments = new ToolArguments();
        for (String arg : args) {
            if (arg.contains("--csvpath=")) {
                arguments.CSVPath = Arrays.stream(arg.split("--csvpath=")).toList().getLast();
                File file = new File(arguments.CSVPath);
                if (!file.exists()) {
                    System.out.println("Error: Provided CSV Path is invalid.\n");
                    System.exit(1);
                }
            }
            else if (arg.contains("--delimiter=")) {
                if (arg.contains("C")) arguments.delimiter = ToolArguments.Delimiter.Comma;
                else if (arg.contains("S")) arguments.delimiter = ToolArguments.Delimiter.Semicolon;
                else if (arg.contains("P")) arguments.delimiter = ToolArguments.Delimiter.Pipe;
                else if (arg.contains("T")) arguments.delimiter = ToolArguments.Delimiter.Tab;
                else {
                    System.out.println("Error: Unknown Delimiter. Valid Delimiters are C,S,P,T.\n");
                    System.exit(1);
                }
            }
            else if (arg.contains("--dbip=")) arguments.dbIP = Arrays.stream(arg.split("--dbip=")).toList().getLast();
            else if (arg.contains("--dbname=")) arguments.dbName = Arrays.stream(arg.split("--dbname=")).toList().getLast();
            else if (arg.contains("--dbuser=")) arguments.dbUsername = Arrays.stream(arg.split("--dbuser=")).toList().getLast();
            else if (arg.contains("--dbpass=")) arguments.dbPassword = Arrays.stream(arg.split("--dbpass=")).toList().getLast();
            else if (arg.contains("--dbtype=")) {
                 if(arg.contains("mssql")) arguments.dbType = ToolArguments.DatabaseType.MSSQL;
                 else if(arg.contains("mysql")) arguments.dbType = ToolArguments.DatabaseType.MYSQL;
                 else if(arg.contains("postgres")) arguments.dbType = ToolArguments.DatabaseType.POSTGRESQL;
                 else {
                     System.out.println("Error: Unknown Database Type. Valid Types are mssql, mysql and postgres");
                     System.exit(1);
                 }
            }
            else if (arg.contains("--help")) {
                printDefaultOutput();
                System.exit(1);
            }
            else {
                System.out.println("Unknown Argument: " + arg + "\n");
                printDefaultOutput();
                System.exit(1);
            }
        }

        if(arguments.CSVPath == null || arguments.delimiter == null || arguments.dbIP == null || arguments.dbName == null || arguments.dbUsername == null || arguments.dbPassword == null || arguments.dbType == null) {
            System.out.println("Error: Too few Arguments.\n");
            printDefaultOutput();
            System.exit(1);
        }


        try{
            InitializeDatabase(arguments);
            if(dbCon.isValid(30))
                System.out.println("Success");
            else{
                System.out.println("Failed");
            }
        }catch (SQLException ex){
            System.out.println("Unable to connect to the Database.\n"+ex);
            System.exit(1);
        }


        return arguments;

    }

    private static  void InitializeDatabase(ToolArguments arguments){
        try {
            if (arguments.dbType == ToolArguments.DatabaseType.MSSQL) {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                dbCon = DriverManager.getConnection(arguments.GetDatabaseURL(), arguments.dbUsername, arguments.dbPassword);
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Unable to Initialize Database Connection");
            System.exit(1);
        }
    }
    private static void printDefaultOutput(){
        System.out.println("""
                    \033[1mUsage: enkidu [options...]

                    Option           Usage                         Description
                    csvpath          --csvpath=[filepath]          Relative Path to the CSV File
                    delimiter        --delimiter=[C,S,P,T]         Delimiter's First Character
                    dbip             --dbip=[ipAddr]               Database's IP Address And Port
                    dbname           --dbname=[name]               Database's Name
                    dbuser           --dbuser=[username]           Database User's Name
                    dbpass           --dbpass=[password]           Database User's Password
                    dbtype           --dbpas=[type]                Database Type
                    """);
    }
}

class ToolArguments {
    public Delimiter delimiter = null;
    public String CSVPath = null;
    public String dbIP =null;
    public String dbName =null;
    public String dbUsername = null;
    public String dbPassword = null;
    public DatabaseType dbType = null;
    enum Delimiter {
        Comma((byte) 44),
        Semicolon((byte) 59),
        Pipe((byte) 124),
        Tab((byte) 9);

        public final byte val;
        Delimiter(byte val){
            this.val = val;
        }
    }
    enum DatabaseType {
        MYSQL("mysql"),
        MSSQL("sqlserver"),
        POSTGRESQL("postgresql");

        public final String value;
        DatabaseType(String value){
            this.value = value;
        }
    }

    public String GetDatabaseURL(){
        if(dbType == DatabaseType.MSSQL){
            return "jdbc:"+dbType.value+"://"+dbIP+";database="+dbName+";trustServerCertificate=true";
        }else{
            return "jdbc:"+dbType.value+"://"+dbIP+"/"+dbName;
        }
    }
}

class Parsable {
    public static boolean toFloat(String input){
        try {
            Float.parseFloat(input);
            return true;
        }catch (Exception ignored){
            return false;
        }
    }
    public static boolean toInt(String input){
        try {
            Integer.parseInt(input);
            return true;
        }catch (Exception ignored){
            return false;
        }
    }
}