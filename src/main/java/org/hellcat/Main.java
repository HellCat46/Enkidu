package org.hellcat;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class Main {

    static FileInputStream csvFile;
    public static void main(String[] args)  {

        // Get Arguments Provided by user
        Optional<ToolArguments> res = ParseArguments(args);
        if(res.isEmpty()){
            printDefaultOutput();
            return;
        }
        final ToolArguments Arguments = res.get();


        // Open a Read Stream the CSV File
        int LineNumber = 1;
        try{
            csvFile = new FileInputStream(Arguments.CSVPath);
        }catch (IOException ex){
            System.out.println("Errors while trying to Read the CSV File\n"+ex);
            return;
        }


        // Parses Out all the Column Name in the CSV File
        Optional<ArrayList<String>> readRes = ReadLine(Arguments.delimiter);
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

        StringBuilder query = new StringBuilder("CREATE TABLE CSV(\""+columns.getFirst()+"\" "+ColumnTypes.getFirst());
        for(int i =1; i< ColumnTypes.size(); i++){
            query.append(", \"")
                    .append(columns.get(i))
                    .append("\" ")
                    .append(ColumnTypes.get(i));
        }
        query.append(");");
        System.out.print(query);
//        try(Connection con = DriverManager.getConnection(arguments.connectionString)){
//
//        }catch (SQLException ex){
//            System.out.println("Errors while trying to communicate with DB"+ ex);
//        }
    }

    private static Optional<ArrayList<String>> ReadLine(ToolArguments.Delimiter delimiter){
        try {
            ArrayList<String> list = new ArrayList<>();
            StringBuilder colname = new StringBuilder();

            int character;
            boolean openQuote = false;
            while ((character = csvFile.read()) != 10){
                if(character == 32) character=95;

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

    private static Optional<ToolArguments> ParseArguments(String[] args) {
        ToolArguments arguments = new ToolArguments();
        for (String arg : args) {
            if (arg.contains("--csvpath=")) {
                arguments.CSVPath = Arrays.stream(arg.split("--csvpath=")).toList().getLast();
                File file = new File(arguments.CSVPath);
                if (!file.exists()) {
                    System.out.println("Error: Provided CSV Path is invalid.\n");
                    return Optional.empty();
                }
            }
            else if (arg.contains("--delimiter=")) {
                if (arg.contains("C")) arguments.delimiter = ToolArguments.Delimiter.Comma;
                else if (arg.contains("S")) arguments.delimiter = ToolArguments.Delimiter.Semicolon;
                else if (arg.contains("P")) arguments.delimiter = ToolArguments.Delimiter.Pipe;
                else if (arg.contains("T")) arguments.delimiter = ToolArguments.Delimiter.Tab;
                else {
                    System.out.println("Error: Unknown Delimiter. Valid Delimiters are C,S,P,T.\n");
                    return Optional.empty();
                }
            }
            else if (arg.contains("--connectionString=")) {
                arguments.connectionString = Arrays.stream(arg.split("--connectionString=")).toList().getLast();
                // Connection con = DriverManager.getConnection();
            }
            else if (arg.contains("--help")) {
                return Optional.empty();
            }
            else {
                System.out.println("Unknown Argument: " + arg + "\n");
                return Optional.empty();

            }
        }

        if(arguments.CSVPath == null || arguments.delimiter == null || arguments.connectionString == null) {
            System.out.println("Error: Too few Arguments.\n");
            return Optional.empty();
        }
        return Optional.of(arguments);

    }

    private static void printDefaultOutput(){
        System.out.println("""
                    \033[1mUsage: enkidu [options...]

                    Option           Usage                         Description
                    csvpath          --csvpath=[filepath]          Relative Path to the CSV File
                    delimiter        --delimiter=[C,S,P,T]         Delimiter's First Character
                    connectionString --connectionString=[string]   Database Connection String
                    """);
    }
}

class ToolArguments {
    public Delimiter delimiter = null;
    public String CSVPath = null;
    public String connectionString =null;

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