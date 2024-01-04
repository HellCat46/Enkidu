package org.hellcat;
import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Optional;

public class Main {
    public static void main(String[] args)  {
        Optional<ToolArguments> res = ParseArguments(args);
        if(res.isEmpty()){
            System.out.println("""
                    \033[1mUsage: enkidu [options...]

                    Option           Usage                         Description
                    csvpath          --csvpath=[filepath]          Relative Path to the CSV File
                    delimiter        --delimiter=[C,S,P,T]         Delimiter's First Character
                    connectionString --connectionString=[string]   Database Connection String
                    """);
            return;
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
}

class ToolArguments {
    public Delimiter delimiter = null;
    public String CSVPath = null;
    public String connectionString =null;

    enum Delimiter {
        Comma,
        Semicolon,
        Pipe,
        Tab,
    }
}