package org.hellcat;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

public class ToolArguments {
    public Delimiter delimiter = null;
    public String CSVPath = null;
    public String dbIP = null;
    public String dbName = null;
    public String dbUsername = null;
    public String dbPassword = null;
    public DatabaseType dbType = null;

    public ToolArguments(String[] args){
        for (String arg : args) {
            if (arg.contains("--csvpath=")) {
                CSVPath = Arrays.stream(arg.split("--csvpath=")).toList().getLast();
                File file = new File(CSVPath);
                if (!file.exists()) {
                    System.out.println("Error: Provided CSV Path is invalid.\n");
                    System.exit(1);
                }
            }
            else if (arg.contains("--delimiter=")) {
                switch (Arrays.stream(arg.split("--delimiter=")).toList().getLast()){
                    case "C" : delimiter = ToolArguments.Delimiter.Comma;
                    break;
                    case "S" : delimiter = ToolArguments.Delimiter.Semicolon;
                        break;
                    case "P" : delimiter = ToolArguments.Delimiter.Pipe;
                        break;
                    case "T" : delimiter = ToolArguments.Delimiter.Tab;
                        break;
                    default:{
                        System.out.println("Error: Unknown Delimiter. Valid Delimiters are C,S,P,T.\n");
                        System.exit(1);
                    }
                }
            }
            else if (arg.contains("--dbip="))
                dbIP = Arrays.stream(arg.split("--dbip=")).toList().getLast();
            else if (arg.contains("--dbname="))
                dbName = Arrays.stream(arg.split("--dbname=")).toList().getLast();
            else if (arg.contains("--dbuser="))
                dbUsername = Arrays.stream(arg.split("--dbuser=")).toList().getLast();
            else if (arg.contains("--dbpass="))
                dbPassword = Arrays.stream(arg.split("--dbpass=")).toList().getLast();
            else if (arg.contains("--dbtype=")) {
                switch (Arrays.stream(arg.split("--dbtype=")).toList().getLast()){
                    case "mssql" : dbType = DatabaseType.MSSQL;
                    break;
                    case "mysql" : dbType = DatabaseType.MYSQL;
                    break;
                    case "postgres" : dbType = DatabaseType.POSTGRESQL;
                    break;
                    default: {
                        System.out.println("Error: Unknown Database Type. Valid Types are mssql, mysql and postgres");
                        System.exit(1);
                    }
                }
            } else if (arg.contains("--help")) {
                Main.printDefaultOutput();
                System.exit(1);
            } else {
                System.out.println("Unknown Argument: " + arg + "\n");
                Main.printDefaultOutput();
                System.exit(1);
            }
        }

        if (CSVPath == null || delimiter == null || dbIP == null || dbName == null || dbUsername == null || dbPassword == null || dbType == null) {
            System.out.println("Error: Too few Arguments.\n");
            Main.printDefaultOutput();
            System.exit(1);
        }


        try(Connection dbCon = Enkidu.createConnection(this)) {
            if (dbCon.isValid(30))
                System.out.println("Success");
            else {
                System.out.println("Failed");
            }
        } catch (SQLException ex) {
            System.out.println("Unable to connect to the Database.\n" + ex.getMessage());
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            System.out.println("Unable to load Database Drivers.\n" + ex.getMessage());
            System.exit(1);
        }catch (RuntimeException ex){
            System.out.println(ex.getMessage());
            System.exit(1);
        }


    }

    public enum Delimiter {
        Comma((byte) 44),
        Semicolon((byte) 59),
        Pipe((byte) 124),
        Tab((byte) 9);

        public final byte val;

        Delimiter(byte val) {
            this.val = val;
        }
    }

    public enum DatabaseType {
        MYSQL,
        MSSQL,
        POSTGRESQL
    }

}
