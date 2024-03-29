package org.hellcat;

import java.io.IOException;
import java.sql.*;
import java.util.Scanner;
import java.io.EOFException;

public class Main {
    public static void main(String[] args) throws IOException {
        Logging logger = new Logging();

        Scanner sc = new Scanner(System.in);
        ToolArguments Arguments = new ToolArguments(args, Main::printDefaultOutput);

        try {
            System.out.print("Enter name of table (Spaces Not Allowed): ");
            String tblName = sc.next();
            Enkidu app = new Enkidu(Arguments, tblName, logger);

            System.out.println("Started Inserting Data");
            while (true) {
                try {
                    app.InsertData();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                } catch (EOFException ex) {
                    break;
                } catch (Exception ex) {
                    throw new RuntimeException(ex.getMessage());
                }
            }
        } catch (RuntimeException ex) {
            System.out.println(ex.getMessage());
            return;
        }
        System.out.println("Successfully Inserted CSV File Data to Database");

        logger.Close();
    }

    private static void printDefaultOutput() {
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

