package org.hellcat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.function.Consumer;

public class Logging {
    private final FileOutputStream file;

    public Logging() throws IOException {
        Path logFolder = Path.of("Log");
        if(!Files.isDirectory(logFolder)) Files.createDirectory(logFolder);

        String fileName = "Log/Log_"+ LocalDateTime.now()+ ".txt";

        file = new FileOutputStream(fileName);
    }

    public void Write(String msg, Type msgType, String moduleName) {
        String log = msg;
        switch (msgType){
            case Error -> log = "\n\n"+LocalDateTime.now()+": [Error in "+moduleName+"]    "+log + "\n";
            case Warning -> log = LocalDateTime.now()+": [Warning in "+moduleName+"]    "+log  + "\n";
            case Normal -> log = LocalDateTime.now()+": "+log + "\n";
        }
        try {
            file.write(log.getBytes());
        }catch (IOException ex){
            System.out.println("Failed to Write Logs"+ex.getMessage());
        }
    }
    public void BackTrace(StackTraceElement[] elements){

        try {
            for(StackTraceElement ele : elements){
                file.write(ele.toString().getBytes());
                file.write('\n');
            }
            file.write('\n');
            file.write('\n');
        }catch (Exception ex){
            System.out.println("Failed to Write Crash bump"+ex.getMessage());
        }
    }
    public void Close() throws IOException {
        file.close();
    }

    public enum Type {
        Warning,
        Error,
        Normal,
    }
}
