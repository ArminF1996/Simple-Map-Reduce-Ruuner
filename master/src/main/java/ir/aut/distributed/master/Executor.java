package ir.aut.distributed.master;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public class Executor {

    private String ip;
    private BlockingQueue idles;
    private Queue<File> mappedFiles;

    public Executor(String ip, BlockingQueue idles, Queue<File> mappedFiles) {
        this.ip = ip;
        this.idles = idles;
        this.mappedFiles = mappedFiles;
    }

    public void exe(File dataFile, String codeFilePath, String lang){
        //TODO send code and data file and then call lang entry point to run process on node,
        // then wait for response and delete file when response received
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
