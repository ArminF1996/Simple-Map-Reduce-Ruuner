package ir.aut.distributed.master;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class Executor {

    private String ip;
    private String myUrl;
    private BlockingQueue idles;
    private Queue<File> mappedFiles;

    public Executor(String ip, BlockingQueue idles, Queue<File> mappedFiles) {
        this.ip = ip;
        this.myUrl = "http://" + ip + ":4568/";
        this.idles = idles;
        this.mappedFiles = mappedFiles;
    }

    public void exec(File dataFile, String codeFilePath, String lang) throws IOException {

        URL url = new URL(myUrl);
        URLConnection con = url.openConnection();
        HttpURLConnection http = (HttpURLConnection)con;
        http.setRequestMethod("POST");
        http.setDoOutput(true);

        String boundary = UUID.randomUUID().toString();
        byte[] boundaryBytes = ("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] finishBoundaryBytes = ("--" + boundary + "--").getBytes(StandardCharsets.UTF_8);
        http.setRequestProperty("Content-Type",
                "multipart/form-data; charset=UTF-8; boundary=" + boundary);
        http.setChunkedStreamingMode(0);

        try(OutputStream out = http.getOutputStream()) {

            out.write(boundaryBytes);
            sendField(out, "code_lang", lang);
            out.write(boundaryBytes);
            try(InputStream file = new FileInputStream(dataFile)) {
                sendFile(out, "mapper_input", file, "mapper.data");
            }
            out.write(boundaryBytes);
            try(InputStream file = new FileInputStream(codeFilePath)) {
                sendFile(out, "mapper_code", file, "mapper." + lang);
            }
            // Finish the request
            out.write(finishBoundaryBytes);
        }
        dataFile.delete();
    }

    private void sendFile(OutputStream out, String name, InputStream in, String fileName) throws IOException {
        String o = "Content-Disposition: form-data; name=\"" + URLEncoder.encode(name,"UTF-8")
                + "\"; filename=\"" + URLEncoder.encode(fileName,"UTF-8") + "\"\r\n\r\n";
        out.write(o.getBytes(StandardCharsets.UTF_8));
        byte[] buffer = new byte[2048];
        for (int n = 0; n >= 0; n = in.read(buffer))
            out.write(buffer, 0, n);
        out.write("\r\n".getBytes(StandardCharsets.UTF_8));
    }

    private void sendField(OutputStream out, String name, String field) throws IOException {
        String o = "Content-Disposition: form-data; name=\""
                + URLEncoder.encode(name,"UTF-8") + "\"\r\n\r\n";
        out.write(o.getBytes(StandardCharsets.UTF_8));
        out.write(URLEncoder.encode(field,"UTF-8").getBytes(StandardCharsets.UTF_8));
        out.write("\r\n".getBytes(StandardCharsets.UTF_8));
    }

    public String getIp() {
        return this.ip;
    }
}
