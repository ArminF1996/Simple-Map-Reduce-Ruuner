package ir.aut.distributed.worker;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import spark.Request;
import spark.utils.IOUtils;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;

public class Worker {

    private String master;
    private String masterUrl;
    private String mine;
    private String defaultPath = "/tmp/worker/";
    private File defaultDir;
    private String codeLang;
    private String codePath;
    private String dataPath;
    private String outputPath;
    private Thread mapRunner;
    private String currentPhase = "OK, I'm here";

    public Worker(Config configs) {

        defaultDir = new File(defaultPath);
        defaultDir.mkdirs();
        master = configs.getString("master");
        masterUrl = "http://" + master + ":4567/result";
        mine = configs.getString("mine");
        port(4568);
    }

    public void run() {
        get("/", (req, res) ->
                currentPhase
        );
        post("/", (req, res) -> {

            req.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/temp"));
            try (InputStream input = req.raw().getPart("code_lang").getInputStream()) {
                codeLang = IOUtils.toString(input);
            }
            dataPath = defaultDir.getPath().concat("/mapper.in");
            outputPath = defaultDir.getPath().concat("/mapper.out");
            codePath = defaultDir.getPath().concat("/mapper.").concat(codeLang);

            Files.deleteIfExists(new File(dataPath).toPath());
            Files.deleteIfExists(new File(codePath).toPath());
            Path tempDataFile = Files.createFile(new File(dataPath).toPath());
            Path tempMapperFile = Files.createFile(new File(codePath).toPath());

            if(req.raw().getPart("mapper_input").getSubmittedFileName().isEmpty()
                    || req.raw().getPart("mapper_code").getSubmittedFileName().isEmpty()) {
                return "one or some files not selected to upload!";
            }

            try (InputStream input = req.raw().getPart("mapper_input").getInputStream()) {
                Files.copy(input, tempDataFile, StandardCopyOption.REPLACE_EXISTING);
            }
            try (InputStream input = req.raw().getPart("mapper_code").getInputStream()) {
                Files.copy(input, tempMapperFile, StandardCopyOption.REPLACE_EXISTING);
            }

            logInfo(req, tempDataFile, "mapper_input");
            logInfo(req, tempMapperFile, "mapper_code");
            mapRunner = new Thread(this::exec, "mapRunner");
            mapRunner.start();
            return HttpServletResponse.SC_ACCEPTED;
        });
    }

    private void exec() {
        try {
            startMapper();
            sendResult();
        } catch (IOException | InterruptedException e) {
            currentPhase = e.getMessage();
            e.printStackTrace();
        }
    }

    private void sendResult() throws IOException {
        URL url = new URL(masterUrl);
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

        File tmpFile = new File(outputPath);
        try(OutputStream out = http.getOutputStream()) {

            out.write(boundaryBytes);
            sendField(out, "my_ip", mine);
            out.write(boundaryBytes);
            try(InputStream file = new FileInputStream(tmpFile)) {
                sendFile(out, "mapped_file", file, "mapper.out");
            }
            // Finish the request
            out.write(finishBoundaryBytes);
        }
        tmpFile.delete();
    }

    private void startMapper() throws IOException, InterruptedException {

        if (codeLang.equals("py")) {
            int exitVal = Runtime.getRuntime()
                    .exec("python " + codePath + " " + dataPath  + " " + outputPath).waitFor();
            if (exitVal == 0) {
                currentPhase = "OK, I'm here";
            } else {
                currentPhase = "something wrong!";
            }
        }
        else if (codeLang.equals("cpp")) {
            int exitVal = Runtime.getRuntime()
                    .exec("g++ --std=c++11 " + codePath + " -o " + defaultPath + "binary.out").waitFor();
            if (exitVal != 0) {
                currentPhase = "Can not compile reducer code!";
                return;
            }
            exitVal = Runtime.getRuntime()
                    .exec(defaultPath + "binary.out " + dataPath + " " + outputPath).waitFor();
            if (exitVal == 0) {
                currentPhase = "OK, I'm here";
            } else {
                currentPhase = "something wrong!";
            }
        }
    }

    // methods used for logging
    private static void logInfo(Request req, Path tempFile, String name) throws IOException, ServletException {
        System.out.println("Uploaded file '" + getFileName(req.raw().getPart(name)) + "' saved as '" + tempFile.toAbsolutePath() + "'");
    }

    private static String getFileName(Part part) {
        for (String cd : part.getHeader("content-disposition").split(";")) {
            if (cd.trim().startsWith("filename")) {
                return cd.substring(cd.indexOf('=') + 1).trim().replace("\"", "");
            }
        }
        return null;
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

    public static void main(String[] args) throws UnknownHostException {
        if (args.length != 1) {
            throw new IllegalArgumentException("One required argument (config file) should be provided.");
        }
        Config configs = ConfigFactory.parseFile(new java.io.File(args[0]));
        Worker worker = new Worker(configs);
        worker.run();
    }
}
