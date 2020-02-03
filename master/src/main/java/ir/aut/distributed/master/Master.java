package ir.aut.distributed.master;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.jetty.util.ArrayQueue;
import spark.Request;
import spark.utils.IOUtils;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.staticFiles;
import static spark.debug.DebugScreen.enableDebugScreen;

public class Master {

    private String codeLang;
    private String defaultPath = "/tmp/master/";
    private String dataFilePath = defaultPath.concat("/code.data");
    private String mapperCodePath;
    private String reducerCodePath;
    private String currentPhase = "Waiting for upload files";
    private Queue<File> files;
    private int blockSize = 10;
    private String finalOutputPath;
    private String master;
    private List<Executor> workers;
    private BlockingQueue<Executor> idleExecutors;
    private Queue<File> mappedFiles;
    private File mergedMapped;
    private Thread progressRunner;
    private File defaultDir;

    public Master(Config configs) {
        defaultDir = new File(defaultPath);
        defaultDir.mkdirs();
        staticFiles.externalLocation("/tmp/master/");
        master = configs.getString("master");
        idleExecutors = new LinkedBlockingQueue<>();
        workers = configs.getAnyRefList("worker").stream()
                .map(worker -> new Executor(worker.toString(), idleExecutors, mappedFiles)).collect(Collectors.toList());
        idleExecutors.addAll(workers);
        enableDebugScreen();
        files = new ArrayQueue<>();
        mappedFiles = new ArrayQueue<>();
    }

    public void run(){

        get("/progress", (req, res) ->
                currentPhase + "</br><p>reload this page to get latest progress state!</p>"
        );

        get("/", (req, res) ->
                "<form method='post' enctype='multipart/form-data'>"
                        + "    <p>Select data file\t<input type='file' name='uploaded_data_file' accept='.*'></p>"
                        + "    <p>Select mapper code\t<input type='file' name='uploaded_mapper_code' accept='.cpp,.py'></p>"
                        + "    <p>Select reducer code\t<input type='file' name='uploaded_reducer_code' accept='.cpp,.py'></p>"
                        + "    <input type='radio' name='code_lang' value='cpp' checked> C++<br>"
                        + "    <input type='radio' name='code_lang' value='py'> Python<br>"
                        + "    <p>enter maximum file size per chunk in MB</br><input type='text' name='chunk_size'></p></br>"
                        + "    <p><button>Upload selected files and run progress</button></p>"
                        + "</form>"
        );

        post("/", (req, res) -> {

            req.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/temp"));
            try (InputStream input = req.raw().getPart("code_lang").getInputStream()) {
                codeLang = IOUtils.toString(input);
            }
            try (InputStream input = req.raw().getPart("chunk_size").getInputStream()) {
                blockSize = Integer.parseInt(IOUtils.toString(input));
            }
            mapperCodePath = defaultDir.getPath().concat("/mapper.").concat(codeLang);
            reducerCodePath = defaultDir.getPath().concat("/reducer.").concat(codeLang);

            Files.deleteIfExists(new File(dataFilePath).toPath());
            Files.deleteIfExists(new File(mapperCodePath).toPath());
            Files.deleteIfExists(new File(reducerCodePath).toPath());
            Path tempDataFile = Files.createFile(new File(dataFilePath).toPath());
            Path tempMapperFile = Files.createFile(new File(mapperCodePath).toPath());
            Path tempReducerFile = Files.createFile(new File(reducerCodePath).toPath());

            if(req.raw().getPart("uploaded_data_file").getSubmittedFileName().isEmpty()
                    || req.raw().getPart("uploaded_mapper_code").getSubmittedFileName().isEmpty()
                    || req.raw().getPart("uploaded_reducer_code").getSubmittedFileName().isEmpty()) {
                return "<h1>one or some files not selected to upload!</br>try again<h1>";
            }

            try (InputStream input = req.raw().getPart("uploaded_data_file").getInputStream()) {
                Files.copy(input, tempDataFile, StandardCopyOption.REPLACE_EXISTING);
            }
            try (InputStream input = req.raw().getPart("uploaded_mapper_code").getInputStream()) {
                Files.copy(input, tempMapperFile, StandardCopyOption.REPLACE_EXISTING);
            }
            try (InputStream input = req.raw().getPart("uploaded_reducer_code").getInputStream()) {
                Files.copy(input, tempReducerFile, StandardCopyOption.REPLACE_EXISTING);
            }

            logInfo(req, tempDataFile, "uploaded_data_file");
            logInfo(req, tempMapperFile, "uploaded_mapper_code");
            logInfo(req, tempReducerFile, "uploaded_reducer_code");
            progressRunner = new Thread(this::startProgress, "Progress Runner");
            progressRunner.start();
            res.redirect("/progress");
            return "";
        });

        post("/result", (req, res) -> {

            req.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/temp"));
            Path tempMappedFile = Files.createTempFile(new File(defaultPath).toPath(),"mapped",".tmp");

            if(req.raw().getPart("mapped_file").getSubmittedFileName().isEmpty()) {
                return "one or some files not selected to upload!";
            }

            try (InputStream input = req.raw().getPart("my_ip").getInputStream()) {
                 String my_ip = IOUtils.toString(input);
                 for (Executor executor : workers) {
                    if(executor.getIp().equals(my_ip)) {
                        idleExecutors.add(executor);
                    }
                 }
            }

            try (InputStream input = req.raw().getPart("mapped_file").getInputStream()) {
                Files.copy(input, tempMappedFile, StandardCopyOption.REPLACE_EXISTING);
            }
            logInfo(req, tempMappedFile, "mapped_file");
            mappedFiles.add(tempMappedFile.toFile());
            return HttpServletResponse.SC_ACCEPTED;
        });
    }

    private void startProgress() {
        try {
            shredDateFile();
            sendFilesForWorkers();
            runReducePhase();
        } catch (IOException | InterruptedException e) {
            currentPhase = e.getMessage();
            e.printStackTrace();
        }
    }

    private void runReducePhase() throws IOException, InterruptedException {
        File mapperCode = new File(mapperCodePath);
        mapperCode.delete();
        finalOutputPath = defaultDir.getPath().concat("/final_result.out");
        mergedMapped = File.createTempFile("merged", "data", new File(defaultPath));
        while (!mappedFiles.isEmpty()) {
            File tmpFile = mappedFiles.poll();
            Files.write(mergedMapped.toPath(), Files.readAllBytes(tmpFile.toPath()), StandardOpenOption.APPEND);
            tmpFile.delete();
        }
        currentPhase = "Shred data file done.</br>Sending shred files for workers and running mapper phase on it done" +
                "</br>mapping is done for all files.</br>" +
                "reduce phase is running...";
        if (codeLang.equals("py")) {
            int exitVal = Runtime.getRuntime()
                    .exec("python " + reducerCodePath + " " + mergedMapped.getAbsolutePath() + finalOutputPath).waitFor();
            if (exitVal != 0) {
                currentPhase = "something wrong!";
            }
        }
        else if (codeLang.equals("cpp")) {

            int exitVal = Runtime.getRuntime()
                    .exec("g++ --std=c++11 " + reducerCodePath + " -o " + defaultPath + "binary.out").waitFor();
            if (exitVal != 0) {
                currentPhase = "Can not compile reducer code!";
                return;
            } else {
                currentPhase = "All done!</br>you can run another progress if you want!</br><p>" +
                        "<a href=\"/final_result.out\" download>download result</a></p>";
            }
            exitVal = Runtime.getRuntime()
                    .exec(defaultPath + "binary.out " + mergedMapped.getAbsolutePath() + " " + finalOutputPath).waitFor();
            if (exitVal != 0) {
                currentPhase = "something wrong!";
            } else {
                currentPhase = "All done!</br>you can run another progress if you want!</br><p>" +
                        "<a href=\"/final_result.out\" download>download result</a></p>";
            }
        }
        mergedMapped.delete();
    }

    private void sendFilesForWorkers() throws InterruptedException, IOException {
        currentPhase = "Shred data file done.</br>Sending shred files for workers ...";
        int total_files = files.size();
        while (!files.isEmpty()) {
            currentPhase = "Shred data file done.</br>Sending shred files for workers and running mapper phase on it..." +
                    "</br>mapping is done for " + mappedFiles.size() + " files from " + total_files + " files.";
            Executor executor = idleExecutors.take();
            executor.exec(files.poll(), mapperCodePath, codeLang);
        }
        while (mappedFiles.size() != total_files){
            TimeUnit.SECONDS.sleep(1);
        }
    }

    private void shredDateFile() throws IOException {
        currentPhase = "Shred data file...";
        File dataFile = new File(dataFilePath);
        BufferedReader br=new BufferedReader(new FileReader(dataFile));
        BufferedWriter bw= null;
        String line;
        File tmpFile= null;
        while((line=br.readLine())!=null) {
            if (tmpFile==null || tmpFile.length() >= blockSize*1024*1024) {
                tmpFile = File.createTempFile("data",".tmp", new File(defaultPath));
                files.add(tmpFile);
                bw = new BufferedWriter(new FileWriter(tmpFile));
            }
            bw.write(line+"\n");
            bw.flush();
        }
        if (bw != null) {
            bw.close();
        }
        br.close();
        dataFile.delete();
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

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("One required argument (config file) should be provided.");
        }
        Config configs = ConfigFactory.parseFile(new java.io.File(args[0]));
        Master master = new Master(configs);
        master.run();
    }
}
