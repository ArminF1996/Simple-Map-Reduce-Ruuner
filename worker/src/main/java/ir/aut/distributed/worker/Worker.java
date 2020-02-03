package ir.aut.distributed.worker;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.UnknownHostException;

import static spark.Spark.port;
import static spark.route.HttpMethod.post;

public class Worker {

    String master;
    String mine;

    public Worker(Config configs) {
        master = configs.getString("master");
        mine = configs.getString("mine");
        port(4568);
    }

    public void run() {

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
