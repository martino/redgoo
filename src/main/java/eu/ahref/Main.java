package eu.ahref;

import com.jimplush.goose.Configuration;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import java.util.concurrent.*;

/**
 * Redgoo main class
 *
 * @author  Martino Pizzol
 */

public class Main implements Observer{
    private static final Logger logger = Logger.getLogger(Main.class);

    Jedis jedis;
    String redisURL;
    String gooseTmpDir = null;
    String jobsQueue;
    //TODO parametrizzarli
    String redgooNum = "redgoo:nsites";
    String redgooSiteList = "redgoo:kosites";
    SignalHandler sh = null;
    Boolean gracefulExit = false;
    Configuration gooseConfig;
    ExecutorService pool = null;
    int timeout = 30; // timeout for blpop

    public String getGooseTmpDir(){
        return gooseTmpDir;
    }

    public Main(String rurl, String jq,  String cpath, String ipath){
        redisURL = (rurl==null) ? "localhost" : rurl;
        jobsQueue = (jq == null) ? "readability:jobs" : jq;
        gooseConfig = new Configuration();
        gooseConfig.setImagemagickConvertPath((cpath==null) ? "/usr/bin/convert" : cpath);
        gooseConfig.setImagemagickIdentifyPath((ipath==null) ? "/usr/bin/identify" : ipath);
        this.createThreadPool();
    }

    public Main(){
        this.redisURL = "localhost";
        this.jobsQueue = "readability:jobs";
        gooseConfig = new Configuration();
        gooseConfig.setImagemagickConvertPath("/usr/bin/convert");
        gooseConfig.setImagemagickIdentifyPath("/usr/bin/identify");
        this.createThreadPool();
    }

    public void createThreadPool(){
        //TODO parameter for maximum thread
        this.pool = Executors.newFixedThreadPool(5);
    }

    public void stopThreadPool(){
        this.pool.shutdown();
    }


    /**
     * Function that handle signal
     * @param observable
     * @param o
     */
    public void update(Observable observable, Object o) {
        logger.info("Graceful exit");
        gracefulExit = true;
    }

    public void setCommandHandle(){
        sh = new SignalHandler();
        sh.addObserver(this);
        sh.handleSignal("TERM");
    }
    public boolean connect(){
        try{
            jedis = new Jedis(redisURL);
            logger.info("Redis on "+redisURL+": OK");
            return true;
        }catch(Exception e){
            logger.error("Redis connection\n "+ e.getStackTrace().toString());
            return false;
        }
    }

    public void disconnect(){
        try {
            jedis.disconnect();
        } catch (IOException e) {
            logger.error("Redis disconnection\n"+e.getStackTrace().toString());
        }
    }

    public void go(){
        List<String> resultRedis;
        while(!gracefulExit){
            try{
                resultRedis =  jedis.blpop(timeout,this.jobsQueue);
                if (resultRedis==null)
                    continue;
                String jswork = resultRedis.get(1);
                jedis.incr(this.redgooNum);
                this.pool.execute(new GWorker(jswork, this.redisURL,logger, gooseConfig,redgooSiteList));
            }catch(Exception e){
                logger.debug("Redis connection reset\n "+e.getStackTrace().toString());
                this.connect();
            }
        }

    }
    public static void main(String[] args){

        Option optionIP = new Option("rhost", "Redis Host");
        optionIP.setArgs(1);
        optionIP.setArgName("host");

        Option optionQueueJobs = new Option("qj", "Jobs Queue");
        optionQueueJobs.setArgs(1);
        optionQueueJobs.setArgName("queue name");

        Option optionGoosePath = new Option("gdir", "Goose working path");
        optionGoosePath.setArgs(1);
        optionGoosePath.setArgName("path");

        Option optionGooseConvert = new Option("convert", "Convert path");
        optionGooseConvert.setArgs(1);
        optionGooseConvert.setArgName("path");

        Option optionGooseIdentify = new Option("identify", "Identify path");
        optionGooseIdentify.setArgs(1);
        optionGooseIdentify.setArgName("path");

        Options options = new Options();
        options.addOption(optionIP);
        options.addOption(optionQueueJobs);
        options.addOption(optionGoosePath);
        options.addOption(optionGooseConvert);
        options.addOption(optionGooseIdentify);
        options.addOption("help", false, "Show program usage");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;

        String rip=null, qj=null, gpath=null, cpath=null, ipath=null;
        try {
            cmd = parser.parse(options,args);
            if (cmd.hasOption("rhost"))
                rip = cmd.getOptionValue("rhost");
            if(cmd.hasOption("qj"))
                qj = cmd.getOptionValue("qj");
            if(cmd.hasOption("gdir"))
                gpath = cmd.getOptionValue("gdir");
            if(cmd.hasOption("convert"))
                cpath = cmd.getOptionValue("convert");
            if(cmd.hasOption("identify"))
                ipath = cmd.getOptionValue("identify");
            if(cmd.hasOption("help")){
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "Ggoose", options );
            }
        } catch (ParseException e) {
            logger.error("Parameter error");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("parameters:", options);
            System.exit(1);
        }

        Main redgoo = new Main(rip, qj, cpath, ipath);
        redgoo.setCommandHandle();
        if (!redgoo.connect()){
            redgoo.stopThreadPool();
            DirectoryManager.deleteDir(redgoo.getGooseTmpDir());
            System.exit(1);
        }

        redgoo.go();
        redgoo.stopThreadPool();
        redgoo.disconnect();
        System.exit(0);
    }


}
