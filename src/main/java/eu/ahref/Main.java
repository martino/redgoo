package eu.ahref;

import com.jimplush.goose.Article;
import com.jimplush.goose.Configuration;
import com.jimplush.goose.ContentExtractor;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

/**
 * Redgoo main class
 *
 * @author  Martino Pizzol
 */

public class Main implements Observer{
    private static final Logger logger = Logger.getLogger(Main.class);

    Jedis jedis;
    String redisURL;
    String jobsQueue;
    //TODO parametrizzarli
    String redgooNum = "redgoo:nsites";
    String redgooSiteList = "redgoo:kosites";
    SignalHandler sh = null;
    Boolean gracefulExit = false;
    Configuration gooseConfig;
    int timeout = 30; // timeout for blpop

    public Main(String rurl, String jq, String gpath, String cpath, String ipath){
        redisURL = (rurl==null) ? "localhost" : rurl;
        jobsQueue = (jq == null) ? "readability:jobs" : jq;
        gooseConfig = new Configuration();
        gooseConfig.setLocalStoragePath((gpath==null) ? "/tmp/goose" : gpath);
        gooseConfig.setImagemagickConvertPath((cpath==null) ? "/usr/bin/convert" : cpath);
        gooseConfig.setImagemagickIdentifyPath((ipath==null) ? "/usr/bin/identify" : ipath);
    }

    public Main(){
        this.redisURL = "localhost";
        this.jobsQueue = "readability:jobs";
        gooseConfig = new Configuration();
        gooseConfig.setLocalStoragePath("/tmp/goose");
        gooseConfig.setImagemagickConvertPath("/usr/bin/convert");
        gooseConfig.setImagemagickIdentifyPath("/usr/bin/identify");
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
                JSONObject jwork = null;
                String url=null;
                try {
                    jwork = new JSONObject(jswork);
                    url = jwork.getString("url");
                    logger.debug("URL: "+url);
                    jedis.incr(this.redgooNum);
                    //String original = jwork.getString("original");

                    ContentExtractor contentExtractor = new ContentExtractor(gooseConfig);
                    Article article = null;
                    try{
                        if(url != null){
                            article = contentExtractor.extractContent(url);
                        }/*else if(original!=null){
                        //TODO capire come gestire il testo senza url

                    }  */
                        JSONObject jout = new JSONObject();
                        jout.put("html", article.getCleanedArticleText());
                        jout.put("title", article.getTitle());
                        jout.put("image", article.getTopImage().getImageSrc());
                        jout.put("domain", article.getDomain());
                        jout.put("original",article.getOriginalDoc());
                        jedis.publish(jwork.getString("id"),jout.toString());
                    }catch(Exception e){
                        JSONObject jerr = new JSONObject();
                        jerr.put("status", "error");

                        jedis.publish(jwork.getString("id"),jerr.toString());

                        jedis.rpush(this.redgooSiteList,url);
                        logger.debug("Goose exception\n"+ e.getStackTrace().toString());
                    }
                } catch (JSONException e) {
                    logger.error("JSON exception\n"+e.getStackTrace().toString());
                    return;
                }
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


        Main redgoo = new Main(rip, qj, gpath, cpath, ipath);
        redgoo.setCommandHandle();
        if (!redgoo.connect())
            System.exit(1);

        redgoo.go();
        redgoo.disconnect();
        System.exit(0);
    }


}
