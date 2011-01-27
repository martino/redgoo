package main.java.eu.ahref;

import com.jimplush.goose.Article;
import com.jimplush.goose.Configuration;
import com.jimplush.goose.ContentExtractor;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: martino
 * Date: 14/01/11
 * Time: 9.56
 * To change this template use File | Settings | File Templates.
 */


public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);


    Jedis jedis;
    String redisURL;
    String jobsQueue;
    //TODO parametrizzarli
    String redgooNum = "redgoo:nsites";
    String redgooSiteList = "redgoo:kosites";
    Configuration gooseConfig;

    public Main(String rurl, String jq, String gpath, String cpath, String ipath){
        redisURL = (rurl==null) ? "localhost" : rurl;
        jobsQueue = (jq == null) ? "readability:jobs" : jq;
        gooseConfig = new Configuration();
        gooseConfig.setLocalStoragePath((gpath==null) ? "/tmp/goose" : gpath);
        gooseConfig.setImagemagickConvertPath((cpath==null) ? "/usr/bin/convert" : cpath);
        gooseConfig.setImagemagickIdentifyPath((ipath==null) ? "/usr/bin/identify" : ipath);


        System.out.println(redisURL+ " "+jobsQueue);
    }

    public Main(){
        this.redisURL = "localhost";
        this.jobsQueue = "readability:jobs";
        gooseConfig = new Configuration();
        gooseConfig.setLocalStoragePath("/tmp/goose");
        gooseConfig.setImagemagickConvertPath("/usr/bin/convert");
        gooseConfig.setImagemagickIdentifyPath("/usr/bin/identify");
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

    public void go(){

        boolean run = true;
        List<String> resultRedis;
        while(run){
            try{
                resultRedis =  jedis.blpop(0,this.jobsQueue);
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


        Main ggoose = new Main(rip, qj, gpath, cpath, ipath);
        if (!ggoose.connect())
            System.exit(1);

        ggoose.go();
        System.exit(0);
        //ggoose.go("readability:jobs","readability:commands");
        //System.out.println("Exit");

        //ggoose.testUrl(error);
        //ggoose.testjedis();


    }
}
