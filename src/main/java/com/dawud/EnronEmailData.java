package com.dawud;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EnronEmailData {

    /********************************
     * APACHE SPARK SETUP           *
     ********************************/
    private SparkConf conf = null;
    // Create Spark Context from configuration
    private JavaSparkContext sc = null;


    /********************************
     * VARIABLES                    *
     ********************************/
    private List<String> listOfFiles = null;
    private String source_file_path = null;
    //    private static final String TEST_SOURCE = "C:\\Users\\dev\\Documents\\workspace\\data\\edrm-enron-v2\\xml version\\native_000\\*.eml";
    private static final String TEST_SOURCE = "C:\\Users\\dev\\Documents\\workspace\\data\\edrm-enron-v2\\pst_version\\3.438368.PK3OFMOYVKRD4XSYR1TCA4RA45VWBGM1B.txt";


    public void setListOfFiles(List<String> listOfFiles) {
        this.listOfFiles = listOfFiles;
    }

    public void setSource_file_path(String source_file_path) {
        this.source_file_path = source_file_path;
    }

    /********************************
     * CONSTANTS                    *
     ********************************/
    public static final String DELIMETER_HEADERS_END = "X-ZLID: "; // Email Headers end after this string in all emails.
    public static final String DELIMITER_TO_FIELD_NAME = "To: "; // TO Email address lists begin
    public static final String DELIMITER_CC_FIELD_NAME = "CC: "; // CC Email address lists begin



    /********************************
     * FUNCTIONS                    *
     ********************************/

    // The following are used for Finding the Top 100 Emails:-

    // FlatMap Function => get list of TO email addresses
    public static FlatMapFunction<String, String> flatMapFunctionTOEmail = msg -> {
        return EnronEmailData.transform_extractEmailAddresses(msg, DELIMITER_TO_FIELD_NAME);
    };

    // FlatMap Function => get list of CC email addresses
    public static FlatMapFunction<String, String> flatMapFunctionCCEmail = msg -> {
        return EnronEmailData.transform_extractEmailAddresses(msg, DELIMITER_CC_FIELD_NAME);
    };

    // Filter Predicate => Regular expression to match email string plus optional comma
    public static Function<String, Boolean> filterPredicateEmail = msg -> msg.toString().matches("<?([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})>?,?");

    // Pair Function => Score 1 count for each To email addrs, Score 0.5 for each Cc email addrs
    public static PairFunction<String, String, Double> pairFunctionFullScore = email -> new Tuple2(email.replace(",", ""), 1.0);
    public static PairFunction<String, String, Double> pairFunctionHalfScore = email -> new Tuple2(email.replace(",", ""), 0.5);

    // ReduceByKey Function => sum of count per email key => (email addrs key, sum(count))
    public static Function2<Double, Double, Double> reduceFunction = (accum, count) -> (accum + count);

    // Comparator for sorting the email addrs by scores
    public static Comparator<Tuple2<String, Double>> tupleComparator = Comparator.comparingDouble(tuple2 -> tuple2._2);



    // The following are used for Average word lengths:-

    // FlatMap Function => message body part after discarding the top part email headers.
    public static FlatMapFunction<String, String> flatMapFunctionMsgBody = msg -> {
//            Arrays.asList(
//                    transform_stripHeaders(
//                            transform_stripOriginalMessageHead(msg)
//                    ).split("\\s+"));
        return EnronEmailData.transform_stripHeaders(msg);

    };

    // Filter Predicate => Regular expression to match words (include hypenated words), filter out number
    public static Function<String, Boolean> filterPredicateWord = msg -> msg.toString().matches(".?[a-zA-Z{-}?]+.?");



    /********************************
     * MAIN PROGRAM STARTS HERE     *
     ********************************/

    public EnronEmailData() {

        System.out.println("Setting up Apache Spark....");
        // Setup Apache Spark
        conf = new SparkConf().setMaster("local").setAppName("Enron Email Data");
//        .set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        // Create Spark Context from configuration
        sc = new JavaSparkContext(conf);
    }

    public EnronEmailData(List<String> sourceFilePath) throws ExecutionException, InterruptedException {
        EnronEmailData enronEmailData = new EnronEmailData();
        enronEmailData.setListOfFiles(sourceFilePath);
        enronEmailData.start_Job();
    }

    /**
     * EXECUTION STARTS HERE
     */
    public void start_Job() throws ExecutionException, InterruptedException {
        Date startDate = new Date();
        long startTime = System.currentTimeMillis();

        if (listOfFiles != null && listOfFiles.size() > 0) {
            System.out.println("\n**************NOW PROCESSING EMAIL MESSAGE FILES**************");
            System.out.println("Please sit back and wait........");

            // spark driver variable to support parallel updating by associative and commutative operations
            final Accumulator<Double> accumAvgWordLength = sc.doubleAccumulator(0.0, "Average Word Length");
            final Accumulator<Integer> accumMsgCounter = sc.intAccumulator(0, "Message Counter");
            final Accumulator<Map<String, Double>> accumEmailAddrsCounter = sc.accumulator(new HashMap<String, Double>(), "Email Address Score", new MapCountAccumulator());


            // Use Java Multithreading to submit these in parallel worker threads that run in each Spark clusters.
            System.out.println("Setting up Java Thread Pools......");
            ExecutorService pool = Executors.newFixedThreadPool(50);

            List<Future<?>> futures = new ArrayList<>();
            for (final String f : listOfFiles) {

                List<String> filenames = openReceivedMessageFolder(f);
                System.out.println("PROCESSING CURRENT FOLDER: " + f + " => NUMBER OF MESSAGES FOUND: " + filenames.size());
                accumMsgCounter.add(filenames.size());

                for (String filename : filenames) {

                    Future<?> fut = pool.submit(new Runnable() {

                        @Override
                        public void run() {

                            JavaRDD<String> rddInput = sc.textFile(filename);

                            double averageWordLength = EnronEmailData.action_wordLengthAverage(rddInput);
                            // Update Spark driver accumulator
                            accumAvgWordLength.add(averageWordLength);

                            Map<String, Double> map = EnronEmailData.action_GetTop100Emails(rddInput);
                            accumEmailAddrsCounter.add(map);

                            System.out.println("PROCESSING CURRENT EMAIL: " + filename
                                    + " => Average word length: " + averageWordLength
                                    + "\n Email score: " + map);
                        }
                    });
                    futures.add(fut);

                }
            }


            pool.shutdown();
            while (!pool.isTerminated()) {
            }


            System.out.println("\n***************************RESULTS***************************");

            System.out.println("\n************************TOP 100 EMAILS***********************");

            accumEmailAddrsCounter.value()
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                    .limit(100)
                    .forEach(s -> System.out.println(s));


            System.out.println("\n**************AVERAGE WORD LENGTH IN ALL EMAILS**************");
            System.out.println("Average word length: " + accumAvgWordLength.value() / new Double(accumMsgCounter.value()));



            System.out.println("\n*************************JOB SUMMARY*************************");
            System.out.println("Date started: " + startDate);
            System.out.println("Date Ended: " + new Date());
            System.out.println("Total Number of Email Messages scanned: " + accumMsgCounter.value());
            System.out.println("Total time taken: " + (System.currentTimeMillis() - startTime) + "ms");


            System.out.println("\nStopping Apache Spark...");
            sc.stop();

        } else {
            System.out.println("Error no files found: " + source_file_path);
        }

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        List<String> source_folder_path = null;
        if (args.length < 1) {
            System.err.println("Usage: EnronEmailData <Enter Path to Email Folders or EML mail message or text message format files(*.eml or *.txt)>");
            System.exit(1);
        } else if (!new File(args[0]).exists()) {
            System.err.println("Error: File or Folder speicifed doesn't exist!: " + args[0]);
            System.exit(1);
        } else {

            if (args[0].toLowerCase().endsWith(".eml") || args[0].toLowerCase().endsWith(".txt")) {
                source_folder_path = Arrays.asList(args[0]);
            } else if (new File(args[0]).isDirectory()) {
                // make it work for subfolders
                source_folder_path = getFilteredFileList(args[0]);
            } else {
                System.err.println("Please enter folder path to Email Folders" +
                        "or Full filename for EML file or text file (*.eml or *.txt)>");
            }
        }


        System.out.println("PROCESSING EMAIL MESSAGE FILE(S)/FOLDER: " + args[0]);

        List<String> list = source_folder_path.stream()
                .map(s -> (s.substring(0, s.lastIndexOf(File.separator))))
                .distinct().collect(Collectors.toList());
        EnronEmailData enronEmailData = new EnronEmailData(list);

    }


    /********************************
     * TRANSFORMATION METHODS       *
     ********************************/

    /**
     * Extract Email Addresses in the To field from message text given the start & end of list DELIMITER values
     * Steps:
     * 1. Find start of email addresses list
     * 2. Check if index of delimiters is within email headers.
     * 3. Return sublist of containing only email addresses
     *
     * @param msg
     * @param startOfList
     * @return
     */
    public static List<String> transform_extractEmailAddresses(String msg, String startOfList) {

        List<String> lines = Arrays.asList(msg.split("\n"));        // <- Tokenise email msg into words by SPACE delimiter
        Optional<String> emailAddrsOptional = lines.stream()
                .filter(s -> s.startsWith(startOfList))
                .findFirst();

        String emailAddrs = (emailAddrsOptional.isPresent()) ? emailAddrsOptional.get() : "";
        return (emailAddrsOptional.isPresent()) ? Arrays.asList(emailAddrs.replace(startOfList, "").split(", ")) : Arrays.asList();
    }


    /**
     * Strip out the header part based on email header delimiter key => words
     *
     * @param msg
     * @return
     */
    public static List<String> transform_stripHeaders(String msg) {
        String delimiter = "\\s(" + DELIMETER_HEADERS_END + ").*\\s*";
        int delimiterIndex = 0;
        Pattern pattern = Pattern.compile(delimiter);
        Matcher matcher = pattern.matcher(msg);
        if (matcher.find()) {
            //System.out.println("Found:" + matcher.group() + "@Index: " + matcher.end());
            delimiterIndex = matcher.end();
        }
        String strippedMsg = msg.substring(delimiterIndex);
        return Arrays.asList(strippedMsg.split("[^a-zA-Z{-}?]+"));
    }


    /**
     * Strip out the header part based on email header delimiter key to return new substring
     * Steps:
     * 1. Strip line -> -----Original Message----- separator
     * 2. Strip line for email fields -> From:|To:|Cc:|Bcc:|Sent:|Subject:
     *
     * @param msg
     * @return
     */
    public static String transform_stripOriginalMessageHead(String msg) {
        String delimiterOriginalMsg = "\\s*.*((-){2,}.*Original Message.*(-){2,}).*\\s*"; //".*(-){2,}.*\\s*";;
        String delimiterEmailFields = "\\s*(From:|To:|Cc:|Bcc:|Sent:|Subject:).*";

        Pattern pattern = Pattern.compile("(" + delimiterOriginalMsg + "|" + delimiterEmailFields + ")");
        Matcher matcher = pattern.matcher(msg);
        if (matcher.find()) {
            msg = matcher.replaceAll("");
        }
        return msg;
    }


    /********************************
     * ACTION METHODS               *
     ********************************/


    /**
     * Split into words (include hypenated words), filter out number, junk text and stream into count of words
     *
     * @param msg
     * @return
     */
    public static long action_getWordCount(String msg) {
        List<String> msgWords = Arrays.asList(msg.split("\\s+"));
        long count = msgWords.stream()
                .filter(msgWord -> msgWord.matches(".?[a-zA-Z{-}?]+.?"))
                .count();
        return count;
    }

    /**
     * Recursive file list generator
     *
     * @param sourceDirectory
     * @return
     */
    public static List<String> openReceivedMessageFolder(String sourceDirectory) {
        File folder = new File(sourceDirectory);
        List<String> listOfFilePaths = new ArrayList<>();

        if (folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();

            if (listOfFiles.length > 0) {

                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        String filePath = file.getAbsolutePath();
                        if(file.length() <= 100000) {
                            if (filePath.endsWith(".txt") || filePath.endsWith(".eml")) {
                                listOfFilePaths.add(filePath);
                            }
                        }
                    } else {
                        // recursive so we addAll lists together
                        listOfFilePaths.addAll(openReceivedMessageFolder(file.getPath()));
                    }
                }
            } else {
                System.out.println("No Email Message files found in folder");
            }

        } else {
            System.out.println("Folder is not a directory");
        }

        return listOfFilePaths;
    }

    /**
     * Filter out stream of *.txt or *.eml files only
     *
     * @param filepath
     * @return
     */
    public static List<String> getFilteredFileList(String filepath) {
        List<String> output = EnronEmailData.openReceivedMessageFolder(filepath);
        return output.stream()
                .filter(s -> (s.endsWith(".txt") || s.endsWith(".eml")))
                .collect(Collectors.toList());
    }


    /**
     * ACTION - GET TOP 100 EMAILS
     *
     * @param rddInput
     * @return
     */
    public static Map<String, Double> action_GetTop100Emails(JavaRDD<String> rddInput) {

        /**
         * TOP 100 EMAILS
         */

        // Extract email address from the first occurence of 'To:' field and score 1.0 for each
        JavaPairRDD<String, Double> rddEmailAddrTo = rddInput
                .flatMap(flatMapFunctionTOEmail)                                    // <- Apply FlatMap Function => get list of TO email addresses
                .filter(filterPredicateEmail)                                       // <- Apply filter for email address regex
                .mapToPair(pairFunctionFullScore);                                  // <- Apply PairFunction => Score 1.0 point for each email

        // Extract email address from the first occurence of 'Cc:' field and score 0.5 for each
        JavaPairRDD<String, Double> rddEmailAddrCc = rddInput
                .flatMap(flatMapFunctionCCEmail)                                    // <- Apply FlatMap Function => get list of CC email addresses
                .filter(filterPredicateEmail)                                       // <- Apply filter for email address regex
                .mapToPair(pairFunctionHalfScore);                                  // <- Apply PairFunction => Score 1.0 point for each email

        // Union of To and CC Emails and respective sum of score
        JavaPairRDD<String, Double> top100Emails = rddEmailAddrTo
                .union(rddEmailAddrCc)                                              // <- Union of To and CC Emails RDD
                .reduceByKey(reduceFunction);                                       // <- Apply ReduceByKey function => (email addrs key, sum(count))

        // Collect result from all processed Spark partitions
        // Sort descending and output the top 100 results
/*        top100Emails.collect().stream()
                .sorted(tupleComparator.reversed())
                .limit(100)
                .forEach(s -> System.out.println(s));*/

//        System.out.println("\nTotal Email Addresses count: " + top100Emails.count());


        Map<String, Double> map = top100Emails.collectAsMap();
        return map;
    }

    /**
     * ACTION - AVERAGE WORD LENGTH IN ALL EMAILS
     * Returns Word count and Total of Word lengths back to the accumulator
     *
     * @param rddInput
     */
    public static double action_wordLengthAverage(JavaRDD<String> rddInput) {

        /**
         * AVERAGE WORD LENGTH IN ALL EMAILS
         */
        // Extract Email Message Body
        JavaPairRDD<String, Integer> rddWords = rddInput
                .flatMap(flatMapFunctionMsgBody)
                .filter(filterPredicateWord)              // <- Apply Filter to remove non word characters and numbers
                .mapToPair(word -> new Tuple2<>(word, word.length()));

        long totalLength = rddInput.flatMap(flatMapFunctionMsgBody)     // <- Apply flatMap function to strip headers, original msg headers
                .mapToPair(word -> new Tuple2<>(word, word.length()))   // <- Map word -> tuple (word, word length)
                .reduceByKey((accum, count) -> (accum + count))         // <- Reduce to sum word lengths by key
                .map(t -> t._2())                                       // <- Map to get length values
                .reduce((accum, count) -> (accum + count));             // <- Sum of word lengths
//                .foreach(s -> System.out.println(s));

        long count = rddInput.flatMap(EnronEmailData.flatMapFunctionMsgBody).count();
        // Average word length
        return new Double((double) totalLength / (double) count);
    }
}