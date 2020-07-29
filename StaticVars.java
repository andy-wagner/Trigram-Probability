public class StaticVars {


    public enum Counter {
        COUNTER_0
    }

    public static final String KEY_PAIR = "dspsKeyPair";
    public static final String JAR_BUCKET_NAME = "jarfilesstepsdistbucket";
    public static final String OUTPUT_BUCKET_NAME = "outputdistbucket";
    public static final String LOGS_BUCKET_NAME = "logsbucketdist";

    public static final String WORD_COUNT_FILE_NAME = "WordCount.jar";
    public static final String COUNT_C0_FILE_NAME = "CountC0.jar";
    public static final String PAIR_COUNT_FILE_NAME = "PairCount.jar";
    public static final String TRIGRAM_COUNT_FILE_NAME = "TrigramCount.jar";
    public static final String CALCULATOR_FILE_NAME = "ProbCalculator.jar";
    public static final String SORT_FILE_NAME = "SortResults.jar";

    public static final String Corpus_PATH = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    public static final String Corpus_Pair_PATH = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    public static final String Corpus_Single_PATH = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    public static final String WORD_COUNT_OUTPUT_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/outputWordCount";
    public static final String COUNT_C0_OUTPUT_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/outputCountC0";
    public static final String PAIR_COUNT_OUTPUT_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/outputPairCount";
    public static final String TRIGRAM_COUNT_OUTPUT_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/outputTrigramCount";
    public static final String PROB_OUTPUT_PATH = "s3n://" + OUTPUT_BUCKET_NAME + "/outputProbability";
    public static final String FINAL_RESULT_PATH = "s3n://" + OUTPUT_BUCKET_NAME + "/final_result";

}
