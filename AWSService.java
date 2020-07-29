import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


public class AWSService {

    private static AmazonS3 s3;
    private static AmazonElasticMapReduce mapReduce;

    private static boolean checkIfLegalWord(String word) {
        word = word.trim();
        boolean checkSpace = false;
        for (int i = 0; i < word.length(); i++) {
            if(((int)word.charAt(i) < 1488 || (int)word.charAt(i) > 1514) && (int)word.charAt(i) != 32 )
                return false;
            if((int)word.charAt(i) >= 1488 && (int)word.charAt(i) <= 1514) {
                checkSpace = true;
            }
        }
        return checkSpace;
    }

    public static void main(String[] args) {
        init();

          ///////WordCount////////////////////////////

        HadoopJarStepConfig WordCountStep = new HadoopJarStepConfig()
                .withMainClass(WordCount.class.toString())
                .withJar("s3n://" + StaticVars.JAR_BUCKET_NAME + "/" + StaticVars.WORD_COUNT_FILE_NAME) // This should be a full map reduce application.
                .withArgs(StaticVars.Corpus_PATH, StaticVars.Corpus_Single_PATH, StaticVars.WORD_COUNT_OUTPUT_PATH);

        StepConfig stepConfig1 = new StepConfig()
                .withName("WordCountStep")
                .withHadoopJarStep(WordCountStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        ///////CountC0////////////////////////////

        HadoopJarStepConfig CountC0Step = new HadoopJarStepConfig()
                .withMainClass(CountC0.class.toString())
                .withJar("s3n://" + StaticVars.JAR_BUCKET_NAME + "/" + StaticVars.COUNT_C0_FILE_NAME) // This should be a full map reduce application.
                .withArgs(StaticVars.Corpus_Single_PATH, StaticVars.COUNT_C0_OUTPUT_PATH);

        StepConfig stepConfig2 = new StepConfig()
                .withName("CountC0Step")
                .withHadoopJarStep(CountC0Step)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////PairCount////////////////////////////

        HadoopJarStepConfig PairCountStep = new HadoopJarStepConfig()
                .withMainClass(PairCount.class.toString())
                .withJar("s3n://" + StaticVars.JAR_BUCKET_NAME + "/" + StaticVars.PAIR_COUNT_FILE_NAME) // This should be a full map reduce application.
                .withArgs(StaticVars.Corpus_PATH, StaticVars.Corpus_Pair_PATH, StaticVars.PAIR_COUNT_OUTPUT_PATH);

        StepConfig stepConfig3 = new StepConfig()
                .withName("PairCountStep")
                .withHadoopJarStep(PairCountStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////TrigramCount////////////////////////////

        HadoopJarStepConfig TrigramCountStep = new HadoopJarStepConfig()
                .withMainClass(TrigramCount.class.toString())
                .withJar("s3n://" + StaticVars.JAR_BUCKET_NAME + "/" + StaticVars.TRIGRAM_COUNT_FILE_NAME) // This should be a full map reduce application.
                .withArgs(StaticVars.Corpus_PATH, StaticVars.TRIGRAM_COUNT_OUTPUT_PATH);

        StepConfig stepConfig4 = new StepConfig()
                .withName("TrigramCountStep")
                .withHadoopJarStep(TrigramCountStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////ProbCalculator////////////////////////////
        HadoopJarStepConfig ProbCalculatorStep = new HadoopJarStepConfig()
                .withJar("s3n://" + StaticVars.JAR_BUCKET_NAME + "/" + StaticVars.CALCULATOR_FILE_NAME) // This should be a full map reduce application.
                .withArgs(StaticVars.COUNT_C0_OUTPUT_PATH, StaticVars.WORD_COUNT_OUTPUT_PATH,
                        StaticVars.PAIR_COUNT_OUTPUT_PATH, StaticVars.TRIGRAM_COUNT_OUTPUT_PATH,
                        StaticVars.PROB_OUTPUT_PATH);

        StepConfig stepConfig5 = new StepConfig()
                .withName("ProbCalculator")
                .withHadoopJarStep(ProbCalculatorStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////SortResults////////////////////////////
        HadoopJarStepConfig SortResultsStep = new HadoopJarStepConfig()
                .withJar("s3n://" + StaticVars.JAR_BUCKET_NAME + "/" + StaticVars.SORT_FILE_NAME) // This should be a full map reduce application.
                .withArgs(StaticVars.PROB_OUTPUT_PATH,
                        StaticVars.FINAL_RESULT_PATH);

        StepConfig stepConfig6 = new StepConfig()
                .withName("SortResults")
                .withHadoopJarStep(SortResultsStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////jobflow/////////////////////////////////
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.6.2")
                .withEc2KeyName(StaticVars.KEY_PAIR)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("hadoopProject")
                .withInstances(instances)
                .withSteps(stepConfig2)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0")
                .withVisibleToAllUsers(true)
                .withLogUri("s3n://" + StaticVars.LOGS_BUCKET_NAME + "/logs/");

        System.out.println("RunJobFlowRequest: " + runFlowRequest.toString());

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static void init() {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }
}