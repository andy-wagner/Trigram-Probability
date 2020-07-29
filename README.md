# Trigram Probability

#### Nimrod Zur & Hadar Harari

## Environment Installation:
1. To have default roles that match EMR create Cluster and terminate it.
2. Create a bucket for logs and edit the StaticVars.LOGS_BUCKET_NAME variable
3. Add the name of your key pair in StaticVars.KEY_PAIR variable
4. Create access key and add the access key Id and secret key in c:\users\username\.aws\credentials the values.
5. Create output and counter bucket and save the name in StaticVars.OUTPUT_BUCKET_NAME variable
6. Create a jar folder and put there all Hadoop MapReduce jar files and save the name in StaticVars.JAR_BUCKET_NAME variable.


## These are the following map-reduce steps in order:

### 1. Word count
    Description: For each word, calculate how many times it shows up in the 1grams. 
                 Attach this number to each trigram it is a part of.
    map input - google 3grams and google 1grams
    reducer output -    key: (Text) trigram
                        value: (Object- Text, LongWritable) word, number of occurrences 

### 2. Count c0
    Description: Count the number of occurrences of all the words in 1grams.
                 Save this number using a counter and upload the counter to the output bucket.
    map input - google 1grams
    reducer output- none (only produces the counter file)
                 
### 3. Pair count
    Description: For each pair of words, calculate how many times they show up in the 2grams.
                 Attach this number to each trigram they are a part of.
    map input - google 3grams and google 2grams
    reducer output -    key: (Text) trigram
                        value: (Object- Text, LongWritable) pair of words, number of occurrences 

### 4. Trigram count
    Description: Count the number of occurrences for each trigram in 3grams.
                 For each trigram pass its number of occurrences.
    map input - google 3grams
    reducer output -    key: (Text) trigram
                        value: (Object- Text, LongWritable) trigarm, number of occurences 
    
### 5. Probability Calculator
    Description: For each trigram, the reducer receives the number of occurrences for each of his words, pair of words, the entire trigram and takes C0 from the bucket and calculates the probability for this trigram.
    map input - results from 1, 2, 3 and 4.
    reducer output -    key: (Text) trigram
                        value: (DoubleWritable) probability 
    
### 6. Sort results
    Description: Sorts the results so they are in the same file and according to the specified order. 
    map1 input - results from 5.
    reducer output -    key: (Text) trigram
                        value: (DoubleWritable) probability
    Notes: The sort is achieved using a key that for each trigram holds his first to words and his probability. 
           After it is finished we return the pairs to their original state (key: trigram value: probability).

#### Link for the output bucket in S3:    
https://s3.console.aws.amazon.com/s3/buckets/outputdist2bucket/?region=us-east-1&tab=overview