## About the project

This project's goal is to extend the machine learning library [Mahout](http://mahout.apache.org/) with
[attribute selection](http://en.wikipedia.org/wiki/Feature_selection) functionality. It is developed by an informal
research group at the [Faculty of Mathematics, Informatics and Mechanics](http://www.mimuw.edu.pl/) at the
[University of Warsaw](http://www.uw.edu.pl/). It is also developed in collaboration with [UK PhD Centre in Financial 
Computing](http://www.financialcomputing.org/) as part of DRACUS project. Members of the group are the following MSc 
students in mathematics: Paweł Olszewski, Krzysztof Rutkowski and Wiktor Gromniak. At University of Warsaw the work 
is supervised by Professor Andrzej Skowron, who is heading the Group of Logic. At UK PhD Centre in Financial
Computing it is supervised by Michał Galas PhD.

The library currently features two attribute selection frameworks:

* Random / Dynamic reducts - an algorithm based on the paper Random Reducts [1] by Kruczyk et al., extended with the
  ideas of Dynamic Reducts [2] by Bazan et al.
* Monte Carlo Feature Selection - an algorithm based on random forest-like approach found in Monte Carlo Feature
  Selection [3] by Draminski et al.

The library uses, among others, the following two machine learning libraries:

* [Rseslib](http://rseslib.mimuw.edu.pl/) - " *a library of machine learning data structures and algorithms implemented
  in Java* "  featuring algorithms for reduct's calculations
* [Cognitive Foundry](http://www.cognitivefoundry.org/) - a great, general-purpose, well-coded, well-documented library
  of machine learning algorithms

The library is currently only partially designed to handle _big data_, most of the algorithms' implementations require
the data to fit  into a single machine's memory. Although, work on _big data_ support is in progress and there is
already one implementation (reducts on Spark, see below) that handles any size of data your cluster can handle (tested
with data up to 1TB in size). The former algorithms take advantage of MapReduce's and Spark's capabilities to
parallelize computationally intensive algorithms on a number of machines.

Currently there exist the following modes of computation:

* For reducts-based framework: standalone (on a single computer), MapReduce, Spark, Spark Big Data
* For trees-based framework: standalone, MapReduce, Spark

References:

* [1] Marcin Kruczyk et. al, [_Random Reducts: A Monte Carlo Rough Set-based Method for Feature Selection in Large Datasets_]
    (http://iospress.metapress.com/content/53556177225267w6/), 2013
* [2] Jan G. Bazan et al., [_Dynamic Reducts as a Tool for Extracting Laws from Decisions Tables_]
    (http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.56.6411), 1994
* [3] M Draminski et al., [_Monte Carlo feature selection for supervised classification_]
    (http://www.ncbi.nlm.nih.gov/pubmed/18048398), 2008

## Running the project

To build the project we use gradle. Currently, there are gradle tasks designed to build one JAR for each computation 
mode (see above; we're planning to put it into a single JAR or extend the Mahout JAR).

To run the project:

1. Clone the repository
  ```
  git clone https://github.com/wgromniak/mahout-extensions.git
  ```

2. Build a JAR of your interest. Available tasks
  * jar4SparkReducts
  * jar4MapRedReducts
  * jar4StandaloneReducts
  * jar4MapRedTrees
  * jar4SparkTrees
  * jar4StandaloneTrees

3. Run the selected task using gradle wrapper, e.g. (in the project directory)
  ```
  ./gradlew jar4SparkReducts
  ```
  This will create a JAR in ```mahout-extensions/build/libs/```.

4. Run the JAR
  * on Spark, e.g. (your command will depend on your cluser configuration, [read more](https://spark.apache.org/docs/latest/submitting-applications.html))
  
      ```
      spark-submit --class org.mimuw.attrsel.trees.spark.TreeAttrSelDriver --master yarn-cluster mahout-extensions-spark-trees.jar -i myData.csv -numSub 5000 -subCard 150 -numCutIter 2 -subGen org.mimuw.attrsel.common.MatrixFixedSizeAttributeSubtableGenerator
      ```
  * on Hadoop, e.g 
  
     ```
     hadoop jar mahout-extensions-mapred-trees.jar -i myData.csv -o outputDir -numSub 5000 -subCard 150 -subGen org.mimuw.attrsel.common.MatrixFixedSizeAttributeSubtableGenerator
     ```
  * standalone, e.g 
  
     ```
     java -jar mahout-extensions-standalone-reducts.jar -i myData.csv -numSub 5000 -subCard 150
     ```

##### Notes on running

* To get all available configuration options run the JARs with ```--help``` option.
* Spark needs the data in HDFS, MapReduce in a local directory (this will be made consistent in the near future).
* Input data should be in CSV format. Objects are in rows, attributes are in columns, last column is always the decision attribute (only integer decision is allowed). Absent values are not possible.
* If you want to run the Reducts Spark Big Data version run the ```mahout-extensions-spark-reducts.jar``` with ```--class org.mimuw.attrsel.reducts.spark.BigDataAttrSelDriver```.
* You might need to run Spark with options such as ```--driver-memory 8g --executor-memory 2g --driver-java-options "-Dspark.akka.frameSize=1000" --num-executors 50``` to make it work. Running spark is tricky, cf. [_Spark should be better than MapReduce (if only it worked)_](http://blog.explainmydata.com/2014/05/spark-should-be-better-than-mapreduce.html). If you need help running the project, please contact us, we've probably already seen all the weired exceptions you get.
