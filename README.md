# About the project

This project's goal is to extend the machine learning library [Mahout](http://mahout.apache.org/) with
[attribute selection](http://en.wikipedia.org/wiki/Feature_selection) functionality. It is developed by an informal
research group at the [Faculty of Mathematics, Informatics and Mechanics](http://www.mimuw.edu.pl/) at the
[University of Warsaw](http://www.uw.edu.pl/). Members of the group are the following MSc students in mathematics:
Paweł Olszewski, Krzysztof Rutkowski and Wiktor Gromniak. It is supervised by Professor Andrzej Skowron, who is heading
the Group of Logic.

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

The library is currently not designed to handle _big data_, it requires the data to fit into a single machine's memory.
It takes advantage of MapReduce's and Spark's capabilities to parallelize computationally intensive algorithms on a
number of machines. Although, it should be easy to extend the library to operate fully on HDFS storage and handle true
_big data_.

Currently there exist the following modes of computation:

* For reducts-based framework: standalone (on a single computer), MapReduce, Spark
* For trees-based framework: standalone, MapReduce

References:

* [1] Marcin Kruczyk et. al, [_Random Reducts: A Monte Carlo Rough Set-based Method for Feature Selection in Large Datasets_]
    (http://iospress.metapress.com/content/53556177225267w6/), 2013
* [2] Jan G. Bazan et al., [_Dynamic Reducts as a Tool for Extracting Laws from Decisions Tables_]
    (http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.56.6411), 1994
* [3] M Draminski et al., [_Monte Carlo feature selection for supervised classification._]
    (http://www.ncbi.nlm.nih.gov/pubmed/18048398), 2008

# Running the project

TODO
