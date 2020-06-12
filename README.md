# Inverted-Index-using-Hadoop
- The aim of this project is to create an inverted list of files using Map-Reduce job in Java
- There are 2 tasks in this project:
  - Creating an Inverted index for Unigrams
  - Creating an Inverted index for Bigrams
- Large documents containing text extracted from HTML pages are given as input which are to be placed in the bucket of the cluster created on GCP's Dataproc
- The Hadoop job is further instructed to work on these input files to produce several output files. These files are further combined using Hadoop commands to form a single file of inverted index 
- The java functions for Map and Reduce are modified according to the format of desired output
- The result is a text file containing the unigram/bigram count in each file segregated by the document ID
- This file is used to quickly search for the count of the required unigram or bigram
- Refer to project_description folder for implementation details of the project

Technologies used:
  -
  - Language: Java
  - Cloud Platform: GCP
  - Apache Hadoop
