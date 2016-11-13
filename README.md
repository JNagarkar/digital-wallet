# Instructions for running the tests:

1. Place the antifraud.java file in the src/ folder 
2. Run.sh is modified so as to run java program. 
3. Batch_input.txt and Stream_input.txt have not been included, you will require to place that paymo_input folder.  

## Assumptions about the dataset:

1. In couple of cases, Payer and Payee were found to be the same (In such case,the transaction is considered to be trusted). 
2. In some cases, either the Payer or Payee were not found to be present in the batch_input.txt, Such transaction is considered to be unverified. 

## Techniques used to speed-up:

1. If you are running large amount of streaming data, there is a provision to increase the number of threads. (By-default, 10 threads are being used). 
2. Two Level lookup has been provided in the searching algorithm to decrease number of iterations. 

## Details of implementation

Modified BFS is used to find out the degree of friendship between 2 entities. If the degree of friendship is > 4, without further attempts to find out the actual degree, the solution returns that the given transaction is unverified.
