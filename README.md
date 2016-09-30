# wikipedia-dump-analysis
Experiment on dumps of Wikipedia


# Starting the experiment on Grid5000
Example of a job with 15 nodes on paravance cluster
```
oarsub -l "{cluster='paravance'}/nodes=15,walltime=2" -t deploy "./start-spark-cluster.sh /PATH/TO/WIKIPEDIA/DUMP/en.preprocessed.xml.bz2 en /PATH/TO/OUTPUT/DIRECTORY false 1000"
```
Parameters
- path to dump (bz2 file)
- language of the dump (e.g. en for the English version of Wikipedia)
- path to output directory
- boolean that indicates if the program need to export the PCMs. WARNING: it takes a lot of space !
- number of partitions for spark (1000 seems quite good)
