#!/bin/sh

home_dir=/home/gbecan

source $home_dir/.bashrc
rm $home_dir/.ssh/known_hosts

echo "deploying"
kadeploy3 -f $OAR_NODE_FILE -k -a $home_dir/opencompare/jessie-java8.env

echo "mounting NFS storage"
storage5k -a mount -j $OAR_JOB_ID

# Configuring environment
export http_proxy=http://proxy:3128/ && export https_proxy=https://proxy:3128/
#export PATH=$PATH:$home_dir/opencompare/spark-1.5.1-bin-spark-scala211-hadoop26/bin
#export PATH=$PATH:$home_dir/opencompare/spark-1.5.1-bin-spark-scala211-hadoop26/sbin

# Starting master
master=$(head -n 1 $OAR_NODE_FILE)
echo "Starting master on $master"
ssh $master "start-master.sh" 

# Starting slaves
cat $OAR_NODE_FILE | sort | uniq | while read node
do
		echo "Starting slave on $node"
		ssh -n $node "SPARK_WORKER_INSTANCES=14 SPARK_WORKER_CORES=1 start-slave.sh spark://$master:7077" &
done

# Start experiment
ssh -n $master "~/opencompare/submit-job.sh \"spark://$master:7077\" $@"
