# Cloud-Computing-Project
This is a course project of cloud computing 

Build the project:

Go to the project directory and open the terminal.
    
run the following command: 
    
    _make build_
After that run: 
    
    _make jar_
This two command will build the the project and generate the jar file.

Put the graph file into the HDFS:

    hadoop fs -put grapgh_file_path hdfs_file_path
Perform pre-process: In the make file change the _graph_ path with the hdfs graph path that you put in the hadoop file system.

Add a path into the variable _preprocessedGraph_ and then perform the following command:

    make preprocess
Run the project:
    Add the _intermediatefile_ and _output_ file path in the Make file and perform the following command:
    
    make run
    
It will print the number of cluster and modularity of the cluster.