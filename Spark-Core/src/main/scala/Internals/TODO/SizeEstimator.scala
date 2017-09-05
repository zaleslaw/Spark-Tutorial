package Internals.TODO

/*
Determining Memory Consumption
The best way to size the amount of memory consumption a dataset will require is to create an RDD, put it into cache, and look at the “Storage” page in the web UI. The page will tell you how much memory the RDD is occupying.

To estimate the memory consumption of a particular object, use SizeEstimator’s estimate method This is useful for experimenting with different data layouts to trim memory usage, as well as determining the amount of space a broadcast variable will occupy on each executor heap.
 */
object SizeEstimator {

}
