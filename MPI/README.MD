Let us assume the water surface as 2D square and wave simulation is given by the equation
Zt_i,j = 2.0 * Zt-1_i,j – Zt-2_i,j + c2 * (dt/dd) 2 * (Zt-1_i+1,j + Zt-1_i-1,j + Zt-1_i,j+1 + Zt-1_i,j- 1
– 4.0 * Zt-1_i,j) for t >= 2
Here c = 1, dt = 0.1 and dd = 2
This is part of the computation that we will focus on for parallelization
The computation of wave simulation based on Schrödinger’s equation is a perfect candidate for distributing a linear space to N stripes and each stripe being calculated by independent computing nodes. This is a classic example of data distribution. However, each computing node would require some data from their adjacent ranks to calculate the edges. For this, we can leverage the MPI to exchange boundaries and facilitate all other communications between ranks. The task of printing the simulation is assigned to rank 0 or the master rank. All other ranks(nodes) calculate the simulation for their respective assigned stripe and send this message back to rank 0. Rank 0 then collects all the data and then takes care of the printing process.
The simulation size and the number of threads are broadcast to all ranks. Based on the simulation size each node calculates the range of their computation boundary.
Mathematically, this can be expressed as:
stripe = size / mpi_size remainder = size % mpi_size if(rank < remainder)
stripe_begin = stripe * rank + rank
stripe_end = stripe_begin + stripe else
stripe_begin = stripe * rank + remainder
stripe_end = stripe_begin + stripe – 1 stripe_size = stripe_end – stripe_begin + 1
mpi_size => No of computing nodes
size => Simulation Size
rank => Current rank of the computing node. stripe => Number of partitions
The Odd-Even communication strategy, also known as Odd-Even Transposition or Odd-Even Sort, is a method employed in parallel computing to facilitate communication between computing nodes while minimizing the risk of deadlocks. In this approach, even-ranked nodes initiate communication by sending data to their neighbouring nodes before receiving data from them. Conversely, odd-ranked nodes first receive data from their neighbours and then send data to them. This strategy is particularly useful for boundary data exchange in our context.
Each rank should receive boundary data located at stripe_begin - 1 and stripe_end + 1 before starting computation. Since these locations reside in areas assigned to neighbouring nodes, MPI communication is required between machines. Notably, Rank 0 and Rank N - 1 do not have left and right neighbours, respectively, and will only send or receive a single set of data.
 
After completing the boundary data exchange, we can initiate the computation phase. To enhance performance within each stripe, we can leverage multithreading parallelism. Each of our computing nodes is equipped with 4 cores. Assuming we designate the number of threads as 'y,' each thread will execute computations in parallel for a segment of (stripe_end - stripe_begin)/y. This strategy is not only thread-safe but also quite efficient. Nevertheless, it does suffer from a notable drawback: the absence of overlap between communication and computation, which can adversely affect overall performance. One might consider alternative parallelization strategies, such as asynchronous I/O or pipelining, to overlap communication and computation. However, in this implementation, the strategy is to separate the communication and computation process to keep the parallelism complexity low.
Mathematically, the number of parallel computations = rank * thread.
In this assignment, we have a maximum of 6 machines and each machine has 4 cores. We have observed the best performance for the combination of 6 machines and 2 threads.
Here is a plot of the performance with respect to threads and machines
Note : Detailed execution statistics are available in the execution summary report.
In conclusion, parallelization can significant help boost the performance of computation. However there is always an inherent overhead associated with parallel computing (tcomm(n,p))
tparallel(n,p) = tcalc(n,p) + ti/o(n,p) + tcomm(n,p)
tcomm(n,p) = ts + ktc where ts : Latency, k : data units, tc : Bandwidth
Hence, keeping this formula in mind while designing any parallel system we must aggregate as many short messages as possible, minimize as many synchronizations as possible, and try to achieve maximum overlap in communication and computation. Keeping the overhead as low as possible will help us achieve linear performance improvement.
Amdahl’s law must be kept in mind to get the maximum benefit of parallelism. The cost of parallelization overhead and sequential execution should be kept at a minimum. 
