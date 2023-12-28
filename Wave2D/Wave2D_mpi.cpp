#include <iostream>
#include "Timer.h"
#include <stdlib.h>   // atoi
#include "mpi.h"
#include "omp.h"


int default_size = 100;  // the default system size
double c = 1.0;      // wave speed
double dt = 0.1;     // time quantum
double dd = 2.0;     // change in system

using namespace std;



int main( int argc, char *argv[] ) {
  int my_rank, mpi_size, stripe, remainder, stripe_begin, stripe_end, stripe_size;
  int tag = 0;
  MPI_Status status;

  // verify arguments
  if ( argc != 5 ) {
    cerr << "usage: Wave2D size max_time interval thread" << endl;
    return -1;
  }
  int size = atoi( argv[1] );
  int max_time = atoi( argv[2] );
  int interval  = atoi( argv[3] );
  int thread  = atoi( argv[4] );

  if ( size < 100 || max_time < 3 || interval < 0 ) {
    cerr << "usage: Wave2D size max_time interval" << endl;
    cerr << "       where size >= 100 && time >= 3 && interval >= 0" << endl;
    return -1;
  }

  // create a simulation space
  double z[3][size][size];

  for ( int p = 0; p < 3; p++ ) 
      for ( int i = 0; i < size; i++ )
        for ( int j = 0; j < size; j++ )
          z[p][i][j] = 0.0; // no wave

  // start a timer
  Timer time;
  time.start( );

  // time = 0;
  // initialize the simulation space: calculate z[0][][]
  int weight = size / default_size;
  for( int i = 0; i < size; i++ ) {
    for( int j = 0; j < size; j++ ) {
      if( i > 40 * weight && i < 60 * weight  &&
          j > 40 * weight && j < 60 * weight ) {
        z[0][i][j] = 20.0;
      } else {
        z[0][i][j] = 0.0;
      }
    }
  }

  // time = 1
  // calculate z[1][][]
  for( int i = 1; i < size - 1; i++ ) {
    for( int j = 1; j < size - 1; j++ ) {
        z[1][i][j] = z[0][i][j] + 
        (c * c)/2 * (dt * dt) / ( dd * dd) * 
        ( z[0][i + 1][j] + z[0][i - 1][j] + z[0][i][j + 1] + z[0][i][j - 1] - 4.0 * z[0][i][j]);
    }
  }


  //Initiate MPI
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  stripe = size / mpi_size;
  remainder = size % mpi_size;

  stripe_begin = ( my_rank < remainder ) ? (stripe * my_rank) + my_rank : (stripe * my_rank) + remainder;
  stripe_end = (my_rank < remainder) ? stripe_begin + stripe  : stripe_begin + stripe - 1;
  stripe_size = stripe_end - stripe_begin + 1;

  //Broadcase the size of the array and number of threads to all machine
  MPI_Bcast(&size, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(&thread, 1, MPI_INT, 0, MPI_COMM_WORLD);
  

  for ( int time = 2; time < max_time; time++ ) {
    int p = time % 3;
    int q = (time + 2) % 3;
    int r = (time + 1) % 3;

    //This block handles printing. Printing happens only time is divisible ny interval or it is max time
    if(interval != 0 && (time % interval == 0 || time == max_time - 1)) {
    
    if(my_rank == 0) {

        for(int dest = 1; dest < mpi_size; dest++) {
          int begin_dest = ( dest < remainder ) ? (stripe * dest) + dest : (stripe * dest) + remainder;
          int end_dest =  (dest < remainder) ? begin_dest + stripe  : begin_dest + stripe - 1;
          int dest_size = end_dest - begin_dest + 1;
          MPI_Recv(&z[p][begin_dest], dest_size * size, MPI_DOUBLE, dest, 1, MPI_COMM_WORLD, &status);
        }
      
        //Printing block
        cout << time << endl;
        for (int j = 0; j < size; j++) {
          for (int i = 0; i < size; i++) {
            cout << z[p][i][j] << " ";
          }
          cout << endl;
        }
        cout << endl;
    } 
    else {
      MPI_Send(&z[p][stripe_begin], stripe_size * size, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
    }
  }
    
    
    //Data exchange between machine boundary.
    //Even rank will always send first and odd rank will receive first.
    //These will prevent deadlock 
    if(my_rank % 2 == 0) {
        if(my_rank > 0 ) {
          MPI_Send(&z[q][stripe_begin], size, MPI_DOUBLE, my_rank - 1, tag, MPI_COMM_WORLD);
          MPI_Recv(&z[q][stripe_begin - 1], size, MPI_DOUBLE, my_rank - 1, tag, MPI_COMM_WORLD, &status);
        }
        if(my_rank < mpi_size - 1) {
          MPI_Send(&z[q][stripe_end], size, MPI_DOUBLE, my_rank + 1, tag, MPI_COMM_WORLD);
          MPI_Recv(&z[q][stripe_end + 1], size, MPI_DOUBLE, my_rank + 1, tag, MPI_COMM_WORLD, &status);
        }

    } else {
      if(my_rank > 0 ) {
        MPI_Recv(&z[q][stripe_begin - 1], size, MPI_DOUBLE, my_rank - 1, tag, MPI_COMM_WORLD, &status);
        MPI_Send(&z[q][stripe_begin], size, MPI_DOUBLE, my_rank - 1, tag, MPI_COMM_WORLD);
      }
      if(my_rank < mpi_size - 1) {
        MPI_Recv(&z[q][stripe_end + 1], size, MPI_DOUBLE, my_rank + 1, tag, MPI_COMM_WORLD, &status);
        MPI_Send(&z[q][stripe_end], size, MPI_DOUBLE, my_rank + 1, tag, MPI_COMM_WORLD);
      }
    }

    // Computation with multithreading
    if(my_rank == 0) {
      omp_set_num_threads( thread );
      #pragma omp parallel for
      for (int i = 1; i <= stripe_end; i++) {
      for (int j = 1; j < size - 1; j++) {
        z[p][i][j] = (2.0 * z[q][i][j]) 
        - z[r][i][j]
        + (c * c * dt * dt / (dd * dd))
          * (z[q][i + 1][j] + z[q][i - 1][j]
              + z[q][i][j + 1] + z[q][i][j - 1]
              - 4.0 * z[q][i][j]);
        }
      }

    }
    else if(my_rank == mpi_size - 1) {
      omp_set_num_threads( thread );
       #pragma omp parallel for
       for (int i = stripe_begin; i < stripe_end; i++) {
        for (int j = 1; j < size - 1; j++) {
          z[p][i][j] = (2.0 * z[q][i][j]) 
          - z[r][i][j]
          + (c * c * dt * dt / (dd * dd))
            * (z[q][i + 1][j] + z[q][i - 1][j]
                + z[q][i][j + 1] + z[q][i][j - 1]
                - 4.0 * z[q][i][j]);
          }
      }

    }
    else{
      omp_set_num_threads( thread );
      #pragma omp parallel for
      for (int i = stripe_begin; i <= stripe_end; i++) {
        for (int j = 1; j < size - 1; j++) {
          z[p][i][j] = (2.0 * z[q][i][j]) 
          - z[r][i][j]
          + (c * c * dt * dt / (dd * dd))
            * (z[q][i + 1][j] + z[q][i - 1][j]
                + z[q][i][j + 1] + z[q][i][j - 1]
                - 4.0 * z[q][i][j]);
          }
      }

   }
  }
  
  // finish the timer
  if (my_rank == 0) {
    cerr << "Elapsed time = " << time.lap( ) << endl;
  }
  cerr << "rank[" << my_rank << "]'s range = "<<  stripe_begin <<" ~ " << stripe_end << endl;
  MPI_Finalize(); 
  
  return 0;
}
