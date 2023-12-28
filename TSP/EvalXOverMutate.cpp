#include <iostream>  // cout
#include <stdlib.h>  // rand
#include <math.h>    // sqrt, pow
#include <omp.h>     // OpenMP
#include <string.h>  // memset
#include "Timer.h"
#include "Trip.h"

#define CHROMOSOMES    50000 // 50000 different trips
#define CITIES         36    // 36 cities = ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789
#define TOP_X          25000 // top optimal 25%
#define MUTATE_RATE    14   // optimal 14%
#define nTHREAD         4   // Thread to be generated
#define SEED           12356 //Seed value for rand
using namespace std;

/* Additional Helper Functions*/

/* 
* This is a custom comparator for qSort
*/
int  compare(const void * p, const void * q)
{
    return (((Trip *)p)->fitness > ((Trip *)q)->fitness ) ? 1 : -1;
}

/*
* Representing all charater as 0 - 35
* Useful for accessing cordinates array and finding complement
*/
int getPos(char &ch) {
    /*
    * The Ascii values are leverage to represent all character bu numeric value 0 - 35
    * ch - 'A' represents [A-Z] as [0-25]
    * ch - '0' represent [0 - 9] as [0 - 9] but by adding 26 to it we have acounted
     for [A-Z]
    */
    return (ch >= 'A' && ch <= 'Z') ? ch -'A' : 26 + ch - '0';
}


/*
* calculate distance between two cities
*/
float calDistance(int srcCordinates[2], int dstCordinates[2]) {
    int xSrc =  srcCordinates[0];
    int ySrc =  srcCordinates[1];
    int xDst =  dstCordinates[0];
    int yDst =  dstCordinates[1];

    return sqrt(pow(yDst - ySrc, 2) + pow(xDst - xSrc, 2));
}


/*
 * Evaluates each trip (or chromosome) and sort them out
 */
void evaluate( Trip trip[CHROMOSOMES], int coordinates[CITIES][2] ) {
    
    // The outer loop is parallelized : Coarse grain parallelism strategy
    // Distributing 50K trips between nTHREAD threads for concurent execution
    // omp_set_num_threads explicitly defines the number of thread to be generated
    omp_set_num_threads( nTHREAD );
    #pragma omp parallel for
    
    for( int i = 0; i < CHROMOSOMES; i++ ) {
        // Initialize fitness as 0 in the begining of the trip
        trip[i].fitness = 0.0;
        
        // Initiazed srcCityC to {0,0} as trip starts from origin cordinates
        int srcCityC[2] = {0};
        int desCityC[2] = {0};
        
        // Calculating the distance/fitness of the trip
        for ( int j = 0; j < CITIES; j++ ) {
            if(j > 0){
                srcCityC[0] = coordinates[getPos(trip[i].itinerary[j - 1])][0];
                srcCityC[1] = coordinates[getPos(trip[i].itinerary[j - 1])][1];
            }
            desCityC[0] = coordinates[getPos(trip[i].itinerary[j])][0];
            desCityC[1] = coordinates[getPos(trip[i].itinerary[j])][1];
            trip[i].fitness += calDistance(srcCityC, desCityC);
        }
            
    }
    //Sorting in decending order of distance/fitness
    qsort(trip , CHROMOSOMES, sizeof(trip[CHROMOSOMES]), compare);
}

/*
 * Generates new TOP_X offsprings from TOP_X parents.
 * Noe that the i-th and (i+1)-th offsprings are created from the i-th and (i+1)-th parents
 */

void crossover( Trip parents[TOP_X], Trip offsprings[TOP_X], int coordinates[CITIES][2] ) {
    string city_list = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    
    // The outer loop is parallelized : Coarse grain parallelism strategy
    // Distributing 25K parents between nTHREAD threads for concurent execution
    // omp_set_num_threads explicitly defines the number of thread to be generated
    omp_set_num_threads( nTHREAD );
    #pragma omp parallel for
    
    for ( int i = 0; i < TOP_X - 1; i = i + 2 ) {
        string parent1 = string(parents[i].itinerary);
        string parent2 = string(parents[i+1].itinerary);
        int visited[CITIES] = {0};
        
        // For set of crossover we start by picking parent1[0]
        char src = parent1[0];
        
        for(int j = 0; j < CITIES; j++) {
            int pos_src = getPos(src);
            
            // Routing is handled in this step
            offsprings[i].itinerary[j] = src;

            // Routing of complement child. Complement of a[i] is a[CITIES - 1 - i]
            offsprings[i + 1].itinerary[j] = city_list[CITIES - 1 - pos_src];
            
            // Mark visited cities
            visited[pos_src] = 1;
            if(j == CITIES - 1)break;
            
            // Find the next city leaving from src
            int idxSrcP1 = parent1.find(src);
            int idxSrcP2 = parent2.find(src);
            int idxNxtP1 = (idxSrcP1 == CITIES - 1) ? 0 : idxSrcP1 + 1;
            int idxNxtP2 = (idxSrcP2 == CITIES - 1) ? 0 : idxSrcP2 + 1;
            int pos_dstP1 = getPos(parent1[idxNxtP1]);
            int pos_dstP2 = getPos(parent2[idxNxtP2]); 

            // If the destination cities from Parent 1 and Parent 2 is not visited
            // Route with the minimum length is selected
            if ( !visited[pos_dstP1] && !visited[pos_dstP2] ) {
                float d1 = calDistance(coordinates[pos_dstP1], coordinates[pos_src]);
                float d2 = calDistance(coordinates[pos_dstP2], coordinates[pos_src]);
                
                if ( d1 <= d2) {
                    src = city_list[pos_dstP1];
                } 
                else {
                    src = city_list[pos_dstP2];
                }
                
            }
            // if the destination cities from Parent 1 and Parent 2 are both visited
            // Find the first non-visited city from Parent 1.
            else if ( visited[pos_dstP1] && visited[pos_dstP2] ) {
                int idx = pos_dstP1;
                while (visited[idx]) {
                    idx++;
                    if ( idx >= CITIES ) {
                        idx = 0;
                    }
                }
                src = city_list[idx];
            }
            // select the non-visited city if one of the two cities is visited ignoring distance
            else {
                if ( visited[pos_dstP1] ) {
                    src = city_list[pos_dstP2];
                }
                else{
                    src = city_list[pos_dstP1];
                }
            }             
        }
    }
    
} 

/*
 * Mutate a pair of genes in each offspring.
 */
void mutate( Trip offsprings[TOP_X] ) {
    int x_mCity = -1, y_mCity = -1;
    srand(SEED);
    for (int i = 0; i < TOP_X; i++) {
        
        // Since we want 10%   we want a number between 0 - 99
        if ( rand() % 100 < MUTATE_RATE ) {
            x_mCity = rand() % 36;
            y_mCity = rand() % 36;
            
            // swap the city at x_mCity and y_mCity position
            swap(offsprings[i].itinerary[x_mCity], offsprings[i].itinerary[y_mCity]);
        } 
    }
}
