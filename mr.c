#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

typedef struct mainarg{
    mapper_t mapper;
    kvlist_t *output;
    kvlist_t *array;
}mainarg;

void *threader(void *arg){
    
    int i = 0;
    struct mainarg *dsi = arg;
    mapper_t mapper = dsi->mapper;
    kvlist_iterator_t *iter = kvlist_iterator_new(dsi->array);
    kvpair_t *pair = kvlist_iterator_next(iter);
    kvlist_print(1, dsi->array);
    printf("before the loop %d \n", i);
    for(;;){ 
        printf("in the loop %d \n", i);
        if(pair == NULL){
            break;
        }
        mapper(kvpair_clone(pair), dsi->output);
        printf("before mapper %d \n", i);
        pair = kvlist_iterator_next(iter);
        
        printf("after mapper %d \n", i);
        //kvlist_print(1, dsi->output);
        
        i++;
        
    }
    // kvlist_print(1, dsi->output);
    return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
                 /*   
                    pthread_t th[num_mapper];
                    for(int i = 0; i < num_mapper; i++{
                        if
                    })
                    */
                // kvlist_print(1, input);
                size_t len = 0;
                kvlist_iterator_t *iter = kvlist_iterator_new(input);
                kvpair_t *pair = kvlist_iterator_next(iter);
                while(len >= 0){
                    pair = kvlist_iterator_next(iter);
                    len++;
                    if(pair == NULL){
                        break;
                    }
                }
                size_t rem = len%num_mapper;
                size_t threadcount = len/num_mapper;
                kvlist_t **array = (kvlist_t **)malloc(num_mapper * sizeof(kvlist_t *));
                for(size_t i = 0; i < num_mapper; i++){
                    array[i]= kvlist_new();
                    
                }
                kvlist_iterator_t *it = kvlist_iterator_new(input);
                for(size_t i = 0; i < threadcount; i++){
                    for(size_t j = 0; j< num_mapper; j++){
                        pair = kvlist_iterator_next(it);
                        if(pair == NULL){
                            break;
                        }
                        kvlist_append(array[j],kvpair_clone(pair));
        
                   }
                }
                for(size_t i = 0; i < rem; i++){
                    pair = kvlist_iterator_next(it);
                        if(pair == NULL){
                            break;
                        }
                        kvlist_append(array[i],kvpair_clone(pair));
                }
                // kvlist_print(1, array[0]);
                // kvlist_print(1, array[1]);
                // kvlist_print(1, array[2]);
                // printf("second thread");
                // printf("1");
                // printf("1");
                pthread_t threads[num_mapper];
                struct mainarg arby;
                arby.mapper = mapper;
                kvlist_t **maparr = (kvlist_t **)malloc(num_mapper * sizeof(kvlist_t *));
                for(size_t i = 0; i < num_mapper; i++){
                    arby.array = array[i];
                    arby.output = maparr[i];
                    pthread_create(&threads[i], NULL, threader,(void *) &arby);
                    pthread_join(threads[i], NULL);
                }
                // for(size_t i = 0; i < num_mapper ;i++){
                //     pthread_join(threads[i], NULL);
                // }

                // for(size_t i = 0; i < num_mapper ;i++){
                //     kvlist_print(1, maparr[i]);
                // }
            }
