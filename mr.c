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
    
    struct mainarg *dsi = arg;
    mapper_t mapper = dsi->mapper;
    kvlist_iterator_t *iter = kvlist_iterator_new(dsi->array);
    kvpair_t *pair = kvlist_iterator_next(iter);
    for(;;){
        pair = kvlist_iterator_next(iter);
        if(pair == NULL){
            break;
        }
        mapper(pair, dsi->output);
    }
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
                        kvlist_append(array[j],pair);
        
                   }
                }
                printf("1");
                for(size_t i = 0; i < rem; i++){
                    pair = kvlist_iterator_next(it);
                        if(pair == NULL){
                            break;
                        }
                        kvlist_append(array[i],pair);
                }
                printf("1");
                printf("first thread");
                kvlist_print(1, array[0]);
                kvlist_print(1, array[1]);
                kvlist_print(1, array[2]);
                printf("second thread");
                // printf("1");
                // pthread_t threads[num_mapper];
                // struct mainarg arby;
                // arby.mapper = mapper;
                // printf("2");
                // kvlist_t **maparr = (kvlist_t **)malloc(num_mapper * sizeof(kvlist_t *));
                // printf("3");

                // for(size_t i = 0; i < num_mapper; i++){
                // printf("4");

                //     arby.array = array[i];
                //     arby.output = maparr[i];
                //     printf("5");

                //     pthread_create(&threads[i], NULL, threader,(void *) &arby);
                //     printf("%zu", i);
                // }
                // kvlist_print(1,maparr[0]);
            }
