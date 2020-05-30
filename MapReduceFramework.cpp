//
// Created by elam on 19/05/2020.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include "Barrier.h"
#include <atomic>
#include <map>
#include <vector>
#include <algorithm>
#include <iostream>

struct JobContext;

/**
 * ThreadContext is a struct which holds the data a thread needs and has an access to jobContext
 */
struct ThreadContext {

    int id;

    ThreadContext(int id, int shuffleKeys, JobContext *sharedJob) : id(id), shuffleKeys(shuffleKeys), sharedJob(sharedJob) {}
//    std::atomic<int> *atomicCounterReduce;
    //mutex
    pthread_mutex_t mapMut = PTHREAD_MUTEX_INITIALIZER;
    //how mach keys
    int shuffleKeys= 0;
    JobContext *sharedJob;
    std::vector<IntermediatePair> vecOfIntermediatePair;
};


/**
 * JobContext is a struct that holds shared information between all threads
 */
struct JobContext
{

    int numOfThread;
    bool finishMapState;
    stage_t  stage;
    Barrier  barrier;
    //shard atomic
    std::atomic<int> atomic_counter;
    std::atomic<int> percentageMap;
    std::atomic<int> countOfKey;
    std::atomic<int> countFinishMap;
    std::atomic<int> atomicCounterReduce;
    std::atomic<int> percentageReduce;
    std::atomic<int> threadPassMap;

    pthread_mutex_t reduceMut =PTHREAD_MUTEX_INITIALIZER ;
    //K1,V1
    const InputVec &inputVec;
    //the client
    const MapReduceClient &client;
    //our output K3,V3
    OutputVec &outputVec;
    pthread_t * threads;
    std::vector<ThreadContext*>contextArr;
    //suful
    IntermediateMap *intermediateMap;

    std::vector<K2*> K2vec;

    JobContext(int numOfThread, const InputVec &inputVec, const MapReduceClient &client, OutputVec &outputVec)
            : numOfThread(numOfThread), inputVec(inputVec), client(client), outputVec(outputVec)
            , barrier(Barrier(numOfThread)), atomic_counter(0), threadPassMap(0), percentageMap(0), countOfKey(0),
              countFinishMap(0), atomicCounterReduce(0),percentageReduce(0)
    {
        stage = MAP_STAGE;
        finishMapState = false;
        threads = new pthread_t [numOfThread];
        intermediateMap = new IntermediateMap ();
        for(int i =0 ; i < numOfThread; i++)
        {
            auto currContext  = new ThreadContext(i, 0, this);
            contextArr.push_back(currContext);
        }
    }
};

//*********************************
void myProgram(JobHandle job);

/**
 *  The emit2 function produces a (K2*,V2*) pair.
 * @param key - the key
 * @param value - the value
 * @param context - the context is for allowing emit 2 to receive information from the function that called map.
 */
void emit2 (K2* key, V2* value, void* context)
{
    auto * currContext = (ThreadContext*) context;
    if (pthread_mutex_lock(&currContext->mapMut) != 0){
        std::cerr << "system error: failed to lock mapMutex\n";
        exit(EXIT_FAILURE);
    }

    IntermediatePair pair(key,value);
    //insert pairs
    //synchronizing Map and Shuffle
    currContext->vecOfIntermediatePair.push_back(pair);
    if (pthread_mutex_unlock(&currContext->mapMut) != 0){
        std::cerr << "system error: failed to unlock mapMutex\n";
        exit(EXIT_FAILURE);
    }
    (currContext->sharedJob->countOfKey)++;
}


/**
 * The emit3 function produces a (K3*,V3*) pair
 * @param key - the key
 * @param value - the value
 * @param context - the context is for allowing emit3 to receive information from the function that called reduce.
 */
void emit3 (K3* key, V3* value, void* context)
{
    auto * currContext = (ThreadContext*) context;
    if (pthread_mutex_lock(&currContext->sharedJob->reduceMut) != 0){
        std::cerr << "system error: failed to lock reduceMutex\n";
        exit(EXIT_FAILURE);
    }
    OutputPair outputPair(key,value);
    //insert pair
    currContext->sharedJob->outputVec.push_back(outputPair);
    if (pthread_mutex_unlock(&currContext->sharedJob->reduceMut) != 0){
        std::cerr << "system error: failed to unlock reduceMutex\n";
        exit(EXIT_FAILURE);
    }
}


/**
 * This function starts running the MapReduce algorithm (with
    several threads) and returns a JobHandle.
 * @param client - The implementation of ​ MapReduceClient class, in other words the task that the
    framework should run.
 * @param inputVec – a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
 * @param outputVec - a vector of type std::vector<std::pair<K3*, V3*>>, to which the output
                      elements will be added before returning. You can assume that outputVec is empty.
 * @param multiThreadLevel – the number of worker threads to be used for running the algorithm.
Y                           ou can assume this argument is valid (greater or equal to 2).
 * @return jobContext - a job context
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel)
{
    auto * jobContext = new JobContext(multiThreadLevel, inputVec, client, outputVec);

    for(int  i =0 ;i<multiThreadLevel;i++)
    {
        if (pthread_create(jobContext->threads+i, nullptr, reinterpret_cast<void *(*)(void *)>(myProgram),
                           jobContext->contextArr[i]) != 0){

            std::cerr << "system error: failed to do pthread_create\n";
            exit(EXIT_FAILURE);
        }
    }
    return jobContext;
}


/**
 * a function gets the stage handle returned by startMapReduceFramework and
    waits until it is finished.
 * @param job - a job
 */
void waitForJob(JobHandle job)
{
    auto * jobContext = (JobContext*) job;
    for(int i =0; i<jobContext->numOfThread;i++  )
    {
        if (pthread_join(jobContext->threads[i], nullptr) != 0){
            std::cerr << "system error: failed to do pthread_join\n";
            exit(EXIT_FAILURE);
        }
    }
}


/**
 * this function gets a stage handle and updates the state of the stage into the given JobState struct.
 * @param job - a job handler
 * @param state - the state
 */
void getJobState(JobHandle job, JobState* state)
{
    auto * jobContext = (JobContext*) job;
//    auto  *context = (ThreadContext*) job;
    state->stage = jobContext->stage;

    if(state->stage == MAP_STAGE)
    {
        state->percentage = (float) jobContext->percentageMap.load()/jobContext->inputVec.size()*100; // todo check this
    }
    if(state->stage == SHUFFLE_STAGE)
    {
        state->percentage = (float)jobContext->contextArr[jobContext->numOfThread-1]->shuffleKeys /
                            jobContext->countOfKey.load() * 100 ;
    }
    if(state->stage == REDUCE_STAGE)
    {
        state->percentage = (float)jobContext->percentageReduce.load() / jobContext->K2vec.size() * 100;
    }
}
/**
 * this function is responsible for releasing all resources of a job
 * @param job - a job
 */
void closeJobHandle(JobHandle job)
{
    auto jobContext = (JobContext*) job;
    waitForJob(job);

    delete [] jobContext->threads;
    for(int i =0; i < jobContext->numOfThread; i++  )
    {
        if (pthread_mutex_destroy( &jobContext->contextArr[i]->mapMut) != 0){
            std::cerr << "system error: failed to call pthread_mutex_destroy in closeJobHandle\n";
            exit(EXIT_FAILURE);
        }
        delete jobContext->contextArr[i];
    }
    if (pthread_mutex_destroy( &jobContext->reduceMut) != 0){
        std::cerr << "system error: failed to call pthread_mutex_destroy in closeJobHandle\n";
        exit(EXIT_FAILURE);
    }
    delete jobContext->intermediateMap;
    jobContext->contextArr.clear();
    delete jobContext;
    jobContext= nullptr;
}


/**
 * this function does the map operation
 * @param job - a job
 */
void map(JobHandle job)
{
    auto *context = (ThreadContext *) job;
    int currPlace = (context->sharedJob->atomic_counter)++;
    while (currPlace < context->sharedJob->inputVec.size() )
    {
        context->sharedJob->client.map((context->sharedJob->inputVec)[currPlace].first,
                                       (context->sharedJob->inputVec)[currPlace].second, context);
        context->sharedJob->percentageMap++;
        currPlace=(context->sharedJob->atomic_counter)++;
    }
    context->sharedJob->finishMapState = ++context->sharedJob->threadPassMap == context->sharedJob->numOfThread - 1;
}


/**
 * this function does the shuffle operation
 * @param context - this context is a thread context
 */
void shuffling(ThreadContext *context)
{
    for (int i = 0; i < context->sharedJob->numOfThread - 1; i++)
    {
        if (pthread_mutex_lock(&context->sharedJob->contextArr[i]->mapMut) !=0){
            std::cerr << "system error: failed to lock mutex in shuffle func\n";
            exit(EXIT_FAILURE);
        }
        while (!context->sharedJob->contextArr[i]->vecOfIntermediatePair.empty())
        {
            //synchronizing Map and Shuffle
            IntermediatePair pair = context->sharedJob->contextArr[i]->vecOfIntermediatePair.back();
            context->sharedJob->contextArr[i]->vecOfIntermediatePair.pop_back();
            //add 1 to number of keys that i make tham sufful
            context->shuffleKeys++;
            // if it is the first time i add this key , we add to K2vec
            if ((*context->sharedJob->intermediateMap).find(pair.first) == (*context->sharedJob->intermediateMap).end())
            {
                (context -> sharedJob->K2vec).push_back(pair.first);
            }
            (*context -> sharedJob->intermediateMap)[pair.first].push_back(pair.second);
        }
        if (pthread_mutex_unlock(&context->sharedJob->contextArr[i]->mapMut) != 0){
            std::cerr << "system error: failed to unlock mutex in shuffle func\n";
            exit(EXIT_FAILURE);
        }
    }
}


/**
 * this function calls the shuffling function for all the map threads , calls shuffling after finished the map stage
 * and does sort
 * @param job - a job
 */
void myShuffle(JobHandle job)
{
    auto *context = (ThreadContext *) job;
    while(!context->sharedJob->finishMapState)
    {
        //run for all the map threads
        shuffling(context);
    }
    //we finish map
    context -> sharedJob -> stage = SHUFFLE_STAGE;
    //do lost time shuffle and sort
    shuffling(context);
    std::sort((context->sharedJob->K2vec).begin(), (context->sharedJob->K2vec).end(), K2PointerComp());
    (context->sharedJob->stage) = REDUCE_STAGE;
}


/**
 * myProgram is the main function of the map - reduce algorithm
 * @param job - a job
 */
void myProgram(JobHandle job)
{
    auto *context = (ThreadContext *) job;
    if(context->id != context->sharedJob->numOfThread - 1)
    {
        map(job);
    }
    else
    {
        myShuffle(job);
    }
//    pass only if all the thread here
    context->sharedJob->barrier.barrier();
//finish all map ans shuffle and start reduce

// reduce
    int currPlace = context->sharedJob->atomicCounterReduce++;
    while(currPlace < context->sharedJob->K2vec.size())
    {
        auto key = (context->sharedJob->K2vec)[currPlace];
        context->sharedJob->client.reduce(key, context->sharedJob->intermediateMap->at(key), context);
        context->sharedJob->percentageReduce++;
        currPlace = (context->sharedJob->atomicCounterReduce)++;
    }
}

