#include <time.h>
#include <string.h>
#include "../RedisModulesSDK/rmutil/vector.h"
#include "tsdb.h"
#include "rmutil/alloc.h"
#include <gsl/gsl_vector.h>
#include <gsl/gsl_statistics.h>


Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk)
{
    Series *newSeries = (Series *)malloc(sizeof(Series));
    newSeries->maxSamplesPerChunk = maxSamplesPerChunk;
    newSeries->firstChunk = NewChunk(newSeries->maxSamplesPerChunk);
    newSeries->lastChunk = newSeries->firstChunk;
    newSeries->chunkCount = 1;
    newSeries->lastTimestamp = 0;
    newSeries->retentionSecs = retentionSecs;
    newSeries->rules = NULL;

    return newSeries;
}

void SeriesTrim(Series * series) {
    if (series->retentionSecs == 0) {
        return;
    }
    Chunk *currentChunk = series->firstChunk;
    timestamp_t minTimestamp = time(NULL) - series->retentionSecs;
    while (currentChunk != NULL)
    {
        if (ChunkGetLastTimestamp(currentChunk) < minTimestamp)
        {
            Chunk *nextChunk = currentChunk->nextChunk;
            if (nextChunk != NULL) {
                series->firstChunk = nextChunk;    
            } else {
                series->firstChunk = NewChunk(series->maxSamplesPerChunk);
            }
            
            series->chunkCount--;
            FreeChunk(currentChunk);
            currentChunk = nextChunk;
        } else {
            break;
        }
    }
}

void FreeSeries(void *value) {
    Series *currentSeries = (Series *) value;
    Chunk *currentChunk = currentSeries->firstChunk;
    while (currentChunk != NULL)
    {
        Chunk *nextChunk = currentChunk->nextChunk;
        FreeChunk(currentChunk);
        currentChunk = nextChunk;
    }
}

size_t SeriesMemUsage(const void *value) {
    Series *series = (Series *)value;
    return sizeof(series) + sizeof(Chunk) * series->chunkCount;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    if (timestamp < series->lastTimestamp) {
        return TSDB_ERR_TIMESTAMP_TOO_OLD;
    } else if (timestamp == series->lastTimestamp) {
        // this is a hack, we want to override the last sample, so lets ignore it first
        series->lastChunk->num_samples--;
    }
    
    Chunk *currentChunk = series->lastChunk;
    Sample sample = {.timestamp = timestamp, .data = value};
    int ret = ChunkAddSample(currentChunk, sample);
    if (ret == 0 ) {
        // When a new chunk is created trim the series
        SeriesTrim(series);

        Chunk *newChunk = NewChunk(series->maxSamplesPerChunk);
        series->lastChunk->nextChunk = newChunk;
        series->lastChunk = newChunk;
        series->chunkCount++;        
        currentChunk = newChunk;
        // re-add the sample
        ChunkAddSample(currentChunk, sample);
    } 
    series->lastTimestamp = timestamp;

    return TSDB_OK;
}


// Danni: Perhaps add this info to series object ?
int SeriesSampleCount(Series *series) {
    SeriesItertor iter = SeriesQuery(series, 0, series->lastTimestamp);
    Sample sample;
    int count = 0;
    while (SeriesItertorGetNext(&iter, &sample) != 0) {
        count++;
    }

    return count;
}

Vector *SeriesGetSamples(Series *series) {
    Vector *v = NewVector(double, 0);
    SeriesItertor iter = SeriesQuery(series, 0, series->lastTimestamp);
    Sample sample;
    while (SeriesItertorGetNext(&iter, &sample) != 0) {
        Vector_Push(v, sample.data);
    }
    return v;
}

double PearsonCoeff(Series *series_1, Series *series_2) {

// Danni: what might cause a memory fault?
/*
    Vector *x = SeriesGetSamples(series_1);
    Vector *y = SeriesGetSamples(series_2);
*/

    SeriesItertor iter;
    Sample sample;

    Vector *x = NewVector(double, 0);
    iter = SeriesQuery(series_1, 0, series_1->lastTimestamp);
    while (SeriesItertorGetNext(&iter, &sample) != 0) {
        Vector_Push(x, sample.data);
    }

    Vector *y = NewVector(double, 0);
    iter = SeriesQuery(series_2, 0, series_2->lastTimestamp);
    while (SeriesItertorGetNext(&iter, &sample) != 0) {
        Vector_Push(y, sample.data);
    }

// Maital: This should yeild pcc == 1
/*
    Vector *x = NewVector(double, 5);
    Vector_Push(x, (double) 5.0);
    Vector_Push(x, (double) 5.0);
    Vector_Push(x, (double) 5.0);
    Vector_Push(x, (double) 5.0);
    Vector_Push(x, (double) 5.0);


    Vector *y = NewVector(double, 5);
    Vector_Push(y, (double) 5.0);
    Vector_Push(y, (double) 5.0);
    Vector_Push(y, (double) 5.0);
    Vector_Push(y, (double) 5.0);
    Vector_Push(y, (double) 5.0);
*/

// Using a regular vector - can't #include <vector>
/*  vector<double> x, y;
    size_t n = 5;
    x.push_back(1.0); y.push_back(1.0);
    x.push_back(3.1); y.push_back(3.2);
    x.push_back(2.0); y.push_back(1.9);
    x.push_back(5.0); y.push_back(4.9);
    x.push_back(2.0); y.push_back(2.1);
*/


    int m;
    double rc_1 = Vector_Get(x, 0, &m);
    gsl_vector_const_view gsl_x = gsl_vector_const_view_array( &rc_1, Vector_Size(x) );

    int n;
    double rc_2 = Vector_Get(y, 0, &n);
    gsl_vector_const_view gsl_y = gsl_vector_const_view_array( &rc_2, Vector_Size(y) );


    // Danni : what is the correct stride for RMUtils Vector ?
    const size_t stride = 1;
    double pcc = gsl_stats_correlation( (double*) gsl_x.vector.data, stride,
                                          (double*) gsl_y.vector.data, stride,
                                          5 );

//  double pcc = gsl_stats_correlation( (double*) gsl_x.vector.data, sizeof(double),
//                                          (double*) gsl_y.vector.data, sizeof(double),
//                                          5 );

    return pcc;
}

SeriesItertor SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp) {
    SeriesItertor iter;
    iter.series = series;
    iter.currentChunk = series->firstChunk;
    iter.chunkIteratorInitilized = FALSE;
    iter.currentSampleIndex = 0;
    iter.minTimestamp = minTimestamp;
    iter.maxTimestamp = maxTimestamp;
    return iter;
}

int SeriesItertorGetNext(SeriesItertor *iterator, Sample *currentSample) {
    Sample internalSample;
    while (iterator->currentChunk != NULL)
    {
        Chunk *currentChunk = iterator->currentChunk;
        if (ChunkGetLastTimestamp(currentChunk) < iterator->minTimestamp)
        {
            iterator->currentChunk = currentChunk->nextChunk;
            iterator->chunkIteratorInitilized = FALSE;
            continue;
        }
        else if (ChunkGetFirstTimestamp(currentChunk) > iterator->maxTimestamp)
        {
            break;
        }
        
        if (!iterator->chunkIteratorInitilized) 
        {
            iterator->chunkIterator = NewChunkIterator(iterator->currentChunk);
            iterator->chunkIteratorInitilized = TRUE;
        }

        if (ChunkItertorGetNext(&iterator->chunkIterator, &internalSample) == 0) { // reached the end of the chunk
            iterator->currentChunk = currentChunk->nextChunk;
            iterator->chunkIteratorInitilized = FALSE;
            continue;
        }

        if (internalSample.timestamp < iterator->minTimestamp) {
            continue;
        } else if (internalSample.timestamp > iterator->maxTimestamp) {
            break;
        } else {
            memcpy(currentSample, &internalSample, sizeof(Sample));
            return 1;
        }
    }
    return 0;
}

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec) {
    if (bucketSizeSec <= 0) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);;
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext();
    rule->bucketSizeSec = bucketSizeSec;
    rule->destKey = destKey;

    rule->nextRule = NULL;

    return rule;
}