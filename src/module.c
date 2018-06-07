#include <time.h>
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"

#include "tsdb.h"
#include "compaction.h"
#include "rdb.h"
#include "config.h"
#include "module.h"

RedisModuleType *SeriesType;
time_t timer;

int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    Series *series;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, 5*2);

    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "retentionSecs");
    RedisModule_ReplyWithLongLong(ctx, series->retentionSecs);
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, series->chunkCount);
    RedisModule_ReplyWithSimpleString(ctx, "maxSamplesPerChunk");
    RedisModule_ReplyWithLongLong(ctx, series->maxSamplesPerChunk);

    RedisModule_ReplyWithSimpleString(ctx, "rules");
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    CompactionRule *rule = series->rules;
    int ruleCount = 0;
    while (rule != NULL) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx, rule->destKey);
        RedisModule_ReplyWithLongLong(ctx, rule->bucketSizeSec);
        RedisModule_ReplyWithSimpleString(ctx, AggTypeEnumToString(rule->aggType));
        
        rule = rule->nextRule;
        ruleCount++;
    }
    RedisModule_ReplySetArrayLength(ctx, ruleCount);

    return REDISMODULE_OK;
}

void ReplyWithAggValue(RedisModuleCtx *ctx, timestamp_t last_agg_timestamp, AggregationClass *aggObject, void *context) {
    RedisModule_ReplyWithArray(ctx, 2);

    RedisModule_ReplyWithLongLong(ctx, last_agg_timestamp);
    RedisModule_ReplyWithDouble(ctx, aggObject->finalize(context));

    aggObject->resetContext(context);
}

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    long long start_ts, end_ts;
    long long time_delta = 0;
    RedisModuleString * aggTypeStr = NULL;

    int pRes = REDISMODULE_ERR;
    switch (argc) {
        case 4:
            pRes = RMUtil_ParseArgs(argv, argc, 2, "ll", &start_ts, &end_ts);
            break;
        case 6:
            pRes = RMUtil_ParseArgs(argv, argc, 2, "llsl", &start_ts, &end_ts, &aggTypeStr, &time_delta );
            if (!time_delta)
                return RedisModule_ReplyWithError(ctx, "TSDB: time-delta must != 0");
            break;
        default:
            return RedisModule_WrongArity(ctx);
    }
    if (pRes != REDISMODULE_OK)
        return RedisModule_WrongArity(ctx);

    long long agg_type = 0;
    Series *series;
    RedisModuleKey *key;
    AggregationClass *aggObject = NULL;

    if (argc > 4)
    {
        if (!aggTypeStr){
            return RedisModule_ReplyWithError(ctx, "TSDB: Unknown aggregation type");
        }

        agg_type = RMStringLenAggTypeToEnum(aggTypeStr);

        if (agg_type < 0 || agg_type >= TS_AGG_TYPES_MAX)
            return RedisModule_ReplyWithError(ctx, "TSDB: Unknown aggregation type");

        aggObject = GetAggClass( agg_type );
        if (!aggObject)
            return RedisModule_ReplyWithError(ctx, "TSDB: Failed to retrieve aggObject");
    }

    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
    SeriesIterator iterator = SeriesQuery(series, start_ts, end_ts);
    Sample sample;
    void *context;
    if (agg_type != AGG_NONE)
        context = aggObject->createContext();
    timestamp_t last_agg_timestamp = 0;
    while (SeriesIteratorGetNext(&iterator, &sample) != 0 ) {
        if (agg_type == AGG_NONE) { // No aggregation whats so ever
            RedisModule_ReplyWithArray(ctx, 2);

            RedisModule_ReplyWithLongLong(ctx, sample.timestamp);
            RedisModule_ReplyWithDouble(ctx, sample.data);
            arraylen++;
        } else {
            timestamp_t current_timestamp = sample.timestamp - (sample.timestamp % time_delta);
            if (current_timestamp > last_agg_timestamp) {
                if (last_agg_timestamp != 0) {
                    ReplyWithAggValue(ctx, last_agg_timestamp, aggObject, context);
                    arraylen++;
                }

                last_agg_timestamp = current_timestamp;
            }
            aggObject->appendValue(context, sample.data);
        }
    }

    if (agg_type != AGG_NONE) {
        // reply last bucket of data
        ReplyWithAggValue(ctx, last_agg_timestamp, aggObject, context);
        arraylen++;
    }

    RedisModule_ReplySetArrayLength(ctx,arraylen);
    return REDISMODULE_OK;
}

void handleCompaction(RedisModuleCtx *ctx, CompactionRule *rule, int doReplace, api_timestamp_t timestamp, double value) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, rule->destKey, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        // key doesn't exist anymore and we don't do anything
        return;
    }
    Series *destSeries = RedisModule_ModuleTypeGetValue(key);

    timestamp_t currentTimestamp = timestamp - timestamp % rule->bucketSizeSec;
    if (currentTimestamp > destSeries->lastTimestamp) {
        rule->aggClass->resetContext(rule->aggContext);
    }
    if (doReplace) {
        rule->aggClass->replaceValue(rule->aggContext, value);
    } else {
        rule->aggClass->appendValue(rule->aggContext, value);
    }
    SeriesAddSample(destSeries, currentTimestamp, rule->aggClass->finalize(rule->aggContext));
}

int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleString *keyName = argv[1];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);

    double timestamp, value;
    if ((RedisModule_StringToDouble(argv[3], &value) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx,"TSDB: invalid value");
    
    if ((RedisModule_StringToDouble(argv[2], &timestamp) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx,"TSDB: invalid timestamp");

    Series *series = NULL;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        if (TSGlobalConfig.hasGlobalConfig) {
            // the key doesn't exist but we have enough information to create one
            CreateTsKey(ctx, keyName, TSGlobalConfig.retentionPolicy, TSGlobalConfig.maxSamplesPerChunk, &series, &key);
            SeriesCreateRulesFromGlobalConfig(ctx, keyName, series);
        } else {
            return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exist");
        }
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, "TSDB: the key is not a TSDB key");
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    int doReplace = timestamp == series->lastTimestamp;
    int retval = SeriesAddSample(series, timestamp, value);
    int result = 0;
    if (retval == TSDB_ERR_TIMESTAMP_TOO_OLD) {
        RedisModule_ReplyWithError(ctx, "TSDB: timestamp is too old");
        result = REDISMODULE_ERR;
    } else if (retval != TSDB_OK) {
        RedisModule_ReplyWithError(ctx, "TSDB: Unknown Error");
        result = REDISMODULE_ERR;
    } else {
        // handle compaction rules
        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            handleCompaction(ctx, rule, doReplace, timestamp, value);
            rule = rule->nextRule;
        }
        
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_ReplicateVerbatim(ctx);
        result = REDISMODULE_OK;
    }
    RedisModule_CloseKey(key);
    return result;
}

int CreateTsKey(RedisModuleCtx *ctx, RedisModuleString *keyName, long long retentionSecs,
                long long maxSamplesPerChunk, Series **series, RedisModuleKey **key) {
    if (*key == NULL) {
        *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);
    }

    *series = NewSeries(retentionSecs, maxSamplesPerChunk);
    if (RedisModule_ModuleTypeSetValue(*key, SeriesType, *series) == REDISMODULE_ERR) {
        return TSDB_ERROR;
    }

    return TSDB_OK;
}

int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2 || argc > 4)
        return RedisModule_WrongArity(ctx);

    RedisModuleString *keyName = argv[1];
    long long retentionSecs = RETENTION_DEFAULT_SECS;
    long long maxSamplesPerChunk = TSGlobalConfig.maxSamplesPerChunk;

    if (argc > 2) {
        if ((RedisModule_StringToLongLong(argv[2], &retentionSecs) != REDISMODULE_OK))
            return RedisModule_ReplyWithError(ctx,"TSDB: invalid retentionSecs");
    }

    if (argc > 3) {
        if ((RedisModule_StringToLongLong(argv[3], &maxSamplesPerChunk) != REDISMODULE_OK))
            return RedisModule_ReplyWithError(ctx,"TSDB: invalid maxSamplesPerChunk");
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,"TSDB: key already exists");
    }

    Series *series;
    CreateTsKey(ctx, keyName, retentionSecs, maxSamplesPerChunk, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "info", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

//TS.DELETERULE SOURCE_KEY DEST_KEY
int TSDB_deleteRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    Series *series = RedisModule_ModuleTypeGetValue(key);

    RedisModuleString *destKey = argv[2];
    if (SeriesHasRule(series, destKey)) {
        CompactionRule *rule = series->rules;
        CompactionRule *prev_rule = NULL;
        while (rule != NULL) {
            if (RMUtil_StringEquals(rule->destKey, destKey)) {
                if (prev_rule == NULL) {
                    series->rules = rule->nextRule;
                } else {
                    prev_rule->nextRule = rule->nextRule;
                }
            }

            prev_rule = rule;
            rule = rule->nextRule;
        }
    } else {
        return RedisModule_ReplyWithError(ctx, "TSDB: compaction rule does not exist");
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/*
TS.CREATERULE src_key AGG_TYPE BUCKET_SIZE DEST_KEY
*/
int TSDB_createRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exist");
    }

    int aggType = RMStringLenAggTypeToEnum(argv[2]);
    if (aggType < 0 && aggType >5) { \
        return RedisModule_ReplyWithError(ctx, "TSDB: Unknown aggregation type"); \
    }
    
    long long bucketSize;
    RedisModule_StringToLongLong(argv[3], &bucketSize);
    if (bucketSize <= 0) {
        return RedisModule_ReplyWithError(ctx, "TSDB: bucketSize must be greater than zero");
    }
    RedisModuleKey *destKey = RedisModule_OpenKey(ctx, argv[4], REDISMODULE_READ);
    if (RedisModule_KeyType(destKey) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the destination key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    Series *series = RedisModule_ModuleTypeGetValue(key);
    if (SeriesHasRule(series, argv[4])) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the destination key already has a rule");
    }

    RedisModuleString *destKeyStr = RedisModule_CreateStringFromString(ctx, argv[4]);
    if (SeriesAddRule(series, destKeyStr, aggType, bucketSize) != NULL) {
        RedisModule_RetainString(ctx, destKeyStr);
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "ERROR creating rule");
        return REDISMODULE_ERR;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


/*
TS.INCRBY ts_key NUMBER [RESET] [RESET TIME SECONDS]
*/
int TSDB_incrby(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 || argc > 5)
        return RedisModule_WrongArity(ctx);

    RedisModuleString *keyName = argv[1];
    Series *series;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        if (TSGlobalConfig.hasGlobalConfig) {
            // the key doesn't exist but we have enough information to create one
            CreateTsKey(ctx, keyName, TSGlobalConfig.retentionPolicy, TSGlobalConfig.maxSamplesPerChunk, &series, &key);
            SeriesCreateRulesFromGlobalConfig(ctx, keyName, series);
        } else {
            return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exists");
        }
    }

    series = RedisModule_ModuleTypeGetValue(key);
    long long incrby = 0;
    if (RMUtil_ParseArgs(argv, argc, 2, "l", &incrby) != REDISMODULE_OK)
        return RedisModule_WrongArity(ctx);
    time(&timer);

    double result;
    long long resetSeconds = 1;
    time_t currentUpdatedTime = timer;
    if (argc > 3) {
        RMUtil_StringToLower(argv[3]);
        if (RMUtil_StringEqualsC(argv[3], "reset")) {
            if (argc > 4) {
                RMUtil_StringToLower(argv[4]);
                if (RMUtil_ParseArgs(argv, argc, 4, "l", &resetSeconds) != REDISMODULE_OK) {
                    return RedisModule_WrongArity(ctx);
                }
            }
            currentUpdatedTime = timer - ((int)timer % resetSeconds);
            if (series->lastTimestamp != 0) {
                int lastTS = series->lastTimestamp;
                if (lastTS - (lastTS % resetSeconds) !=  currentUpdatedTime) {
                    series->lastValue = 0;
                }
            }
        } else {
            return RedisModule_WrongArity(ctx);
        }
    }

    RMUtil_StringToLower(argv[0]);
    if (RMUtil_StringEqualsC(argv[0], "ts.incrby")) {
        result = series->lastValue + incrby;
    } else {
        result = series->lastValue - incrby;
    }

    SeriesAddSample(series, currentUpdatedTime, result);

    // handle compaction rules
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        handleCompaction(ctx, rule, TRUE, currentUpdatedTime, result);
        rule = rule->nextRule;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/*
module loading function, possible arguments:
COMPACTION_POLICY - compaction policy from parse_policies,h
RETENTION_POLICY - integer that represents the retention in seconds
MAX_SAMPLE_PER_CHUNK - how many samples per chunk
example:
redis-server --loadmodule ./redis-tsdb-module.so COMPACTION_POLICY "max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d" RETENTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "tsdb", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (ReadConfig(argv, argc) == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleTypeMethods tm = {
            .version = REDISMODULE_TYPE_METHOD_VERSION,
            .rdb_load = series_rdb_load,
            .rdb_save = series_rdb_save,
            .aof_rewrite = RMUtil_DefaultAofRewrite,
            .mem_usage = SeriesMemUsage,
            .free = FreeSeries
        };

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", TS_ENC_VER, &tm);
    if (SeriesType == NULL) return REDISMODULE_ERR;
    RMUtil_RegisterWriteCmd(ctx, "ts.create", TSDB_create);
    RMUtil_RegisterWriteCmd(ctx, "ts.createrule", TSDB_createRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.deleterule", TSDB_deleteRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.add", TSDB_add);
    RMUtil_RegisterWriteCmd(ctx, "ts.incrby", TSDB_incrby);
    RMUtil_RegisterWriteCmd(ctx, "ts.decrby", TSDB_incrby);
    RMUtil_RegisterReadCmd(ctx, "ts.range", TSDB_range);
    RMUtil_RegisterReadCmd(ctx, "ts.info", TSDB_info);

    return REDISMODULE_OK;
}
