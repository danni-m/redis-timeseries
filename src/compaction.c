#include <ctype.h>
#include <string.h>
#include "compaction.h"
#include "rmutil/alloc.h"

typedef struct AvgContext {
    double val;
    double cnt;
    struct AvgContext *last;
} AvgContext;

typedef struct MaxMinContext {
    double value;
    char isResetted;
    struct MaxMinContext *last;
} MaxMinContext;

void *AvgCreateContext() {
    AvgContext *last = (AvgContext*)malloc(sizeof(AvgContext));
    last->val = 0;
    last->cnt = 0;
    last->last = last;
    
    AvgContext *context = (AvgContext*)malloc(sizeof(AvgContext));
    memcpy(context, last, sizeof(*context));
    
    return context;
}

void AvgFree(void *contextPtr) {
    AvgContext *context = (AvgContext *)contextPtr;
    free(context->last);
    free(contextPtr);
}

void AvgAppendValue(void *contextPtr, double value){
    AvgContext *context = (AvgContext *)contextPtr;
    memcpy(context->last, context, sizeof(*context));
    
    context->val += value;
    context->cnt++;
}

void AvgReplaceValue(void *contextPtr, double value){
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = context->last->val + value;
}

void AvgReset(void *contextPtr) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = 0;
    context->cnt = 0;
    memcpy(context->last, context, sizeof(*context));
}

double AvgFinalize(void *contextPtr) {
    AvgContext *context = (AvgContext *)contextPtr;
    return context->val / context->cnt;
}

static AggregationClass aggAvg = {
    .createContext = AvgCreateContext,
    .freeContext = AvgFree,
    .appendValue = AvgAppendValue,
    .replaceValue = AvgReplaceValue,
    .resetContext = AvgReset,
    .finalize = AvgFinalize,
};

void *MaxMinCreateContext() {
    MaxMinContext *last = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    last->value = 0;
    last->isResetted = TRUE;
    last->last = last;
    
    MaxMinContext *context = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    memcpy(context, last, sizeof(*context));
    
    return context;
}

void MaxMinFree(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    free(context->last);
    free(contextPtr);
}

void MaxAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    memcpy(context->last, context, sizeof(*context));
    
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    } else if (value > context->value) {
        context->value = value;
    }
}

void MaxReplaceValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    
    if (context->last->isResetted || value > context->last->value) {
        context->value = value;
    } else {
        context->value = context->last->value;
    }
}

void MaxMinReset(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value = 0;
    context->isResetted = TRUE;
    memcpy(context->last, context, sizeof(*context));
}

double MaxMinFinalize(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    return context->value;
}

void MinAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    memcpy(context->last, context, sizeof(*context));
    
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    } else if (value < context->value) {
        context->value = value;
    }
}

void MinReplaceValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    
    if (context->last->isResetted || value < context->last->value) {
        context->value = value;
    } else {
        context->value = context->last->value;
    }
}

void SumAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    memcpy(context->last, context, sizeof(*context));
    
    context->value += value;
}

void SumReplaceValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    
    context->value = context->last->value + value;
}

void CountAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    memcpy(context->last, context, sizeof(*context));
    
    context->value++;
}

void CountReplaceValue(void *contextPtr, double value) {
    // NOP: We've already counted this timestamp.
}

void FirstAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    memcpy(context->last, context, sizeof(*context));
    
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    }
}

void FirstReplaceValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->last->isResetted) {
        context->value = value;
    }
}

void LastAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    
    context->value = value;
}

static AggregationClass aggMax = {
    .createContext = MaxMinCreateContext,
    .freeContext = MaxMinFree,
    .appendValue = MaxAppendValue,
    .replaceValue = MaxReplaceValue,
    .resetContext = MaxMinReset,
    .finalize = MaxMinFinalize
};

static AggregationClass aggMin = {
    .createContext = MaxMinCreateContext,
    .freeContext = MaxMinFree,
    .appendValue = MinAppendValue,
    .replaceValue = MinReplaceValue,
    .resetContext = MaxMinReset,
    .finalize = MaxMinFinalize
};

static AggregationClass aggSum = {
    .createContext = MaxMinCreateContext,
    .freeContext = MaxMinFree,
    .appendValue = SumAppendValue,
    .replaceValue = SumReplaceValue,
    .resetContext = MaxMinReset,
    .finalize = MaxMinFinalize
};

static AggregationClass aggCount = {
    .createContext = MaxMinCreateContext,
    .freeContext = MaxMinFree,
    .appendValue = CountAppendValue,
    .replaceValue = CountReplaceValue,
    .resetContext = MaxMinReset,
    .finalize = MaxMinFinalize
};

static AggregationClass aggFirst = {
    .createContext = MaxMinCreateContext,
    .freeContext = MaxMinFree,
    .appendValue = FirstAppendValue,
    .replaceValue = FirstReplaceValue,
    .resetContext = MaxMinReset,
    .finalize = MaxMinFinalize
};

static AggregationClass aggLast = {
    .createContext = MaxMinCreateContext,
    .freeContext = MaxMinFree,
    .appendValue = LastAppendValue,
    .replaceValue = LastAppendValue,
    .resetContext = MaxMinReset,
    .finalize = MaxMinFinalize
};

int StringAggTypeToEnum(const char *agg_type) {
    return StringLenAggTypeToEnum(agg_type, strlen(agg_type));
}

int RMStringLenAggTypeToEnum(RedisModuleString *aggTypeStr) {
    size_t str_len;
    const char *aggTypeCStr = RedisModule_StringPtrLen(aggTypeStr, &str_len);
    return StringLenAggTypeToEnum(aggTypeCStr, str_len);
}

int StringLenAggTypeToEnum(const char *agg_type, size_t len) {
    char agg_type_lower[10];
    int result;

    for(int i = 0; i < len; i++){
        agg_type_lower[i] = tolower(agg_type[i]);
    }
    if (strncmp(agg_type_lower, "min", len) == 0){
        result = TS_AGG_MIN;
    } else if (strncmp(agg_type_lower, "max", len) == 0) {
        result =  TS_AGG_MAX;
    } else if (strncmp(agg_type_lower, "sum", len) == 0) {
        result =  TS_AGG_SUM;
    } else if (strncmp(agg_type_lower, "avg", len) == 0) {
        result =  TS_AGG_AVG;
    } else if (strncmp(agg_type_lower, "count", len) == 0) {
        result =  TS_AGG_COUNT;
    } else if (strncmp(agg_type_lower, "first", len) == 0) {
        result =  TS_AGG_FIRST;
    } else if (strncmp(agg_type_lower, "last", len) == 0) {
        result =  TS_AGG_LAST;
    } else {
        result =  TS_AGG_INVALID;
    }

    return result;
}

const char * AggTypeEnumToString(int aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return "MIN";
        case TS_AGG_MAX:
            return "MAX";
        case TS_AGG_SUM:
            return "SUM";
        case TS_AGG_AVG:
            return "AVG";
        case TS_AGG_COUNT:
            return "COUNT";
        case TS_AGG_FIRST:
            return "FIRST";
        case TS_AGG_LAST:
            return "LAST";
        default:
            return "Unknown";
    }
}

AggregationClass* GetAggClass(int aggType) {
    switch (aggType) {
        case AGG_NONE:
            return NULL;
            break;
        case AGG_MIN:
            return &aggMin;
            break;
        case AGG_MAX:
            return &aggMax;
        case AGG_AVG:
            return &aggAvg;
            break;
        case AGG_SUM:
            return &aggSum;
            break;
        case AGG_COUNT:
            return &aggCount;
            break;
        case AGG_FIRST:
            return &aggFirst;
            break;
        case AGG_LAST:
            return &aggLast;
            break;
        default:
            return NULL;
    }
}
