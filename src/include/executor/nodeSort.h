/*-------------------------------------------------------------------------
 *
 * nodeSort.h
 *
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESORT_H
#define NODESORT_H

#include "access/parallel.h"
#include "executor/execProgram.h"
#include "nodes/execnodes.h"

extern SortState *ExecInitSort(Sort *node, EState *estate, int eflags);
extern void ExecEndSort(SortState *node);
extern void ExecSortMarkPos(SortState *node);
extern void ExecSortRestrPos(SortState *node);
extern void ExecReScanSort(SortState *node);

/* parallel instrumentation support */
extern void ExecSortEstimate(SortState *node, ParallelContext *pcxt);
extern void ExecSortInitializeDSM(SortState *node, ParallelContext *pcxt);
extern void ExecSortInitializeWorker(SortState *node, ParallelWorkerContext *pwcxt);
extern void ExecSortRetrieveInstrumentation(SortState *node);

extern void ExecProgramBuildForSort(ExecProgramBuild *b, PlanState *node, int eflags, int jumpfail, EmitForPlanNodeData *d);

#endif							/* NODESORT_H */
