/*-------------------------------------------------------------------------
 *
 * nodeLimit.h
 *
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeLimit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODELIMIT_H
#define NODELIMIT_H

#include "executor/execProgram.h"
#include "nodes/execnodes.h"

extern LimitState *ExecInitLimit(Limit *node, EState *estate, int eflags);
extern void ExecEndLimit(LimitState *node);
extern void ExecReScanLimit(LimitState *node);

extern void ExecComputeLimit(LimitState *node);
extern bool ExecCheckLimit(LimitState *node);
extern bool ExecUpdateLimit(LimitState *node);

extern void ExecProgramBuildForLimit(ExecProgramBuild *b, PlanState *node, int eflags, int jumpfail, EmitForPlanNodeData *d);

#endif							/* NODELIMIT_H */
