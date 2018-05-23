/*-------------------------------------------------------------------------
 *
 * nodeNestloop.h
 *
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeNestloop.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODENESTLOOP_H
#define NODENESTLOOP_H

#include "nodes/execnodes.h"
#include "executor/execProgram.h"

extern NestLoopState *ExecInitNestLoop(NestLoop *node, EState *estate, int eflags);
extern void ExecEndNestLoop(NestLoopState *node);
extern void ExecReScanNestLoop(NestLoopState *node);

extern void ExecProgramBuildForNestloop(ExecProgramBuild *b, PlanState *node, int eflags, int jumpfail, EmitForPlanNodeData *d);

#endif							/* NODENESTLOOP_H */
