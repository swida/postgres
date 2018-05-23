/*-------------------------------------------------------------------------
 *
 * execProgram.h
 *	  Build program to evaluate query
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execProgram.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXEC_PROGRAM_H
#define EXEC_PROGRAM_H

#include "nodes/execnodes.h"


typedef struct ExecProgram ExecProgram;
typedef struct ExecStep ExecStep;

typedef struct ExecRelocatableJump
{
	int varno; /* id to relocate*/
	int step;  /* step */
	size_t offset; /* offset in that step */
} ExecRelocatableJump;

typedef struct ExecRelocation
{
	int varno; /* id to relocate */
	int target; /* resolved relocation */
} ExecRelocation;

typedef struct ExecProgramBuild
{
	ExecProgram *program;

	EState *estate;

	ExecRelocatableJump *reloc_jumps;
	ExecRelocation *reloc;

	int steps_alloc;

	int slots_alloc;

	int reloc_jumps_alloc;
	int reloc_jumps_len;

	int varno;

	bool failed;
} ExecProgramBuild;

struct ExecProgram
{
	TupleTableSlot **slots;
	ExecStep *steps;

	PlanState *top;

	int cur_step;
	int steps_len;
	int slots_len;
};


typedef enum ExecEvalOp
{
	/* actual tuple scan sources */
	XO_SEQSCAN_FIRST,
	XO_SEQSCAN,

	XO_INDEX_SCAN_FIRST,
	XO_INDEX_SCAN,

	XO_INDEXONLY_SCAN_FIRST,
	XO_INDEXONLY_SCAN,

	/* scan like tuple sources */
	XO_DRAIN_HASHAGG,
	XO_DRAIN_SORTAGG,
	XO_DRAIN_SORT,

	XO_INIT_HASH,

	/* */
	XO_INIT_SORT,
	XO_SORT,

	XO_COMPUTE_LIMIT,
	XO_CHECK_LIMIT,
	XO_UPDATE_LIMIT,

	/* joins */
	XO_PROBE_HASH,

	/* tuple sinks */
	XO_HASH_TUPLE,
	XO_HASHAGG_TUPLE,
	XO_SORTAGG_TUPLE,
	XO_SORT_TUPLE,

	/* helper steps referenced by multiple executor nodes */
	XO_QUAL_SCAN,
	XO_QUAL_JOIN,
	XO_PROJECT_SCAN,
	XO_PROJECT_JOIN,
	XO_PARAM,
	XO_RESCAN,
	XO_RETURN,
	XO_DONE
} ExecEvalOp;

struct ExecStep
{
	/* XXX: convert to jump threading */
	ExecEvalOp opcode;

	union
	{
		/* for XO_SEQSCAN */
		struct
		{
			SeqScanState *state;
			int slot;
			int jumpempty;
		} seqscan;

		/* for XO_INDEX_SCAN[_FIRST] */
		struct
		{
			IndexScanState *state;
			int slot;
			int jumpempty;
		} indexscan;

		/* for XO_INDEXONLY_SCAN[_FIRST] */
		struct
		{
			IndexOnlyScanState *state;
			int slot;
			int jumpempty;
		} ioscan;

		/* for XO_INIT_HASH, XO_HASH_TUPLE */
		struct
		{
			int jumpbuilt;
			int jumpnext;
			int inputslot;
			struct HashJoinState *hjstate;
			struct HashState *hstate;
		} hash;

		/* for XO_PROBE_HASH */
		struct
		{
			struct HashJoinState *state;
			int inputslot;
			int jumpmiss;
			int probeslot;
		} hj;

		/* for XO_QUAL_* */
		struct
		{
			ExprState *qual;
			ExprContext *econtext;
			int scanslot;
			int innerslot;
			int outerslot;
			int jumpfail;
		}			qual;

		/* for XO_PROJECT_* */
		struct
		{
			ProjectionInfo *project;
			int scanslot;
			int innerslot;
			int outerslot;
			int result;
		}			project;

		/* for XO_RESCAN */
		struct
		{
			int num_nodes;
			PlanState **nodes;
		} rescan;

		/* for XO_PARAM */
		struct
		{
			int slot;
			int nparams;
			/* FIXME: shouldn't be nestloop specific */
			NestLoopParam *params;
			ExprContext *econtext;
			PlanState *ps;
		} param;

		/* for XO_RETURN */
		struct
		{
			int next;
			int slotno;
		} ret;

		/* for XO_SORTAGG_TUPLE */
		struct
		{
			int jumpnext;
			int jumpgroup;
			int jumpempty;
			int inputslot;
			int outputslot;
			struct AggState *state;
		} sortagg;

		/* for XO_HASHAGG_TUPLE */
		struct
		{
			int jumpnext;
			int inputslot;
			struct AggState *state;
		} hashagg;

		/* for XO_DRAIN_HASHAGG */
		struct
		{
			int jumpempty;
			int outputslot;
			struct AggState *state;
		} drain_hashagg;


		/* for XO_SORT_TUPLE and XO_DRAIN_SORT */
		struct
		{
			int jumpempty;
			int jumpnext;
			int inputslot;
			int outputslot;
			struct SortState *state;
		} sort;

		/* for XO_LIMIT* */
		struct
		{
			int jumpnext;
			int jumpout;
			bool initialized;
			struct LimitState *state;
		} limit;

	} d;
};

extern ExecProgram *ExecBuildProgram(PlanState *node, EState *estate, int eflags);
extern TupleTableSlot *ExecExecProgram(ExecProgram *p, EState *estate);
extern StringInfo ExecDescribeProgram(ExecProgram *p);
extern void ExecShutdownProgram(ExecProgram *p);



typedef struct EmitForPlanNodeData
{
	int jumpret;
	int resslot;
	List *resetnodes;
} EmitForPlanNodeData;

extern void ExecProgramBuildForNode(ExecProgramBuild *b, PlanState *node, int eflags, int jumpfail, EmitForPlanNodeData *d);
extern ExecStep *ExecProgramAddStep(ExecProgramBuild *b);
extern int ExecProgramAddSlot(ExecProgramBuild *b);
extern void ExexProgramAssignJump(ExecProgramBuild *b, ExecStep *s, int *a, int t);
extern void ExecProgramDefineJump(ExecProgramBuild *b, int var, int target);


#endif							/* EXEC_PROGRAM_H */
