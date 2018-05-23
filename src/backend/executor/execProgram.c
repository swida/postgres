/*-------------------------------------------------------------------------
 *
 * execProgram.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execProgram.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execProgram.h"

#include "access/relscan.h"
#include "access/heapam.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "executor/nodeAppend.h"
#include "executor/nodeBitmapAnd.h"
#include "executor/nodeBitmapHeapscan.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeBitmapOr.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeCustom.h"
#include "executor/nodeForeignscan.h"
#include "executor/nodeFunctionscan.h"
#include "executor/nodeGather.h"
#include "executor/nodeGatherMerge.h"
#include "executor/nodeGroup.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeLimit.h"
#include "executor/nodeLockRows.h"
#include "executor/nodeMaterial.h"
#include "executor/nodeMergeAppend.h"
#include "executor/nodeMergejoin.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeNamedtuplestorescan.h"
#include "executor/nodeNestloop.h"
#include "executor/nodeProjectSet.h"
#include "executor/nodeRecursiveunion.h"
#include "executor/nodeResult.h"
#include "executor/nodeSamplescan.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeSetOp.h"
#include "executor/nodeSort.h"
#include "executor/nodeSubplan.h"
#include "executor/nodeSubqueryscan.h"
#include "executor/nodeTableFuncscan.h"
#include "executor/nodeTidscan.h"
#include "executor/nodeUnique.h"
#include "executor/nodeValuesscan.h"
#include "executor/nodeWindowAgg.h"
#include "executor/nodeWorktablescan.h"
#include "nodes/nodeFuncs.h"
#include "miscadmin.h"


/*
 * Add another expression evaluation step to ExprState->steps.
 *
 * Note that this potentially re-allocates es->steps, therefore no pointer
 * into that array may be used while the expression is still being built.
 */
ExecStep *
ExecProgramAddStep(ExecProgramBuild *b)
{
	ExecProgram *p = b->program;

	if (b->steps_alloc == 0)
	{
		b->steps_alloc = 16;
		b->program->steps = palloc(sizeof(ExecStep) * b->steps_alloc);
	}
	else if (b->steps_alloc == p->steps_len)
	{
		b->steps_alloc *= 2;
		p->steps = repalloc(p->steps,
							sizeof(ExecStep) * b->steps_alloc);
	}

	return &p->steps[p->steps_len++];
}

int
ExecProgramAddSlot(ExecProgramBuild *b)
{
	ExecProgram *p = b->program;

	if (b->slots_alloc == 0)
	{
		b->slots_alloc = 16;
		b->program->slots = palloc(sizeof(TupleTableSlot *) * b->slots_alloc);
	}
	else if (b->slots_alloc == p->slots_len)
	{
		b->slots_alloc *= 2;
		p->slots = repalloc(p->slots,
							sizeof(TupleTableSlot *) * b->slots_alloc);
	}

	return p->slots_len++;
}

void
ExexProgramAssignJump(ExecProgramBuild *b, ExecStep *s, int *a, int t)
{
	*a = t;

	/* if it's a not yet known */
	if (t < 0)
	{
		ExecRelocatableJump *j;

		if (b->reloc_jumps_alloc == 0)
		{
			b->reloc_jumps_alloc = 16;
			b->reloc_jumps = palloc(sizeof(ExecRelocatableJump) * b->reloc_jumps_alloc);
		}
		else if (b->reloc_jumps_alloc == b->reloc_jumps_len)
		{
			b->reloc_jumps_alloc *= 2;
			b->reloc_jumps = repalloc(b->reloc_jumps,
									  sizeof(ExecRelocatableJump) * b->reloc_jumps_alloc);
		}

		j = &b->reloc_jumps[b->reloc_jumps_len++];

		j->varno = t;
		j->step = s - b->program->steps;
		j->offset = (char *) a - (char *) s;
	}
}

void
ExecProgramDefineJump(ExecProgramBuild *b, int var, int target)
{
	int i;

	Assert(var < 0);
	Assert(target >= 0);

	for (i = 0; i < b->reloc_jumps_len; i++)
	{
		ExecRelocatableJump *j = &b->reloc_jumps[i];
		ExecStep *step = &b->program->steps[j->step];
		int *val = (int*)(((char *) step) + j->offset);

		if (*val != var)
			continue;
		*val = target;
	}
}

ExecProgram *
ExecBuildProgram(PlanState *node, EState *estate, int eflags)
{
	ExecProgramBuild build = {};
	ExecStep *step;
	int resvar = --build.varno;
	EmitForPlanNodeData nodeData = {};

	build.program = (ExecProgram *) palloc0(sizeof(ExecProgram));
	build.estate = estate;

	ExecProgramBuildForNode(&build, node, eflags, resvar, &nodeData);
	if (build.failed)
		return NULL;

	step = ExecProgramAddStep(&build);
	step->opcode = XO_RETURN;
	step->d.ret.slotno = nodeData.resslot;
	step->d.ret.next = nodeData.jumpret;

	step = ExecProgramAddStep(&build);
	step->opcode = XO_DONE;

	ExecProgramDefineJump(&build, resvar, build.program->steps_len - 1);

	return build.program;
}

void
ExecProgramBuildForNode(ExecProgramBuild *b, PlanState *node, int eflags, int jumpfail, EmitForPlanNodeData *d)
{
	switch (nodeTag(node))
	{
		case T_SeqScanState:
			ExecProgramBuildForSeqScan(b, node, eflags, jumpfail, d);
			break;

		case T_IndexScanState:
			ExecProgramBuildForIndexScan(b, node, eflags, jumpfail, d);
			break;

		case T_IndexOnlyScanState:
			ExecProgramBuildForIndexOnlyScan(b, node, eflags, jumpfail, d);
			break;

		case T_AggState:
			ExecProgramBuildForAgg(b, node, eflags, jumpfail, d);
			break;

		case T_NestLoopState:
			ExecProgramBuildForNestloop(b, node, eflags, jumpfail, d);
			break;

		case T_HashJoinState:
			ExecProgramBuildForHashJoin(b, node, eflags, jumpfail, d);
			break;

		case T_HashState:
			pg_unreachable();
			break;

		case T_SortState:
			ExecProgramBuildForSort(b, node, eflags, jumpfail, d);
			break;

		case T_LimitState:
			ExecProgramBuildForLimit(b, node, eflags, jumpfail, d);
			break;

		default:
			b->failed = true;
			break;
	}
}



extern TupleTableSlot *
ExecExecProgram(ExecProgram *p, EState *estate)
{
	ExecStep *step = &p->steps[p->cur_step];
	TupleTableSlot **slots = p->slots;

eval:
	switch(step->opcode)
	{
		case XO_SEQSCAN_FIRST:
			{
				SeqScanState *state = step->d.seqscan.state;
				TableScanDesc scandesc = state->ss.ss_currentScanDesc;

				if (scandesc == NULL)
				{
					/*
					 * We reach here if the scan is not parallel, or if we're executing a
					 * scan that was intended to be parallel serially.
					 */
					scandesc = table_beginscan(state->ss.ss_currentRelation,
											  estate->es_snapshot,
											  0, NULL);
					state->ss.ss_currentScanDesc = scandesc;
				}

				step++;
			}
			/* FALLTHROUGH */

		case XO_SEQSCAN:
			{
				TupleTableSlot *slot = slots[step->d.seqscan.slot];
				SeqScanState *state = step->d.seqscan.state;
				TableScanDesc scandesc = state->ss.ss_currentScanDesc;

				/*
				 * get the next tuple from the table
				 */
				if (!table_scan_getnextslot(scandesc, ForwardScanDirection, slot))				{
					ExecClearTuple(slot);
					step = &p->steps[step->d.seqscan.jumpempty];
				}
				else
				{
					step++;
				}

				goto eval;
			}

		case XO_INDEX_SCAN_FIRST:
			{
				IndexScanState *state = step->d.indexscan.state;
				IndexScanDesc scandesc = state->iss_ScanDesc;

				if (scandesc == NULL)
				{
					/*
					 * We reach here if the index scan is not parallel, or if we're
					 * executing a index scan that was intended to be parallel serially.
					 */
					scandesc = index_beginscan(state->ss.ss_currentRelation,
											   state->iss_RelationDesc,
											   estate->es_snapshot,
											   state->iss_NumScanKeys,
											   state->iss_NumOrderByKeys);

					state->iss_ScanDesc = scandesc;

					/*
					 * If no run-time keys to calculate or they are ready, go ahead and
					 * pass the scankeys to the index AM.
					 */
					if (state->iss_NumRuntimeKeys == 0 || state->iss_RuntimeKeysReady)
						index_rescan(scandesc,
									 state->iss_ScanKeys, state->iss_NumScanKeys,
									 state->iss_OrderByKeys, state->iss_NumOrderByKeys);

				}

				step++;
			}
			/* FALLTHROUGH */

		case XO_INDEX_SCAN:
			{
				TupleTableSlot *slot = slots[step->d.indexscan.slot];
				IndexScanState *state = step->d.indexscan.state;
				IndexScanDesc scandesc = state->iss_ScanDesc;
				HeapTuple	tuple;

				/*
				 * get the next tuple from the table
				 */
				if (!index_getnext_slot(scandesc, ForwardScanDirection, slot))
				{
					ExecClearTuple(slot);
					step = &p->steps[step->d.indexscan.jumpempty];
				}
				else
				{
					step++;
				}

				goto eval;
			}


		case XO_INDEXONLY_SCAN_FIRST:
			{
				IndexOnlyScanState *state = step->d.ioscan.state;
				IndexScanDesc scandesc = state->ioss_ScanDesc;

				if (scandesc == NULL)
				{
					/*
					 * We reach here if the index scan is not parallel, or if we're
					 * executing a index scan that was intended to be parallel serially.
					 */
					scandesc = index_beginscan(state->ss.ss_currentRelation,
											   state->ioss_RelationDesc,
											   estate->es_snapshot,
											   state->ioss_NumScanKeys,
											   state->ioss_NumOrderByKeys);

					state->ioss_ScanDesc = scandesc;

					/*
					 * If no run-time keys to calculate or they are ready, go ahead and
					 * pass the scankeys to the index AM.
					 */
					if (state->ioss_NumRuntimeKeys == 0 || state->ioss_RuntimeKeysReady)
						index_rescan(scandesc,
									 state->ioss_ScanKeys, state->ioss_NumScanKeys,
									 state->ioss_OrderByKeys, state->ioss_NumOrderByKeys);

				}

				step++;
			}
			/* FALLTHROUGH */

		case XO_INDEXONLY_SCAN:
			{
				TupleTableSlot *slot = slots[step->d.ioscan.slot];
				IndexOnlyScanState *state = step->d.ioscan.state;

				/*
				 * get the next tuple from the table
				 *
				 * XXX: qual invocations should be moved to here.
				 */
				if (!indexonly_getnext(state, slot, ForwardScanDirection))
				{
					ExecClearTuple(slot);
					step = &p->steps[step->d.indexscan.jumpempty];
				}
				else
				{
					step++;
				}

				goto eval;
			}

		case XO_QUAL_SCAN:
			{
				TupleTableSlot *scanslot = slots[step->d.qual.scanslot];
				ExprContext *econtext = step->d.qual.econtext;

				econtext->ecxt_scantuple = scanslot;

				if (ExecQualAndReset(step->d.qual.qual, econtext))
				{
					step++;
				}
				else
				{
					step = &p->steps[step->d.qual.jumpfail];
				}

				goto eval;
			}

		case XO_QUAL_JOIN:
			{
				TupleTableSlot *outerslot = slots[step->d.qual.outerslot];
				TupleTableSlot *innerslot = slots[step->d.qual.innerslot];
				ExprContext *econtext = step->d.qual.econtext;

				econtext->ecxt_outertuple = outerslot;
				econtext->ecxt_innertuple = innerslot;

				if (ExecQualAndReset(step->d.qual.qual, econtext))
				{
					step++;
				}
				else
				{
					step = &p->steps[step->d.qual.jumpfail];
				}

				goto eval;
			}

		case XO_PROJECT_SCAN:
			{
				TupleTableSlot *scanslot = slots[step->d.project.scanslot];
				ProjectionInfo *project = step->d.project.project;

				project->pi_exprContext->ecxt_scantuple = scanslot;

				ExecProject(project);

				step++;
				goto eval;
			}

		case XO_PROJECT_JOIN:
			{
				TupleTableSlot *innerslot = slots[step->d.project.innerslot];
				TupleTableSlot *outerslot = slots[step->d.project.outerslot];
				ProjectionInfo *project = step->d.project.project;

				project->pi_exprContext->ecxt_outertuple = outerslot;
				project->pi_exprContext->ecxt_innertuple = innerslot;

				ExecProject(project);

				step++;
				goto eval;
			}

		case XO_SORTAGG_TUPLE:
			{
				AggState *state = step->d.sortagg.state;
				TupleTableSlot *inputslot = slots[step->d.sortagg.inputslot];
				AggStateBoundaryState action;

				action = agg_fill_direct_onetup(state, inputslot);

				switch (action)
				{
					case AGGBOUNDARY_NEXT:
						step = &p->steps[step->d.sortagg.jumpnext];
						break;
					case AGGBOUNDARY_REACHED:
						step = &p->steps[step->d.sortagg.jumpgroup];
						break;
					case AGGBOUNDARY_FINISHED:
						step = &p->steps[step->d.sortagg.jumpempty];
						break;
				}
				goto eval;
			}

		case XO_HASHAGG_TUPLE:
			{
				TupleTableSlot *slot = slots[step->d.hashagg.inputslot];
				AggState *state = step->d.hashagg.state;

				agg_fill_hash_table_onetup(state, slot);

				step = &p->steps[step->d.hashagg.jumpnext];
				goto eval;
			}

		case XO_DRAIN_HASHAGG:
			{
				TupleTableSlot *slot = slots[step->d.drain_hashagg.outputslot];
				AggState *state = step->d.drain_hashagg.state;
				TupleTableSlot *slot2;

				/*
				 * XXX: Good chunks of this should be moved here, so that when
				 * JITing the function calls to expressions happen here. As
				 * they are in turn JITed, that'd allow them to be inlined
				 * (without full inlining, which'd not recognize the
				 * opportunity anway).
				 */
				slot2 = agg_retrieve_hash_table(state);
				Assert(slot2 == NULL || slot == slot2);
				if (slot2 == NULL)
				{
					step = &p->steps[step->d.drain_hashagg.jumpempty];
				}
				else
				{
					step++;
				}
				goto eval;
			}

		case XO_INIT_SORT:
			{
				SortState *state = step->d.sort.state;
				PlanState  *outerNode;
				TupleDesc	tupDesc;
				Sort	   *plannode = (Sort *) state->ss.ps.plan;

				outerNode = outerPlanState(state);
				tupDesc = ExecGetResultType(outerNode);

				state->tuplesortstate =
					tuplesort_begin_heap(tupDesc,
										 plannode->numCols,
										 plannode->sortColIdx,
										 plannode->sortOperators,
										 plannode->collations,
										 plannode->nullsFirst,
										 work_mem,
										 NULL, state->randomAccess);
				if (state->bounded)
					tuplesort_set_bound(state->tuplesortstate, state->bound);

				step++;
				goto eval;
			}

		case XO_SORT_TUPLE:
			{
				TupleTableSlot *slot = slots[step->d.sort.inputslot];
				SortState *state = step->d.sort.state;

				tuplesort_puttupleslot(state->tuplesortstate, slot);

				step = &p->steps[step->d.sort.jumpnext];
				goto eval;
			}

		case XO_SORT:
			{
				SortState *state = step->d.sort.state;
				Tuplesortstate *tuplesortstate = state->tuplesortstate;

				if (!state->sort_Done)
				{
					tuplesort_performsort(tuplesortstate);
					state->sort_Done = true;
				}

				/* could fall through? */
				step++;
				goto eval;
			}

		case XO_DRAIN_SORT:
			{
				TupleTableSlot *slot = slots[step->d.sort.outputslot];
				SortState *state = step->d.sort.state;
				Tuplesortstate *tuplesortstate = state->tuplesortstate;

				Assert(state->sort_Done);

				if (tuplesort_gettupleslot(tuplesortstate,
										   ForwardScanDirection,
										   false, slot, NULL))
					step++;
				else
					step = &p->steps[step->d.sort.jumpempty];

				goto eval;
			}

		case XO_INIT_HASH:
			{
				HashJoinState *hjstate = step->d.hash.hjstate;
				HashState *hstate = step->d.hash.hstate;

				if (hjstate->hj_HashTable)
				{
					step = &p->steps[step->d.hash.jumpbuilt];
				}
				else
				{
					HashJoinTable hashtable;
					hashtable = ExecHashTableCreate(
							hstate, hjstate->hj_HashOperators,
							hjstate->hj_Collations,
							false);
					hjstate->hj_HashTable = hashtable;

					step++;
				}

				goto eval;
			}

		case XO_HASH_TUPLE:
			{
				HashJoinState *hjstate = step->d.hash.hjstate;
				HashState *hstate = step->d.hash.hstate;
				TupleTableSlot *slot = p->slots[step->d.hash.inputslot];

				ExecHashDoInsert(hjstate, hstate, slot);

				step = &p->steps[step->d.hash.jumpnext];

				goto eval;
			}

		case XO_PROBE_HASH:
			{
				HashJoinState *hjstate = step->d.hj.state;
				ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
				HashJoinTable hashtable = hjstate->hj_HashTable;
				uint32 hashvalue;
				int			batchno;

				if (!hjstate->hj_OuterNotEmpty)
				{
					TupleTableSlot *slot = p->slots[step->d.hash.inputslot];

					econtext->ecxt_outertuple = slot;

					if (!ExecHashGetHashValue(hashtable, econtext,
											 hjstate->hj_OuterHashKeys,
											 true,	/* outer tuple */
											 HJ_FILL_OUTER(hjstate),
											 &hashvalue))
					{

						hjstate->hj_OuterNotEmpty = false;
						step = &p->steps[step->d.hj.jumpmiss];
						goto eval;
					}

					/* remember outer relation is not empty for possible rescan */
					hjstate->hj_OuterNotEmpty = true;

					hjstate->hj_CurHashValue = hashvalue;
					ExecHashGetBucketAndBatch(hashtable, hashvalue,
											  &hjstate->hj_CurBucketNo, &batchno);
					hjstate->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																		hashvalue);
					hjstate->hj_CurTuple = NULL;

				}

				if (ExecScanHashBucket(hjstate, econtext))
				{
					step++;
				}
				else
				{
					hjstate->hj_OuterNotEmpty = false;

					/* out of matches; check for possible outer-join fill */
					hjstate->hj_JoinState = HJ_FILL_OUTER_TUPLE;

					step = &p->steps[step->d.hj.jumpmiss];
				}

				goto eval;
			}

		case XO_RESCAN:
			{
				int i;

				for (i = 0; i < step->d.rescan.num_nodes; i++)
				{
					ExecReScan(step->d.rescan.nodes[i]);
				}

				step++;
				goto eval;
			}

		case XO_PARAM:
			{
				int nparam = step->d.param.nparams;
				NestLoopParam *params = step->d.param.params;
				ExprContext *econtext = step->d.param.econtext;
				TupleTableSlot *slot = slots[step->d.param.slot];
				PlanState *plan = step->d.param.ps;
				int i;

				for (i = 0; i < nparam; i++)
				{
					NestLoopParam *nlp = &params[i];
					int			paramno = nlp->paramno;
					ParamExecData *prm = &econtext->ecxt_param_exec_vals[paramno];


					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					prm->value = slot_getattr(slot,
											  nlp->paramval->varattno,
											  &(prm->isnull));
					plan->chgParam = bms_add_member(plan->chgParam, paramno);
				}

				step++;
				goto eval;
			}

		case XO_COMPUTE_LIMIT:
			ExecComputeLimit(step->d.limit.state);
			step++;
			goto eval;

		case XO_CHECK_LIMIT:
			if (ExecCheckLimit(step->d.limit.state))
				step = &p->steps[step->d.limit.jumpout];
			else if (unlikely(!step->d.limit.initialized))
			{
				step->d.limit.initialized = true;;
				step++;
			}
			else
			{
				step = &p->steps[step->d.limit.jumpnext];
			}
			goto eval;

		case XO_UPDATE_LIMIT:
			if (ExecUpdateLimit(step->d.limit.state))
				step = &p->steps[step->d.limit.jumpnext];
			else
				step++;

			goto eval;

		case XO_RETURN:
			{
				TupleTableSlot *slot = slots[step->d.ret.slotno];

				/* setup for next call */
				p->cur_step = step->d.ret.next;

				return slot;
			}

		case XO_DONE:
			{
				return NULL;
			}

		default:
			pg_unreachable();
			break;
	}

	return NULL;
}

extern void
ExecShutdownProgram(ExecProgram *p)
{
}

extern StringInfo
ExecDescribeProgram(ExecProgram *p)
{
	StringInfo s = makeStringInfo();
	int i = 0;

	for (i = 0; i < p->steps_len; i++)
	{
		ExecStep *step = &p->steps[i];

		appendStringInfo(s, "%d: ", i);

		switch(step->opcode)
		{
			case XO_SEQSCAN_FIRST:
				{
					appendStringInfo(s, "seqscan_first\n");
					break;
				}

			case XO_SEQSCAN:
				{
					appendStringInfo(s, "seqscan [j empty %d] > s%d\n",
									 step->d.seqscan.jumpempty,
									 step->d.seqscan.slot);
					break;
				}

			case XO_INDEX_SCAN_FIRST:
				{
					appendStringInfo(s, "indexscan_first\n");
					break;
				}

			case XO_INDEX_SCAN:
				{
					appendStringInfo(s, "indexscan [j empty %d] > s%d\n",
									 step->d.indexscan.jumpempty,
									 step->d.indexscan.slot);
					break;
				}

			case XO_INDEXONLY_SCAN_FIRST:
				{
					appendStringInfo(s, "indexonlyscan_first\n");
					break;
				}

			case XO_INDEXONLY_SCAN:
				{
					appendStringInfo(s, "indexonlyscan [j empty %d] > s%d\n",
									 step->d.ioscan.jumpempty,
									 step->d.ioscan.slot);
					break;
				}

			case XO_QUAL_SCAN:
				{
					appendStringInfo(s, "qual [j fail %d] < scan s%d\n",
									 step->d.qual.jumpfail,
									 step->d.qual.scanslot);
					break;
				}

			case XO_QUAL_JOIN:
				{
					appendStringInfo(s, "qual [j fail %d] < outer s%d inner s%d\n",
									 step->d.qual.jumpfail,
									 step->d.qual.outerslot,
									 step->d.qual.innerslot);
					break;
				}

			case XO_PROJECT_SCAN:
				{
					appendStringInfo(s, "project < scan s%d > s%d\n",
									 step->d.project.scanslot,
									 step->d.project.result);
					break;
				}

			case XO_PROJECT_JOIN:
				{
					appendStringInfo(s, "project < outer s%d inner s%d > s%d\n",
									 step->d.project.outerslot,
									 step->d.project.innerslot,
									 step->d.project.result);
					break;
				}

			case XO_HASHAGG_TUPLE:
				{
					appendStringInfo(s, "hashagg_tuple [j %d] < s%d\n",
									 step->d.hashagg.jumpnext,
									 step->d.hashagg.inputslot);
					break;
				}

			case XO_HASH_TUPLE:
				{
					appendStringInfo(s, "hash_tuple [j %d] < s%d\n",
									 step->d.hash.jumpnext,
									 step->d.hash.inputslot);
					break;
				}

			case XO_SORTAGG_TUPLE:
				{
					appendStringInfo(s, "sortagg_tuple [j next %d, j group %d, j empty %d] < s%d > s%d\n",
									 step->d.sortagg.jumpnext,
									 step->d.sortagg.jumpgroup,
									 step->d.sortagg.jumpempty,
									 step->d.sortagg.inputslot,
									 step->d.sortagg.outputslot);
					break;
				}

			case XO_DRAIN_HASHAGG:
				{
					appendStringInfo(s, "drain_hashagg [j empty %d] > s%d\n",
									 step->d.drain_hashagg.jumpempty,
									 step->d.drain_hashagg.outputslot);
					break;
				}

			case XO_INIT_HASH:
				{
					appendStringInfo(s, "init_hash [j built %d]\n",
									 step->d.hash.jumpbuilt);
					break;
				}

			case XO_PROBE_HASH:
				{
					appendStringInfo(s, "probe_hash [j miss %d] < s%d > s%d\n",
									 step->d.hj.jumpmiss,
									 step->d.hj.inputslot,
									 step->d.hj.probeslot);
					break;
				}

			case XO_INIT_SORT:
				{
					appendStringInfo(s, "init_sort\n");
					break;
				}

			case XO_SORT_TUPLE:
				{
					appendStringInfo(s, "sort_tuple [j %d] < s%d\n",
									 step->d.sort.jumpnext,
									 step->d.sort.inputslot);
					break;
				}

			case XO_SORT:
				{
					appendStringInfo(s, "sort\n");
					break;
				}

			case XO_DRAIN_SORT:
				{
					appendStringInfo(s, "drain_sort [j empty %d] > s%d\n",
									 step->d.sort.jumpempty,
									 step->d.sort.outputslot);
					break;
				}

			case XO_RESCAN:
				{
					appendStringInfo(s, "rescan #%d\n",
									 step->d.rescan.num_nodes);
					break;
				}

			case XO_PARAM:
				{
					appendStringInfo(s, "param #%d < s%d\n",
									 step->d.param.nparams,
									 step->d.param.slot);
					break;
				}

			case XO_COMPUTE_LIMIT:
				{
					appendStringInfo(s, "compute_limit\n");
					break;
				}

			case XO_CHECK_LIMIT:
				{
					appendStringInfo(s, "check_limit [j empty %d, j reenter %d]\n",
									 step->d.limit.jumpout,
									 step->d.limit.jumpnext);
					break;
				}

			case XO_UPDATE_LIMIT:
				{
					appendStringInfo(s, "update_limit [j next %d]\n",
									 step->d.limit.jumpnext);
					break;
				}

			case XO_RETURN:
				{
					appendStringInfo(s, "return < s%d [next %d]\n",
									 step->d.ret.slotno,
									 step->d.ret.next);
					break;
				}

			case XO_DONE:
				{
					appendStringInfo(s, "done\n");
					break;
				}

			default:
				appendStringInfo(s, "unknown\n");
				break;
		}
	}

	return s;
}
