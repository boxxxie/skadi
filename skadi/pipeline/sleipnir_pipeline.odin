/*
Package `pipeline` provides a cache-resident Sleipnir actor execution pipeline
(`Morsel_Pipeline`) for Skadi columnar analytics, built on `actor:.` (`sleipnir/actor`).

It dispatches coarsened super-morsels over pooled MPSC work batches
(`Envelope_Pool` + `mpsc_push_chain`), processes L1d/L2-resident `u16` morsel
selection vectors, and aggregates partial results through wait-free
`Sharded_SPSC_Mailbox` lanes with zero steady-state heap allocations for fused
predicates, dictionary histograms, and MSB-bucket parallel sorting.
*/
package pipeline

import "base:intrinsics"
import "core:mem"
import "core:sync"
import oa ".."
import actor "actor:."

// Cache-resident Sleipnir actor morsel pipeline using pooled MPSC work batches
// (`Envelope_Pool` + `mpsc_push_chain`), coarsened super-morsels stepped across
// compact `u16` L1d/L2 sub-morsel selection vectors (`<= 16,384` rows = 32 KB),
// and wait-free `Sharded_SPSC_Mailbox` subtotal aggregation (0 heap allocations
// in steady state).

MORSEL_MAX_ROWS             :: oa.MORSEL_MAX_ROWS
SUPER_MORSEL_MULTIPLIER     :: 8
DISPATCH_BATCH_SIZE         :: 8
WORK_POOL_CAP               :: 4096
SPSC_LANE_CAP               :: 512
SORT_PARALLEL_PARTITION_MIN :: 8192
DICT_INITIAL_SCRATCH_CAP    :: 1024

Agg_Result :: oa.Aggregates

Morsel_Work_Kind :: enum u8 {
	Sel_2Pred_Sum  = 0,
	Agg_Where      = 1,
	Dict_Counts    = 2,
	Dict_Group_Sum = 3,
}

Morsel_Work :: struct {
	kind:            Morsel_Work_Kind,
	col_a:           ^oa.Array,
	col_b:           ^oa.Array,
	codes_ptr:       [^]i32,
	dict_len:        int,
	row_start:       int,
	row_len:         int,
	sub_morsel_rows: int,
	morsel_idx:      int,
	op1:             oa.Compare_Op,
	s1:              f64,
	op2:             oa.Compare_Op,
	s2:              f64,
}

Morsel_Subtotal :: struct {
	sum:        f64,
	min_val:    f64,
	max_val:    f64,
	count:      int,
	morsel_idx: int,
	valid:      bool,
}

Sort_Task_Kind :: enum u8 {
	Count_U32_Signed,
	Count_U32_Unsigned,
	Scatter_U32_Signed,
	Scatter_U32_Unsigned,
	Count_U64_Key,
	Scatter_U64_Key,
	Sort_Bucket_U64,
	Count_Umbra,
	Scatter_Umbra,
	Sort_Bucket_Umbra,
	Count_Str_Lead,
	Scatter_Str_Lead,
	Count_Str_Byte,
	Scatter_Str_Byte,
	Sort_Bucket_Str,
}

Sort_Bucket_Work :: struct {
	kind:            Sort_Task_Kind,
	shift:           u32,
	rem_mask:        u32,
	start:           int,
	end:             int,
	out_start:       int,
	raw_u32:         [^]u32,
	entries_src:     []u64,
	entries_dst:     []u64,
	out_i64:         []i64,
	umbra_arr:       ^oa.Umbra_Array,
	str_offsets:     [^]i32,
	str_bytes:       [^]u8,
	str_arr_off:     int,
	str_entries_src: []oa.Str_Sort_Entry,
	str_entries_dst: []oa.Str_Sort_Entry,
}

Sort_Bucket_Done :: struct {
	done: u32,
}

@(private="file")
_Worker_State :: struct {
	actor_idx: int,
	scratch:   ^[MORSEL_MAX_ROWS]u16,
	out_box:   ^actor.Sharded_SPSC_Mailbox(Morsel_Subtotal, SPSC_LANE_CAP),
	pipeline:  ^Morsel_Pipeline,
}

@(private="file")
_Sort_Worker_State :: struct {
	actor_idx: int,
	hist:      ^[256]u32,
	offs:      ^[256]u32,
	out_box:   ^actor.Sharded_SPSC_Mailbox(Sort_Bucket_Done, SPSC_LANE_CAP),
}

Morsel_Pipeline :: struct {
	sched:                ^actor.Scheduler,
	work_pools:           []actor.Envelope_Pool(Morsel_Work),
	subtotal_box:         actor.Sharded_SPSC_Mailbox(Morsel_Subtotal, SPSC_LANE_CAP),
	workers:              []^actor.Actor(_Worker_State, Morsel_Work),
	scratches:            [][MORSEL_MAX_ROWS]u16,
	dispatch_bufs:        [][DISPATCH_BATCH_SIZE]Morsel_Work,
	dispatch_counts:      []int,
	dict_worker_counts:   []i64,
	dict_worker_sums:     []f64,
	dict_scratch_stride:  int,
	sort_pools:           []actor.Envelope_Pool(Sort_Bucket_Work),
	sort_done_box:        actor.Sharded_SPSC_Mailbox(Sort_Bucket_Done, SPSC_LANE_CAP),
	sort_workers:         []^actor.Actor(_Sort_Worker_State, Sort_Bucket_Work),
	sort_dispatch_bufs:   [][DISPATCH_BATCH_SIZE]Sort_Bucket_Work,
	sort_dispatch_counts: []int,
	sort_worker_hists:    [][256]u32,
	sort_worker_offs:     [][256]u32,
	sort_entries_a:       []u64,
	sort_entries_b:       []u64,
	str_entries_a:        []oa.Str_Sort_Entry,
	str_entries_b:        []oa.Str_Sort_Entry,
	num_workers:          int,
	allocator:            mem.Allocator,
	initialized:          bool,
}

@(private="file")
_Agg_State :: struct {
	sum:         f64,
	min_val:     f64,
	max_val:     f64,
	valid_count: int,
	valid:       bool,
}

@(private="file")
_agg_subtotal_handler :: proc(state: ^_Agg_State, msg: ^Morsel_Subtotal) {
	if msg.count <= 0 { return }
	state.sum         += msg.sum
	state.valid_count += msg.count
	if msg.valid {
		if state.valid {
			if state.min_val != state.min_val || msg.min_val < state.min_val { state.min_val = msg.min_val }
			if state.max_val != state.max_val || msg.max_val > state.max_val { state.max_val = msg.max_val }
		} else {
			state.min_val = msg.min_val
			state.max_val = msg.max_val
			state.valid   = true
		}
	}
}

morsel_select_compare_f64_u16 :: oa.morsel_select_compare_f64_u16
morsel_refine_compare_f64_u16 :: oa.morsel_refine_compare_f64_u16
morsel_sum_f64_u16            :: oa.morsel_sum_f64_u16
morsel_agg_f64_u16            :: oa.morsel_agg_f64_u16

@(private="file")
_worker_fallback_range :: proc(msg: ^Morsel_Work, step: int, sub: ^Morsel_Subtotal) {
	ws: [MORSEL_MAX_ROWS]i32 = ---
	end := msg.row_start + msg.row_len
	for pos := msg.row_start; pos < end; pos += step {
		rlen    := min(step, end - pos)
		a_slice := oa.array_slice(msg.col_a^, pos, pos + rlen)
		b_slice := oa.array_slice(msg.col_b^, pos, pos + rlen)
		sel     := oa.compute_select_compare_into(&a_slice, msg.op1, msg.s1, ws[:rlen])
		oa.selection_refine_compare_inplace(&sel, &b_slice, msg.op2, msg.s2)
		if sel.length > 0 {
			msum, mvc := oa.compute_sum_selection(&a_slice, &sel)
			sub.sum   += msum
			sub.count += mvc
		}
	}
}

@(private="file")
_worker_agg_where_fallback_range :: proc(msg: ^Morsel_Work, step: int, sub: ^Morsel_Subtotal) {
	end := msg.row_start + msg.row_len
	for pos := msg.row_start; pos < end; pos += step {
		rlen    := min(step, end - pos)
		v_slice := oa.array_slice(msg.col_a^, pos, pos + rlen)
		p_slice := oa.array_slice(msg.col_b^, pos, pos + rlen)
		part    := oa.compute_agg_where_compare(&v_slice, &p_slice, msg.op1, msg.s1)
		if part.valid {
			sub.sum   += part.sum
			sub.count += part.count
			if sub.valid {
				if sub.min_val != sub.min_val || part.min_val < sub.min_val { sub.min_val = part.min_val }
				if sub.max_val != sub.max_val || part.max_val > sub.max_val { sub.max_val = part.max_val }
			} else {
				sub.min_val = part.min_val
				sub.max_val = part.max_val
				sub.valid   = true
			}
		}
	}
}

@(private="file")
_worker_dict_counts_range :: proc(st: ^_Worker_State, msg: ^Morsel_Work, step: int, sub: ^Morsel_Subtotal) {
	dlen := msg.dict_len
	if dlen <= 0 { return }
	stride   := st.pipeline.dict_scratch_stride
	w_counts := st.pipeline.dict_worker_counts[st.actor_idx * stride : st.actor_idx * stride + dlen]
	codes    := msg.codes_ptr
	dlen_u32 := u32(dlen)
	end      := msg.row_start + msg.row_len

	if dlen <= 256 {
		// 4 stack-resident u32 banks (slot 0 absorbs null/negative codes) eliminate
		// store-forwarding stalls on repeated dictionary codes inside L1d.
		b0: [257]u32 = ---
		b1: [257]u32 = ---
		b2: [257]u32 = ---
		b3: [257]u32 = ---
		for k in 0..=dlen {
			b0[k] = 0
			b1[k] = 0
			b2[k] = 0
			b3[k] = 0
		}

		for pos := msg.row_start; pos < end; pos += step {
			rlen := min(step, end - pos)
			p    := mem.ptr_offset(codes, pos)
			i    := 0
			for ; i + 4 <= rlen; i += 4 {
				c0 := u32(p[i])
				c1 := u32(p[i + 1])
				c2 := u32(p[i + 2])
				c3 := u32(p[i + 3])
				u0 := c0 < dlen_u32 ? c0 + 1 : 0
				u1 := c1 < dlen_u32 ? c1 + 1 : 0
				u2 := c2 < dlen_u32 ? c2 + 1 : 0
				u3 := c3 < dlen_u32 ? c3 + 1 : 0
				b0[u0] += 1
				b1[u1] += 1
				b2[u2] += 1
				b3[u3] += 1
			}
			for ; i < rlen; i += 1 {
				c := u32(p[i])
				u := c < dlen_u32 ? c + 1 : 0
				b0[u] += 1
			}
		}

		valid_total := 0
		for k in 0..<dlen {
			cnt := i64(b0[k + 1]) + i64(b1[k + 1]) + i64(b2[k + 1]) + i64(b3[k + 1])
			w_counts[k] += cnt
			valid_total += int(cnt)
		}
		sub.count = valid_total
		return
	}

	valid_total := 0
	for pos := msg.row_start; pos < end; pos += step {
		rlen := min(step, end - pos)
		p    := mem.ptr_offset(codes, pos)
		for i in 0..<rlen {
			c := u32(p[i])
			if c < dlen_u32 {
				w_counts[c] += 1
				valid_total += 1
			}
		}
	}
	sub.count = valid_total
}

@(private="file")
_worker_dict_group_sum_typed :: #force_inline proc(
	codes:    [^]i32,
	dlen_u32: u32,
	weights:  ^oa.Array,
	w_sums:   []f64,
	start:    int,
	end:      int,
	step:     int,
	$T:       typeid,
) -> int {
	when T == u8 {
		wdata := weights.buffers[1].data
	} else {
		wdata := cast([^]T)weights.buffers[1].data
	}
	woff        := weights.offset
	w_nulls     := weights.buffers[0].data != nil
	vbits       := weights.buffers[0].data
	valid_total := 0

	if !w_nulls {
		for pos := start; pos < end; pos += step {
			rlen := min(step, end - pos)
			cp   := mem.ptr_offset(codes, pos)
			wp   := mem.ptr_offset(wdata, woff + pos)
			for i in 0..<rlen {
				c := u32(cp[i])
				if c < dlen_u32 {
					w_sums[c] += f64(wp[i])
					valid_total += 1
				}
			}
		}
	} else {
		for pos := start; pos < end; pos += step {
			rlen := min(step, end - pos)
			cp   := mem.ptr_offset(codes, pos)
			wp   := mem.ptr_offset(wdata, woff + pos)
			for i in 0..<rlen {
				c := u32(cp[i])
				if c < dlen_u32 && oa.bitmap_get(vbits, woff + pos + i) {
					w_sums[c] += f64(wp[i])
					valid_total += 1
				}
			}
		}
	}
	return valid_total
}

@(private="file")
_worker_dict_group_sum_range :: proc(st: ^_Worker_State, msg: ^Morsel_Work, step: int, sub: ^Morsel_Subtotal) {
	dlen := msg.dict_len
	if dlen <= 0 || msg.col_a == nil { return }
	stride   := st.pipeline.dict_scratch_stride
	w_sums   := st.pipeline.dict_worker_sums[st.actor_idx * stride : st.actor_idx * stride + dlen]
	dlen_u32 := u32(dlen)
	start    := msg.row_start
	end      := msg.row_start + msg.row_len

	#partial switch _ in msg.col_a.type {
	case oa.Int8_Type:    sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, i8)
	case oa.Int16_Type:   sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, i16)
	case oa.Int32_Type:   sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, i32)
	case oa.Int64_Type:   sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, i64)
	case oa.UInt8_Type:   sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, u8)
	case oa.UInt16_Type:  sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, u16)
	case oa.UInt32_Type:  sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, u32)
	case oa.UInt64_Type:  sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, u64)
	case oa.Float32_Type: sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, f32)
	case oa.Float64_Type: sub.count = _worker_dict_group_sum_typed(msg.codes_ptr, dlen_u32, msg.col_a, w_sums, start, end, step, f64)
	}
}

@(private="file")
_worker_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(_Worker_State, Morsel_Work), msg: ^Morsel_Work) {
	_ = ctx
	st   := &self.state
	step := msg.sub_morsel_rows if (msg.sub_morsel_rows > 0 && msg.sub_morsel_rows <= MORSEL_MAX_ROWS) else MORSEL_MAX_ROWS
	sub  := Morsel_Subtotal{ morsel_idx = msg.morsel_idx }

	switch msg.kind {
	case .Sel_2Pred_Sum:
		_, is_a_f64 := msg.col_a.type.(oa.Float64_Type)
		_, is_b_f64 := msg.col_b.type.(oa.Float64_Type)
		fast_path   := is_a_f64 && is_b_f64 &&
			msg.col_a.buffers[0].data == nil &&
			msg.col_b.buffers[0].data == nil
		if fast_path {
			scratch := st.scratch[:]
			end     := msg.row_start + msg.row_len
			for pos := msg.row_start; pos < end; pos += step {
				rlen := min(step, end - pos)
				k1   := oa.morsel_select_compare_f64_u16(msg.col_a, msg.op1, msg.s1, pos, rlen, scratch)
				if k1 > 0 {
					k2 := oa.morsel_refine_compare_f64_u16(msg.col_b, msg.op2, msg.s2, pos, scratch, k1)
					if k2 > 0 {
						sub.sum   += oa.morsel_sum_f64_u16(msg.col_a, pos, scratch, k2)
						sub.count += k2
					}
				}
			}
		} else {
			_worker_fallback_range(msg, step, &sub)
		}

	case .Agg_Where:
		_, is_a_f64 := msg.col_a.type.(oa.Float64_Type)
		_, is_b_f64 := msg.col_b.type.(oa.Float64_Type)
		fast_path   := is_a_f64 && is_b_f64 &&
			msg.col_a.buffers[0].data == nil &&
			msg.col_b.buffers[0].data == nil
		if fast_path {
			scratch := st.scratch[:]
			end     := msg.row_start + msg.row_len
			for pos := msg.row_start; pos < end; pos += step {
				rlen := min(step, end - pos)
				k1   := oa.morsel_select_compare_f64_u16(msg.col_b, msg.op1, msg.s1, pos, rlen, scratch)
				if k1 > 0 {
					msum, mmin, mmax := oa.morsel_agg_f64_u16(msg.col_a, pos, scratch, k1)
					sub.sum   += msum
					sub.count += k1
					if sub.valid {
						if sub.min_val != sub.min_val || mmin < sub.min_val { sub.min_val = mmin }
						if sub.max_val != sub.max_val || mmax > sub.max_val { sub.max_val = mmax }
					} else {
						sub.min_val = mmin
						sub.max_val = mmax
						sub.valid   = true
					}
				}
			}
		} else {
			_worker_agg_where_fallback_range(msg, step, &sub)
		}

	case .Dict_Counts:
		_worker_dict_counts_range(st, msg, step, &sub)

	case .Dict_Group_Sum:
		_worker_dict_group_sum_range(st, msg, step, &sub)
	}

	// Each worker actor has a unique `actor_idx` executed by at most one thread at a
	// time, preserving strict SPSC single-producer ownership of `out_box.lanes[actor_idx]`.
	actor.sharded_spsc_push(st.out_box, st.actor_idx, sub)
}

// Initialize a persistent Sleipnir actor pipeline sized to `num_workers` (defaults
// to `oa.hardware_config_get().physical_cores` when `<= 0`). Call once before trials
// so steady-state execution allocates zero heap memory.
pipeline_init :: proc(p: ^Morsel_Pipeline, num_workers := 0, allocator := context.allocator) -> bool {
	if p == nil { return false }
	p^ = {}
	hw := oa.hardware_config_get()
	nw := num_workers if num_workers > 0 else max(hw.physical_cores, 1)

	sched, s_err := new(actor.Scheduler, allocator)
	if s_err != .None || sched == nil { return false }

	cfg := actor.Scheduler_Config{
		num_workers    = nw,
		num_shards     = nw,
		batch_quota    = 64,
		enable_parking = true,
	}
	if actor.scheduler_init(sched, cfg, allocator) != .None {
		free(sched, allocator)
		return false
	}

	work_pools, wp_err := make([]actor.Envelope_Pool(Morsel_Work), 1, allocator)
	if wp_err != .None {
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}
	if actor.pool_init(&work_pools[0], 0, WORK_POOL_CAP, allocator, heap_fallback = true) != .None {
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}

	if actor.sharded_spsc_init(&p.subtotal_box, nw, allocator) != .None {
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}

	workers, w_err    := make([]^actor.Actor(_Worker_State, Morsel_Work), nw, allocator)
	scratches, sc_err := make([][MORSEL_MAX_ROWS]u16, nw, allocator)
	dbufs, db_err     := make([][DISPATCH_BATCH_SIZE]Morsel_Work, nw, allocator)
	dcnt, dc_err      := make([]int, nw, allocator)
	if w_err != .None || sc_err != .None || db_err != .None || dc_err != .None {
		delete(workers, allocator)
		delete(scratches, allocator)
		delete(dbufs, allocator)
		delete(dcnt, allocator)
		actor.sharded_spsc_destroy(&p.subtotal_box)
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}

	sort_pools, sp_err := make([]actor.Envelope_Pool(Sort_Bucket_Work), 1, allocator)
	if sp_err != .None {
		delete(workers, allocator)
		delete(scratches, allocator)
		delete(dbufs, allocator)
		delete(dcnt, allocator)
		actor.sharded_spsc_destroy(&p.subtotal_box)
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}
	if actor.pool_init(&sort_pools[0], 0, WORK_POOL_CAP, allocator, heap_fallback = true) != .None {
		delete(sort_pools, allocator)
		delete(workers, allocator)
		delete(scratches, allocator)
		delete(dbufs, allocator)
		delete(dcnt, allocator)
		actor.sharded_spsc_destroy(&p.subtotal_box)
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}
	if actor.sharded_spsc_init(&p.sort_done_box, nw, allocator) != .None {
		actor.pool_destroy(&sort_pools[0])
		delete(sort_pools, allocator)
		delete(workers, allocator)
		delete(scratches, allocator)
		delete(dbufs, allocator)
		delete(dcnt, allocator)
		actor.sharded_spsc_destroy(&p.subtotal_box)
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}

	s_workers, sw_err := make([]^actor.Actor(_Sort_Worker_State, Sort_Bucket_Work), nw, allocator)
	s_dbufs, sdb_err  := make([][DISPATCH_BATCH_SIZE]Sort_Bucket_Work, nw, allocator)
	s_dcnt, sdc_err   := make([]int, nw, allocator)
	s_hists, sh_err   := make([][256]u32, nw, allocator)
	s_offs, so_err    := make([][256]u32, nw, allocator)
	if sw_err != .None || sdb_err != .None || sdc_err != .None || sh_err != .None || so_err != .None {
		delete(s_workers, allocator)
		delete(s_dbufs, allocator)
		delete(s_dcnt, allocator)
		delete(s_hists, allocator)
		delete(s_offs, allocator)
		actor.sharded_spsc_destroy(&p.sort_done_box)
		actor.pool_destroy(&sort_pools[0])
		delete(sort_pools, allocator)
		delete(workers, allocator)
		delete(scratches, allocator)
		delete(dbufs, allocator)
		delete(dcnt, allocator)
		actor.sharded_spsc_destroy(&p.subtotal_box)
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}

	d_cnts, dc2_err := make([]i64, nw * DICT_INITIAL_SCRATCH_CAP, allocator)
	d_sums, ds2_err := make([]f64, nw * DICT_INITIAL_SCRATCH_CAP, allocator)
	if dc2_err != .None || ds2_err != .None {
		delete(d_cnts, allocator)
		delete(d_sums, allocator)
		delete(s_workers, allocator)
		delete(s_dbufs, allocator)
		delete(s_dcnt, allocator)
		delete(s_hists, allocator)
		delete(s_offs, allocator)
		actor.sharded_spsc_destroy(&p.sort_done_box)
		actor.pool_destroy(&sort_pools[0])
		delete(sort_pools, allocator)
		delete(workers, allocator)
		delete(scratches, allocator)
		delete(dbufs, allocator)
		delete(dcnt, allocator)
		actor.sharded_spsc_destroy(&p.subtotal_box)
		actor.pool_destroy(&work_pools[0])
		delete(work_pools, allocator)
		actor.scheduler_stop_and_destroy(sched)
		free(sched, allocator)
		return false
	}

	p.sched                = sched
	p.work_pools           = work_pools
	p.workers              = workers
	p.scratches            = scratches
	p.dispatch_bufs        = dbufs
	p.dispatch_counts      = dcnt
	p.dict_worker_counts   = d_cnts
	p.dict_worker_sums     = d_sums
	p.dict_scratch_stride  = DICT_INITIAL_SCRATCH_CAP
	p.sort_pools           = sort_pools
	p.sort_workers         = s_workers
	p.sort_dispatch_bufs   = s_dbufs
	p.sort_dispatch_counts = s_dcnt
	p.sort_worker_hists    = s_hists
	p.sort_worker_offs     = s_offs
	p.num_workers          = nw
	p.allocator            = allocator

	for i in 0..<nw {
		w_actor, a_err := actor.spawn_sched_actor_pooled(
			sched,
			_Worker_State{
				actor_idx = i,
				scratch   = &p.scratches[i],
				out_box   = &p.subtotal_box,
				pipeline  = p,
			},
			_worker_receive,
			p.work_pools,
			u32(i),
			allocator,
		)
		if a_err != .None || w_actor == nil {
			pipeline_destroy(p)
			return false
		}
		p.workers[i] = w_actor

		sw_actor, sa_err := actor.spawn_sched_actor_pooled(
			sched,
			_Sort_Worker_State{
				actor_idx = i,
				hist      = &p.sort_worker_hists[i],
				offs      = &p.sort_worker_offs[i],
				out_box   = &p.sort_done_box,
			},
			_sort_worker_receive,
			p.sort_pools,
			u32(i),
			allocator,
		)
		if sa_err != .None || sw_actor == nil {
			pipeline_destroy(p)
			return false
		}
		p.sort_workers[i] = sw_actor
	}

	actor.scheduler_start(sched)
	p.initialized = true
	return true
}

// Stop all Sleipnir scheduler workers and free all pipeline pools/buffers.
pipeline_destroy :: proc(p: ^Morsel_Pipeline) {
	if p == nil { return }
	alloc := p.allocator
	if p.sched != nil {
		actor.scheduler_wait_idle(p.sched)
		actor.scheduler_stop_and_destroy(p.sched)
		free(p.sched, alloc)
	}
	if len(p.work_pools) > 0 {
		for &wp in p.work_pools {
			actor.pool_destroy(&wp)
		}
		delete(p.work_pools, alloc)
	}
	if len(p.sort_pools) > 0 {
		for &sp in p.sort_pools {
			actor.pool_destroy(&sp)
		}
		delete(p.sort_pools, alloc)
	}
	actor.sharded_spsc_destroy(&p.subtotal_box)
	actor.sharded_spsc_destroy(&p.sort_done_box)
	if p.workers != nil {
		delete(p.workers, alloc)
	}
	if p.scratches != nil {
		delete(p.scratches, alloc)
	}
	if p.dispatch_bufs != nil {
		delete(p.dispatch_bufs, alloc)
	}
	if p.dispatch_counts != nil {
		delete(p.dispatch_counts, alloc)
	}
	if p.dict_worker_counts != nil {
		delete(p.dict_worker_counts, alloc)
	}
	if p.dict_worker_sums != nil {
		delete(p.dict_worker_sums, alloc)
	}
	if p.sort_workers != nil {
		delete(p.sort_workers, alloc)
	}
	if p.sort_dispatch_bufs != nil {
		delete(p.sort_dispatch_bufs, alloc)
	}
	if p.sort_dispatch_counts != nil {
		delete(p.sort_dispatch_counts, alloc)
	}
	if p.sort_worker_hists != nil {
		delete(p.sort_worker_hists, alloc)
	}
	if p.sort_worker_offs != nil {
		delete(p.sort_worker_offs, alloc)
	}
	if p.sort_entries_a != nil {
		delete(p.sort_entries_a, alloc)
	}
	if p.sort_entries_b != nil {
		delete(p.sort_entries_b, alloc)
	}
	if p.str_entries_a != nil {
		delete(p.str_entries_a, alloc)
	}
	if p.str_entries_b != nil {
		delete(p.str_entries_b, alloc)
	}
	p^ = {}
}

@(private="file")
_enqueue_morsel_work :: proc(
	p:             ^Morsel_Pipeline,
	w:             int,
	work:          Morsel_Work,
	agg:           ^_Agg_State,
	total_morsels: ^int,
	received:      ^int,
) {
	if total_morsels^ - received^ >= SPSC_LANE_CAP - 64 {
		for k in 0..<p.num_workers {
			k_cnt := p.dispatch_counts[k]
			if k_cnt > 0 {
				actor.send_batch_from_pool(p.sched, &p.work_pools[0], p.workers[k], p.dispatch_bufs[k][:k_cnt], k)
				p.dispatch_counts[k] = 0
			}
		}
		for total_morsels^ - received^ >= SPSC_LANE_CAP / 2 {
			got := actor.sharded_spsc_consume(&p.subtotal_box, 128, agg, _agg_subtotal_handler)
			if got > 0 {
				received^ += got
			} else {
				intrinsics.cpu_relax()
			}
		}
	}
	m_idx := total_morsels^
	total_morsels^ = m_idx + 1
	msg := work
	msg.morsel_idx = m_idx
	cnt := p.dispatch_counts[w]
	p.dispatch_bufs[w][cnt] = msg
	cnt += 1
	if cnt == len(p.dispatch_bufs[w]) {
		actor.send_batch_from_pool(p.sched, &p.work_pools[0], p.workers[w], p.dispatch_bufs[w][:cnt], w)
		p.dispatch_counts[w] = 0
		received^ += actor.sharded_spsc_consume(&p.subtotal_box, 128, agg, _agg_subtotal_handler)
	} else {
		p.dispatch_counts[w] = cnt
	}
}

@(private="file")
_dispatch_columns :: proc(
	p:             ^Morsel_Pipeline,
	col_a:         ^oa.Array,
	op1:           oa.Compare_Op,
	s1:            f64,
	col_b:         ^oa.Array,
	op2:           oa.Compare_Op,
	s2:            f64,
	agg:           ^_Agg_State,
	total_morsels: ^int,
	received:      ^int,
) {
	n := col_a.length
	if n <= 0 { return }
	hw                := oa.hardware_config_get()
	bpr               := max(oa.type_byte_width(col_a.type) + oa.type_byte_width(col_b.type), 8)
	sub_morsel_rows   := clamp((hw.morsel_target_bytes / bpr + 63) &~ 63, 4096, MORSEL_MAX_ROWS)
	super_morsel_rows := sub_morsel_rows * SUPER_MORSEL_MULTIPLIER
	n_morsels         := (n + super_morsel_rows - 1) / super_morsel_rows
	nw                := p.num_workers

	for local_idx := 0; local_idx < n_morsels; local_idx += 1 {
		from := local_idx * super_morsel_rows
		to   := min(from + super_morsel_rows, n)
		w    := total_morsels^ % nw
		_enqueue_morsel_work(p, w, Morsel_Work{
			kind            = .Sel_2Pred_Sum,
			col_a           = col_a,
			col_b           = col_b,
			row_start       = from,
			row_len         = to - from,
			sub_morsel_rows = sub_morsel_rows,
			op1             = op1,
			s1              = s1,
			op2             = op2,
			s2              = s2,
		}, agg, total_morsels, received)
	}
}

@(private="file")
_finish_dispatch :: proc(p: ^Morsel_Pipeline, agg: ^_Agg_State, total_morsels: int, received: ^int) {
	for w in 0..<p.num_workers {
		cnt := p.dispatch_counts[w]
		if cnt > 0 {
			actor.send_batch_from_pool(p.sched, &p.work_pools[0], p.workers[w], p.dispatch_bufs[w][:cnt], w)
			p.dispatch_counts[w] = 0
		}
	}
	for received^ < total_morsels {
		got := actor.sharded_spsc_consume(&p.subtotal_box, 256, agg, _agg_subtotal_handler)
		if got > 0 {
			received^ += got
		} else {
			intrinsics.cpu_relax()
		}
	}
	actor.scheduler_wait_idle(p.sched)
}

// Execute `sum(col_a) WHERE col_a <op1> s1 AND col_b <op2> s2` over L2-resident
// morsels dispatched via `actor.send_batch_from_pool` and aggregated through
// `Sharded_SPSC_Mailbox` with 0 heap allocations in steady state.
pipeline_sel_2pred_sum_f64 :: proc(
	p:     ^Morsel_Pipeline,
	col_a: ^oa.Array,
	op1:   oa.Compare_Op,
	s1:    f64,
	col_b: ^oa.Array,
	op2:   oa.Compare_Op,
	s2:    f64,
) -> (sum: f64, valid_count: int, ok: bool) {
	if p == nil || !p.initialized || col_a == nil || col_b == nil || col_a.length != col_b.length {
		return 0, 0, false
	}
	if col_a.length == 0 { return 0, 0, true }

	agg           := _Agg_State{}
	total_morsels := 0
	received      := 0
	_dispatch_columns(p, col_a, op1, s1, col_b, op2, s2, &agg, &total_morsels, &received)
	_finish_dispatch(p, &agg, total_morsels, &received)
	return agg.sum, agg.valid_count, true
}

// Execute the 2-predicate filtered sum across a stream of Record_Batches concurrently
// in a single pipelined wave.
pipeline_record_batch_stream_sum :: proc(
	p:         ^Morsel_Pipeline,
	batches:   []oa.Record_Batch,
	col_a_idx: int,
	op1:       oa.Compare_Op,
	s1:        f64,
	col_b_idx: int,
	op2:       oa.Compare_Op,
	s2:        f64,
) -> (sum: f64, valid_count: int, ok: bool) {
	if p == nil || !p.initialized { return 0, 0, false }
	for &rb in batches {
		if col_a_idx < 0 || col_a_idx >= len(rb.columns) || col_b_idx < 0 || col_b_idx >= len(rb.columns) {
			return 0, 0, false
		}
		if rb.columns[col_a_idx].length != rb.columns[col_b_idx].length {
			return 0, 0, false
		}
	}

	agg           := _Agg_State{}
	total_morsels := 0
	received      := 0
	for &rb in batches {
		_dispatch_columns(
			p,
			&rb.columns[col_a_idx], op1, s1,
			&rb.columns[col_b_idx], op2, s2,
			&agg, &total_morsels, &received,
		)
	}
	_finish_dispatch(p, &agg, total_morsels, &received)
	return agg.sum, agg.valid_count, true
}

// Execute fused `sum`, `count`, `min`, `max` over `values` where `pred <op> scalar`
// in one Sleipnir super-morsel actor pass with 0 steady-state heap allocations.
pipeline_agg_where_f64 :: proc(
	p:      ^Morsel_Pipeline,
	values: ^oa.Array,
	pred:   ^oa.Array,
	op:     oa.Compare_Op,
	scalar: f64,
) -> (aggs: oa.Aggregates, ok: bool) {
	if p == nil || !p.initialized || values == nil || pred == nil || values.length != pred.length || values.type != pred.type {
		return {}, false
	}
	#partial switch _ in values.type {
	case oa.Null_Type, oa.Bool_Type, oa.String_Type, oa.Large_String_Type, oa.Binary_Type, oa.Large_Binary_Type:
		return {}, false
	}
	n := values.length
	if n == 0 {
		return {}, true
	}

	hw                := oa.hardware_config_get()
	bpr               := values == pred ? max(oa.type_byte_width(values.type), 8) : max(oa.type_byte_width(values.type) + oa.type_byte_width(pred.type), 8)
	sub_morsel_rows   := clamp((hw.morsel_target_bytes / bpr + 63) &~ 63, 4096, MORSEL_MAX_ROWS)
	nw                := p.num_workers
	target_super      := sub_morsel_rows * SUPER_MORSEL_MULTIPLIER
	even_split        := max((n + nw - 1) / nw, 4096)
	super_morsel_rows := min(target_super, even_split)
	n_morsels         := (n + super_morsel_rows - 1) / super_morsel_rows

	was_parking   := _sort_begin_unparked(p)
	agg           := _Agg_State{}
	total_morsels := 0
	received      := 0
	for local_idx := 0; local_idx < n_morsels; local_idx += 1 {
		from := local_idx * super_morsel_rows
		to   := min(from + super_morsel_rows, n)
		w    := local_idx % nw
		_enqueue_morsel_work(p, w, Morsel_Work{
			kind            = .Agg_Where,
			col_a           = values,
			col_b           = pred,
			row_start       = from,
			row_len         = to - from,
			sub_morsel_rows = sub_morsel_rows,
			op1             = op,
			s1              = scalar,
		}, &agg, &total_morsels, &received)
	}
	_finish_dispatch(p, &agg, total_morsels, &received)
	_sort_end_unparked(p, was_parking)

	return oa.Aggregates{
		sum     = agg.sum,
		count   = agg.valid_count,
		min_val = agg.min_val,
		max_val = agg.max_val,
		valid   = agg.valid,
	}, true
}

pipeline_agg_where_compare :: pipeline_agg_where_f64

@(private="file")
_ensure_dict_scratch :: proc(p: ^Morsel_Pipeline, dict_len: int) -> mem.Allocator_Error {
	if dict_len <= p.dict_scratch_stride {
		return .None
	}
	new_stride := max((dict_len + 63) &~ 63, p.dict_scratch_stride * 2)
	new_counts, err_c := make([]i64, p.num_workers * new_stride, p.allocator)
	if err_c != .None {
		return err_c
	}
	new_sums, err_s := make([]f64, p.num_workers * new_stride, p.allocator)
	if err_s != .None {
		delete(new_counts, p.allocator)
		return err_s
	}
	if p.dict_worker_counts != nil {
		delete(p.dict_worker_counts, p.allocator)
	}
	if p.dict_worker_sums != nil {
		delete(p.dict_worker_sums, p.allocator)
	}
	p.dict_worker_counts  = new_counts
	p.dict_worker_sums    = new_sums
	p.dict_scratch_stride = new_stride
	return .None
}

@(private="file")
_dispatch_dict_counts_raw :: proc(
	p:        ^Morsel_Pipeline,
	codes:    []i32,
	dict_len: int,
) -> (active_nw: int, valid_total: int, ok: bool) {
	if p == nil || !p.initialized || dict_len < 0 {
		return 0, 0, false
	}
	n := len(codes)
	if n == 0 || dict_len == 0 {
		return 0, 0, true
	}
	if _ensure_dict_scratch(p, dict_len) != .None {
		return 0, 0, false
	}

	nw                := p.num_workers
	sub_morsel_rows   := MORSEL_MAX_ROWS
	target_super      := sub_morsel_rows * SUPER_MORSEL_MULTIPLIER
	even_split        := max((n + nw - 1) / nw, 4096)
	super_morsel_rows := min(target_super, even_split)
	n_morsels         := (n + super_morsel_rows - 1) / super_morsel_rows
	active_nw          = min(nw, n_morsels)

	stride := p.dict_scratch_stride
	for w in 0..<active_nw {
		w_counts := p.dict_worker_counts[w * stride : w * stride + dict_len]
		for k in 0..<dict_len {
			w_counts[k] = 0
		}
	}

	was_parking   := _sort_begin_unparked(p)
	agg           := _Agg_State{}
	total_morsels := 0
	received      := 0
	codes_ptr     := raw_data(codes)

	for local_idx := 0; local_idx < n_morsels; local_idx += 1 {
		from := local_idx * super_morsel_rows
		to   := min(from + super_morsel_rows, n)
		w    := local_idx % active_nw
		_enqueue_morsel_work(p, w, Morsel_Work{
			kind            = .Dict_Counts,
			codes_ptr       = codes_ptr,
			dict_len        = dict_len,
			row_start       = from,
			row_len         = to - from,
			sub_morsel_rows = sub_morsel_rows,
		}, &agg, &total_morsels, &received)
	}
	_finish_dispatch(p, &agg, total_morsels, &received)
	_sort_end_unparked(p, was_parking)

	return active_nw, agg.valid_count, true
}

// Accumulate parallel dictionary value counts for raw `codes` into caller-owned
// `out_counts` using worker-private L1d-resident histograms (0 steady-state allocs).
pipeline_codes_value_counts_into :: proc(
	p:          ^Morsel_Pipeline,
	codes:      []i32,
	dict_len:   int,
	out_counts: []i64,
) -> (null_count: int, ok: bool) {
	if p == nil || !p.initialized || dict_len < 0 || len(out_counts) < dict_len {
		return 0, false
	}
	for k in 0..<dict_len {
		out_counts[k] = 0
	}
	n := len(codes)
	if n == 0 || dict_len == 0 {
		return n, true
	}

	active_nw, valid_total, raw_ok := _dispatch_dict_counts_raw(p, codes, dict_len)
	if !raw_ok {
		return 0, false
	}

	stride := p.dict_scratch_stride
	for w in 0..<active_nw {
		w_counts := p.dict_worker_counts[w * stride : w * stride + dict_len]
		for k in 0..<dict_len {
			out_counts[k] += w_counts[k]
		}
	}
	return n - valid_total, true
}

pipeline_str_dict_value_counts_into :: proc(
	p:          ^Morsel_Pipeline,
	d:          ^oa.Str_Dict_Array,
	out_counts: []i64,
) -> (null_count: int, ok: bool) {
	if d == nil { return 0, false }
	return pipeline_codes_value_counts_into(p, d.codes[:d.length], d.dict.length, out_counts)
}

pipeline_num_dict_value_counts_into :: proc(
	p:          ^Morsel_Pipeline,
	d:          ^oa.Dict_Array($T),
	out_counts: []i64,
) -> (null_count: int, ok: bool) {
	if d == nil { return 0, false }
	return pipeline_codes_value_counts_into(p, d.codes[:d.length], len(d.dictionary), out_counts)
}

pipeline_codes_value_counts :: proc(
	p:         ^Morsel_Pipeline,
	codes:     []i32,
	dict_len:  int,
	allocator := context.allocator,
) -> (counts: []i64, null_count: int, ok: bool) {
	if p == nil || !p.initialized || dict_len < 0 {
		return nil, 0, false
	}
	out, alloc_err := make([]i64, dict_len, allocator)
	if alloc_err != .None {
		return nil, 0, false
	}
	nc, run_ok := pipeline_codes_value_counts_into(p, codes, dict_len, out)
	if !run_ok {
		delete(out, allocator)
		return nil, 0, false
	}
	return out, nc, true
}

pipeline_str_dict_value_counts :: proc(
	p:         ^Morsel_Pipeline,
	d:         ^oa.Str_Dict_Array,
	allocator := context.allocator,
) -> (counts: []i64, null_count: int, ok: bool) {
	if d == nil { return nil, 0, false }
	return pipeline_codes_value_counts(p, d.codes[:d.length], d.dict.length, allocator)
}

pipeline_num_dict_value_counts :: proc(
	p:         ^Morsel_Pipeline,
	d:         ^oa.Dict_Array($T),
	allocator := context.allocator,
) -> (counts: []i64, null_count: int, ok: bool) {
	if d == nil { return nil, 0, false }
	return pipeline_codes_value_counts(p, d.codes[:d.length], len(d.dictionary), allocator)
}

pipeline_dict_value_counts :: proc{
	pipeline_str_dict_value_counts,
	pipeline_num_dict_value_counts,
	pipeline_codes_value_counts,
}

pipeline_dict_value_counts_into :: proc{
	pipeline_str_dict_value_counts_into,
	pipeline_num_dict_value_counts_into,
	pipeline_codes_value_counts_into,
}

// Compute parallel sum over numeric `Dict_Array(T)` using worker-private histograms
// with 0 steady-state heap allocations.
pipeline_dict_sum :: proc(p: ^Morsel_Pipeline, d: ^oa.Dict_Array($T)) -> (sum: f64, null_count: int, ok: bool) {
	if p == nil || !p.initialized || d == nil {
		return 0, 0, false
	}
	dlen := len(d.dictionary)
	n    := d.length
	if n == 0 || dlen == 0 {
		return 0, n, true
	}

	active_nw, valid_total, raw_ok := _dispatch_dict_counts_raw(p, d.codes[:n], dlen)
	if !raw_ok {
		return 0, 0, false
	}

	stride    := p.dict_scratch_stride
	w0_counts := p.dict_worker_counts[:dlen]
	for w in 1..<active_nw {
		w_counts := p.dict_worker_counts[w * stride : w * stride + dlen]
		for k in 0..<dlen {
			w0_counts[k] += w_counts[k]
		}
	}
	for v, ci in d.dictionary {
		c := w0_counts[ci]
		if c > 0 {
			sum += f64(v) * f64(c)
		}
	}
	return sum, n - valid_total, true
}

// Accumulate parallel dictionary group-by sums into caller-owned `out_sums` using
// contention-free worker-private f64 tables (0 steady-state heap allocations).
pipeline_codes_group_sum_into :: proc(
	p:        ^Morsel_Pipeline,
	codes:    []i32,
	dict_len: int,
	weights:  ^oa.Array,
	out_sums: []f64,
) -> (null_count: int, ok: bool) {
	if p == nil || !p.initialized || weights == nil || dict_len < 0 || len(out_sums) < dict_len {
		return 0, false
	}
	#partial switch _ in weights.type {
	case oa.Null_Type, oa.Bool_Type, oa.String_Type, oa.Large_String_Type, oa.Binary_Type, oa.Large_Binary_Type:
		return 0, false
	}
	for k in 0..<dict_len {
		out_sums[k] = 0
	}
	n := min(len(codes), weights.length)
	if n == 0 || dict_len == 0 {
		return n, true
	}
	if _ensure_dict_scratch(p, dict_len) != .None {
		return 0, false
	}

	nw                := p.num_workers
	sub_morsel_rows   := MORSEL_MAX_ROWS
	target_super      := sub_morsel_rows * SUPER_MORSEL_MULTIPLIER
	even_split        := max((n + nw - 1) / nw, 4096)
	super_morsel_rows := min(target_super, even_split)
	n_morsels         := (n + super_morsel_rows - 1) / super_morsel_rows
	active_nw         := min(nw, n_morsels)

	stride := p.dict_scratch_stride
	for w in 0..<active_nw {
		w_sums := p.dict_worker_sums[w * stride : w * stride + dict_len]
		for k in 0..<dict_len {
			w_sums[k] = 0
		}
	}

	was_parking   := _sort_begin_unparked(p)
	agg           := _Agg_State{}
	total_morsels := 0
	received      := 0
	codes_ptr     := raw_data(codes)

	for local_idx := 0; local_idx < n_morsels; local_idx += 1 {
		from := local_idx * super_morsel_rows
		to   := min(from + super_morsel_rows, n)
		w    := local_idx % active_nw
		_enqueue_morsel_work(p, w, Morsel_Work{
			kind            = .Dict_Group_Sum,
			col_a           = weights,
			codes_ptr       = codes_ptr,
			dict_len        = dict_len,
			row_start       = from,
			row_len         = to - from,
			sub_morsel_rows = sub_morsel_rows,
		}, &agg, &total_morsels, &received)
	}
	_finish_dispatch(p, &agg, total_morsels, &received)
	_sort_end_unparked(p, was_parking)

	for w in 0..<active_nw {
		w_sums := p.dict_worker_sums[w * stride : w * stride + dict_len]
		for k in 0..<dict_len {
			out_sums[k] += w_sums[k]
		}
	}
	return n - agg.valid_count, true
}

pipeline_str_dict_group_sum_into :: proc(
	p:        ^Morsel_Pipeline,
	d:        ^oa.Str_Dict_Array,
	weights:  ^oa.Array,
	out_sums: []f64,
) -> (null_count: int, ok: bool) {
	if d == nil { return 0, false }
	return pipeline_codes_group_sum_into(p, d.codes[:d.length], d.dict.length, weights, out_sums)
}

pipeline_num_dict_group_sum_into :: proc(
	p:        ^Morsel_Pipeline,
	d:        ^oa.Dict_Array($T),
	weights:  ^oa.Array,
	out_sums: []f64,
) -> (null_count: int, ok: bool) {
	if d == nil { return 0, false }
	return pipeline_codes_group_sum_into(p, d.codes[:d.length], len(d.dictionary), weights, out_sums)
}

pipeline_codes_group_sum :: proc(
	p:         ^Morsel_Pipeline,
	codes:     []i32,
	dict_len:  int,
	weights:   ^oa.Array,
	allocator := context.allocator,
) -> (sums: []f64, null_count: int, ok: bool) {
	if p == nil || !p.initialized || weights == nil || dict_len < 0 {
		return nil, 0, false
	}
	#partial switch _ in weights.type {
	case oa.Null_Type, oa.Bool_Type, oa.String_Type, oa.Large_String_Type, oa.Binary_Type, oa.Large_Binary_Type:
		return nil, 0, false
	}
	out, alloc_err := make([]f64, dict_len, allocator)
	if alloc_err != .None {
		return nil, 0, false
	}
	nc, run_ok := pipeline_codes_group_sum_into(p, codes, dict_len, weights, out)
	if !run_ok {
		delete(out, allocator)
		return nil, 0, false
	}
	return out, nc, true
}

pipeline_str_dict_group_sum :: proc(
	p:         ^Morsel_Pipeline,
	d:         ^oa.Str_Dict_Array,
	weights:   ^oa.Array,
	allocator := context.allocator,
) -> (sums: []f64, null_count: int, ok: bool) {
	if d == nil { return nil, 0, false }
	return pipeline_codes_group_sum(p, d.codes[:d.length], d.dict.length, weights, allocator)
}

pipeline_num_dict_group_sum :: proc(
	p:         ^Morsel_Pipeline,
	d:         ^oa.Dict_Array($T),
	weights:   ^oa.Array,
	allocator := context.allocator,
) -> (sums: []f64, null_count: int, ok: bool) {
	if d == nil { return nil, 0, false }
	return pipeline_codes_group_sum(p, d.codes[:d.length], len(d.dictionary), weights, allocator)
}

pipeline_dict_group_sum :: proc{
	pipeline_str_dict_group_sum,
	pipeline_num_dict_group_sum,
	pipeline_codes_group_sum,
}

pipeline_dict_group_sum_into :: proc{
	pipeline_str_dict_group_sum_into,
	pipeline_num_dict_group_sum_into,
	pipeline_codes_group_sum_into,
}

// ── Sleipnir Actor MSB-Bucket Parallel Sort ──────────────────────────────────

@(private="file")
_MAX_SORT_BUCKET_JOBS :: 2048

@(private="file")
_Sort_Bucket_Job :: struct {
	start:     int,
	end:       int,
	rem_mask:  u32,
	src_is_a:  bool,
}

@(private="file")
_Sort_Done_Acc :: struct {
	received: int,
}

@(private="file")
_sort_done_handler :: proc(_: ^_Sort_Done_Acc, _: ^Sort_Bucket_Done) {}

@(private="file")
_top_set_bit_u32 :: #force_inline proc "contextless" (v: u32) -> int {
	x := v
	r := 0
	if x >= 1 << 16 { x >>= 16; r += 16 }
	if x >= 1 << 8  { x >>= 8;  r += 8 }
	if x >= 1 << 4  { x >>= 4;  r += 4 }
	if x >= 1 << 2  { x >>= 2;  r += 2 }
	if x >= 1 << 1  { r += 1 }
	return r
}

@(private="file")
_compute_diff_mask_u32 :: proc "contextless" (raw: [^]u32, n: int) -> u32 {
	if n <= 1 { return 0 }
	k0 := raw[0]
	d0, d1, d2, d3: u32 = 0, 0, 0, 0
	i := 1
	for i + 4 <= n {
		d0 |= raw[i]     ~ k0
		d1 |= raw[i + 1] ~ k0
		d2 |= raw[i + 2] ~ k0
		d3 |= raw[i + 3] ~ k0
		i += 4
	}
	diff := d0 | d1 | d2 | d3
	for i < n {
		diff |= raw[i] ~ k0
		i += 1
	}
	return diff
}

@(private="file")
_compute_diff_mask_u64_key :: proc "contextless" (entries: []u64, mask: u32) -> u32 {
	m := len(entries)
	if m <= 1 { return 0 }
	k0 := u32(entries[0] >> 32)
	d0, d1, d2, d3: u32 = 0, 0, 0, 0
	i := 1
	for i + 4 <= m {
		d0 |= u32(entries[i]     >> 32) ~ k0
		d1 |= u32(entries[i + 1] >> 32) ~ k0
		d2 |= u32(entries[i + 2] >> 32) ~ k0
		d3 |= u32(entries[i + 3] >> 32) ~ k0
		i += 4
	}
	diff := d0 | d1 | d2 | d3
	for i < m {
		diff |= u32(entries[i] >> 32) ~ k0
		i += 1
	}
	return diff & mask
}

@(private="file")
_ensure_u64_sort_scratch :: proc(p: ^Morsel_Pipeline, n: int) -> mem.Allocator_Error {
	if len(p.sort_entries_a) >= n && len(p.sort_entries_b) >= n {
		return .None
	}
	new_cap := max(n, max(len(p.sort_entries_a) * 2, SORT_PARALLEL_PARTITION_MIN))
	if p.sort_entries_a != nil {
		delete(p.sort_entries_a, p.allocator)
		p.sort_entries_a = nil
	}
	if p.sort_entries_b != nil {
		delete(p.sort_entries_b, p.allocator)
		p.sort_entries_b = nil
	}
	a, err_a := make([]u64, new_cap, p.allocator)
	if err_a != .None {
		return err_a
	}
	b, err_b := make([]u64, new_cap, p.allocator)
	if err_b != .None {
		delete(a, p.allocator)
		return err_b
	}
	p.sort_entries_a = a
	p.sort_entries_b = b
	return .None
}

@(private="file")
_ensure_str_sort_scratch :: proc(p: ^Morsel_Pipeline, n: int) -> mem.Allocator_Error {
	if len(p.str_entries_a) >= n && len(p.str_entries_b) >= n {
		return .None
	}
	new_cap := max(n, max(len(p.str_entries_a) * 2, SORT_PARALLEL_PARTITION_MIN))
	if p.str_entries_a != nil {
		delete(p.str_entries_a, p.allocator)
		p.str_entries_a = nil
	}
	if p.str_entries_b != nil {
		delete(p.str_entries_b, p.allocator)
		p.str_entries_b = nil
	}
	a, err_a := make([]oa.Str_Sort_Entry, new_cap, p.allocator)
	if err_a != .None {
		return err_a
	}
	b, err_b := make([]oa.Str_Sort_Entry, new_cap, p.allocator)
	if err_b != .None {
		delete(a, p.allocator)
		return err_b
	}
	p.str_entries_a = a
	p.str_entries_b = b
	return .None
}

@(private="file")
_sort_send_one :: proc(p: ^Morsel_Pipeline, w: int, msg: Sort_Bucket_Work, acc: ^_Sort_Done_Acc, sent: ^int) {
	if sent^ - acc.received >= SPSC_LANE_CAP - 64 {
		for k in 0..<p.num_workers {
			k_cnt := p.sort_dispatch_counts[k]
			if k_cnt > 0 {
				actor.send_batch_from_pool(p.sched, &p.sort_pools[0], p.sort_workers[k], p.sort_dispatch_bufs[k][:k_cnt], k)
				p.sort_dispatch_counts[k] = 0
			}
		}
		for sent^ - acc.received >= SPSC_LANE_CAP / 2 {
			got := actor.sharded_spsc_consume(&p.sort_done_box, 128, acc, _sort_done_handler)
			if got > 0 {
				acc.received += got
			} else {
				intrinsics.cpu_relax()
			}
		}
	}
	cnt := p.sort_dispatch_counts[w]
	p.sort_dispatch_bufs[w][cnt] = msg
	cnt += 1
	sent^ += 1
	if cnt == len(p.sort_dispatch_bufs[w]) {
		actor.send_batch_from_pool(p.sched, &p.sort_pools[0], p.sort_workers[w], p.sort_dispatch_bufs[w][:cnt], w)
		p.sort_dispatch_counts[w] = 0
		acc.received += actor.sharded_spsc_consume(&p.sort_done_box, 128, acc, _sort_done_handler)
	} else {
		p.sort_dispatch_counts[w] = cnt
	}
}

@(private="file")
_sort_begin_unparked :: proc(p: ^Morsel_Pipeline) -> bool {
	if p == nil || p.sched == nil { return false }
	prev := intrinsics.atomic_exchange_explicit(&p.sched.enable_parking, false, .Seq_Cst)
	if prev {
		intrinsics.atomic_add_explicit((^u32)(&p.sched.wake_futex), 1, .Seq_Cst)
		sync.futex_broadcast(&p.sched.wake_futex)
	}
	return prev
}

@(private="file")
_sort_end_unparked :: proc(p: ^Morsel_Pipeline, was_parking: bool) {
	if p == nil || p.sched == nil { return }
	actor.scheduler_wait_idle(p.sched)
	if was_parking {
		intrinsics.atomic_store_explicit(&p.sched.enable_parking, true, .Seq_Cst)
	}
}

@(private="file")
_sort_scatter_workers :: #force_inline proc "contextless" (p: ^Morsel_Pipeline, n: int) -> int {
	return min(p.num_workers, clamp(n / 16_384, 2, 8))
}

@(private="file")
_sort_flush_and_wait :: proc(p: ^Morsel_Pipeline, acc: ^_Sort_Done_Acc, total_sent: int) {
	for w in 0..<p.num_workers {
		cnt := p.sort_dispatch_counts[w]
		if cnt > 0 {
			actor.send_batch_from_pool(p.sched, &p.sort_pools[0], p.sort_workers[w], p.sort_dispatch_bufs[w][:cnt], w)
			p.sort_dispatch_counts[w] = 0
		}
	}
	for acc.received < total_sent {
		got := actor.sharded_spsc_consume(&p.sort_done_box, 256, acc, _sort_done_handler)
		if got > 0 {
			acc.received += got
		} else {
			intrinsics.cpu_relax()
		}
	}
}

@(private="file")
_prefix_sum_sort_buckets :: proc(
	p:           ^Morsel_Pipeline,
	nw:          int,
	base_start:  u32,
	bucket_offs: ^[256]u32,
	bucket_cnts: ^[256]u32,
) {
	acc := base_start
	for b in 0..<256 {
		bucket_offs[b] = acc
		b_sum: u32 = 0
		for w in 0..<nw {
			p.sort_worker_offs[w][b] = acc
			cnt := p.sort_worker_hists[w][b]
			acc += cnt
			b_sum += cnt
		}
		bucket_cnts[b] = b_sum
	}
}

@(private="file")
_sort_worker_receive :: proc(_: ^actor.Context, self: ^actor.Actor(_Sort_Worker_State, Sort_Bucket_Work), work: ^Sort_Bucket_Work) {
	state := &self.state
	s := work.start
	e := work.end

	switch work.kind {
	case .Count_U32_Signed:
		hist := state.hist
		hist^ = {}
		shift := work.shift
		raw := work.raw_u32
		for i in s..<e {
			k := raw[i] ~ 0x8000_0000
			hist[(k >> shift) & 0xFF] += 1
		}

	case .Count_U32_Unsigned:
		hist := state.hist
		hist^ = {}
		shift := work.shift
		raw := work.raw_u32
		for i in s..<e {
			hist[(raw[i] >> shift) & 0xFF] += 1
		}

	case .Scatter_U32_Signed:
		offs := state.offs
		shift := work.shift
		raw := work.raw_u32
		dst := work.entries_dst
		for i in s..<e {
			k := raw[i] ~ 0x8000_0000
			b := (k >> shift) & 0xFF
			pos := offs[b]
			offs[b] = pos + 1
			dst[pos] = (u64(k) << 32) | u64(u32(i))
		}

	case .Scatter_U32_Unsigned:
		offs := state.offs
		shift := work.shift
		raw := work.raw_u32
		dst := work.entries_dst
		for i in s..<e {
			k := raw[i]
			b := (k >> shift) & 0xFF
			pos := offs[b]
			offs[b] = pos + 1
			dst[pos] = (u64(k) << 32) | u64(u32(i))
		}

	case .Count_U64_Key:
		hist := state.hist
		hist^ = {}
		key_shift := u32(32) + work.shift
		src := work.entries_src
		for i in s..<e {
			hist[(src[i] >> key_shift) & 0xFF] += 1
		}

	case .Scatter_U64_Key:
		offs := state.offs
		key_shift := u32(32) + work.shift
		src := work.entries_src
		dst := work.entries_dst
		for i in s..<e {
			v := src[i]
			b := (v >> key_shift) & 0xFF
			pos := offs[b]
			offs[b] = pos + 1
			dst[pos] = v
		}

	case .Sort_Bucket_U64:
		oa.radix_sort_bucket_u64(work.entries_src[s:e], work.entries_dst[s:e], work.out_i64[s:e], work.rem_mask)

	case .Count_Umbra:
		hist := state.hist
		hist^ = {}
		slots := work.umbra_arr.slots
		for i in s..<e {
			hist[slots[i].data[0]] += 1
		}

	case .Scatter_Umbra:
		offs := state.offs
		slots := work.umbra_arr.slots
		dst := work.entries_dst
		for i in s..<e {
			k := oa.umbra_prefix(&slots[i])
			b := (k >> 24) & 0xFF
			pos := offs[b]
			offs[b] = pos + 1
			dst[pos] = (u64(k) << 32) | u64(u32(i))
		}

	case .Sort_Bucket_Umbra:
		oa.umbra_sort_range_into(work.umbra_arr, work.entries_src[s:e], work.entries_dst[s:e], work.out_i64[s:e], work.rem_mask)

	case .Count_Str_Lead:
		hist := state.hist
		hist^ = {}
		offsets := work.str_offsets
		bytes := work.str_bytes
		off := work.str_arr_off
		for i in s..<e {
			si := int(offsets[off + i])
			ei := int(offsets[off + i + 1])
			b := ei > si ? bytes[si] : u8(0)
			hist[b] += 1
		}

	case .Scatter_Str_Lead:
		offs := state.offs
		offsets := work.str_offsets
		bytes := work.str_bytes
		off := work.str_arr_off
		dst := work.str_entries_dst
		for i in s..<e {
			si := int(offsets[off + i])
			ei := int(offsets[off + i + 1])
			b := ei > si ? bytes[si] : u8(0)
			pos := offs[b]
			offs[b] = pos + 1
			dst[pos] = oa.Str_Sort_Entry{
				prefix = oa.str_entry_prefix(bytes, si, ei),
				idx    = i32(i),
				slen   = i32(ei - si),
			}
		}

	case .Count_Str_Byte:
		hist := state.hist
		hist^ = {}
		shift := work.shift
		src := work.str_entries_src
		for i in s..<e {
			hist[(src[i].prefix >> shift) & 0xFF] += 1
		}

	case .Scatter_Str_Byte:
		offs := state.offs
		shift := work.shift
		src := work.str_entries_src
		dst := work.str_entries_dst
		for i in s..<e {
			v := src[i]
			b := (v.prefix >> shift) & 0xFF
			pos := offs[b]
			offs[b] = pos + 1
			dst[pos] = v
		}

	case .Sort_Bucket_Str:
		oa.string_sort_range_into(
			work.str_offsets,
			work.str_bytes,
			work.str_arr_off,
			work.str_entries_src[s:e],
			work.str_entries_dst[s:e],
			work.out_i64[s:e],
			work.rem_mask,
		)
	}

	actor.sharded_spsc_push(state.out_box, state.actor_idx, Sort_Bucket_Done{done = 1})
}

@(private="file")
_run_u64_count_scatter_wave :: proc(
	p:           ^Morsel_Pipeline,
	range_start: int,
	range_end:   int,
	src_buf:     []u64,
	dst_buf:     []u64,
	shift:       u32,
	bucket_offs: ^[256]u32,
	bucket_cnts: ^[256]u32,
) {
	total := range_end - range_start
	nw := _sort_scatter_workers(p, total)
	chunk := (total + nw - 1) / nw

	acc := _Sort_Done_Acc{}
	sent := 0
	for w in 0..<nw {
		cs := range_start + w * chunk
		ce := min(cs + chunk, range_end)
		if cs >= ce {
			p.sort_worker_hists[w] = {}
			continue
		}
		_sort_send_one(p, w, Sort_Bucket_Work{
			kind        = .Count_U64_Key,
			shift       = shift,
			start       = cs,
			end         = ce,
			entries_src = src_buf,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)

	_prefix_sum_sort_buckets(p, nw, u32(range_start), bucket_offs, bucket_cnts)

	acc = {}
	sent = 0
	for w in 0..<nw {
		cs := range_start + w * chunk
		ce := min(cs + chunk, range_end)
		if cs >= ce { continue }
		_sort_send_one(p, w, Sort_Bucket_Work{
			kind        = .Scatter_U64_Key,
			shift       = shift,
			start       = cs,
			end         = ce,
			entries_src = src_buf,
			entries_dst = dst_buf,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)
}

@(private="file")
_collect_u64_bucket_jobs :: proc(
	p:                 ^Morsel_Pipeline,
	bucket_offs:       ^[256]u32,
	bucket_cnts:       ^[256]u32,
	total_n:           int,
	src_is_a:          bool,
	rem_mask:          u32,
	depth:             int,
	jobs:              ^[_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job,
	job_count:         ^int,
	reserved_siblings := 0,
) {
	skew_thresh := max(total_n / 2, SORT_PARALLEL_PARTITION_MIN)
	for b in 0..<256 {
		cnt := int(bucket_cnts[b])
		if cnt == 0 { continue }
		b_start := int(bucket_offs[b])
		b_end   := b_start + cnt
		rem_sib := 255 - b

		if cnt > skew_thresh && rem_mask != 0 && depth < 2 && p.num_workers > 1 &&
		   job_count^ + 256 + reserved_siblings + rem_sib <= _MAX_SORT_BUCKET_JOBS {
			cur_src := src_is_a ? p.sort_entries_a : p.sort_entries_b
			cur_dst := src_is_a ? p.sort_entries_b : p.sort_entries_a
			sub_diff := _compute_diff_mask_u64_key(cur_src[b_start:b_end], rem_mask)
			if sub_diff == 0 {
				if job_count^ >= len(jobs) { panic("_collect_u64_bucket_jobs: exceeded _MAX_SORT_BUCKET_JOBS") }
				jobs[job_count^] = _Sort_Bucket_Job{start = b_start, end = b_end, rem_mask = 0, src_is_a = src_is_a}
				job_count^ += 1
				continue
			}
			sub_msb   := _top_set_bit_u32(sub_diff)
			sub_shift := u32(sub_msb >= 7 ? sub_msb - 7 : 0)
			sub_below := sub_shift < 32 ? ((u32(1) << sub_shift) - 1) : max(u32)
			sub_rem   := sub_diff & sub_below

			sub_offs: [256]u32
			sub_cnts: [256]u32
			_run_u64_count_scatter_wave(p, b_start, b_end, cur_src, cur_dst, sub_shift, &sub_offs, &sub_cnts)
			_collect_u64_bucket_jobs(p, &sub_offs, &sub_cnts, total_n, !src_is_a, sub_rem, depth + 1, jobs, job_count, reserved_siblings + rem_sib)
			continue
		}

		if job_count^ >= len(jobs) { panic("_collect_u64_bucket_jobs: exceeded _MAX_SORT_BUCKET_JOBS") }
		jobs[job_count^] = _Sort_Bucket_Job{start = b_start, end = b_end, rem_mask = rem_mask, src_is_a = src_is_a}
		job_count^ += 1
	}
}

@(private="file")
_collect_umbra_bucket_jobs :: proc(
	p:                 ^Morsel_Pipeline,
	bucket_offs:       ^[256]u32,
	bucket_cnts:       ^[256]u32,
	total_n:           int,
	src_is_a:          bool,
	rem_shift:         u32,
	depth:             int,
	jobs:              ^[_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job,
	job_count:         ^int,
	reserved_siblings := 0,
) {
	skew_thresh := max(total_n / 2, SORT_PARALLEL_PARTITION_MIN)
	for b in 0..<256 {
		cnt := int(bucket_cnts[b])
		if cnt == 0 { continue }
		b_start := int(bucket_offs[b])
		b_end   := b_start + cnt
		rem_sib := 255 - b

		if cnt > skew_thresh && rem_shift >= 8 && depth < 2 && p.num_workers > 1 &&
		   job_count^ + 256 + reserved_siblings + rem_sib <= _MAX_SORT_BUCKET_JOBS {
			cur_src := src_is_a ? p.sort_entries_a : p.sort_entries_b
			cur_dst := src_is_a ? p.sort_entries_b : p.sort_entries_a
			low_mask := rem_shift < 32 ? ((u32(1) << rem_shift) - 1) : max(u32)
			sub_diff := _compute_diff_mask_u64_key(cur_src[b_start:b_end], low_mask)
			if sub_diff == 0 {
				if job_count^ >= len(jobs) { panic("_collect_umbra_bucket_jobs: exceeded _MAX_SORT_BUCKET_JOBS") }
				jobs[job_count^] = _Sort_Bucket_Job{start = b_start, end = b_end, rem_mask = 0, src_is_a = src_is_a}
				job_count^ += 1
				continue
			}
			next_shift: int = -1
			for s := int(rem_shift) - 8; s >= 0; s -= 8 {
				if ((sub_diff >> u32(s)) & 0xFF) != 0 {
					next_shift = s
					break
				}
			}
			if next_shift >= 0 {
				sub_offs: [256]u32
				sub_cnts: [256]u32
				_run_u64_count_scatter_wave(p, b_start, b_end, cur_src, cur_dst, u32(next_shift), &sub_offs, &sub_cnts)
				_collect_umbra_bucket_jobs(p, &sub_offs, &sub_cnts, total_n, !src_is_a, u32(next_shift), depth + 1, jobs, job_count, reserved_siblings + rem_sib)
				continue
			}
		}

		if job_count^ >= len(jobs) { panic("_collect_umbra_bucket_jobs: exceeded _MAX_SORT_BUCKET_JOBS") }
		jobs[job_count^] = _Sort_Bucket_Job{start = b_start, end = b_end, rem_mask = rem_shift, src_is_a = src_is_a}
		job_count^ += 1
	}
}

@(private="file")
_run_str_byte_count_scatter_wave :: proc(
	p:           ^Morsel_Pipeline,
	range_start: int,
	range_end:   int,
	src_buf:     []oa.Str_Sort_Entry,
	dst_buf:     []oa.Str_Sort_Entry,
	shift:       u32,
	bucket_offs: ^[256]u32,
	bucket_cnts: ^[256]u32,
) {
	total := range_end - range_start
	nw := _sort_scatter_workers(p, total)
	chunk := (total + nw - 1) / nw

	acc := _Sort_Done_Acc{}
	sent := 0
	for w in 0..<nw {
		cs := range_start + w * chunk
		ce := min(cs + chunk, range_end)
		if cs >= ce {
			p.sort_worker_hists[w] = {}
			continue
		}
		_sort_send_one(p, w, Sort_Bucket_Work{
			kind            = .Count_Str_Byte,
			shift           = shift,
			start           = cs,
			end             = ce,
			str_entries_src = src_buf,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)

	_prefix_sum_sort_buckets(p, nw, u32(range_start), bucket_offs, bucket_cnts)

	acc = {}
	sent = 0
	for w in 0..<nw {
		cs := range_start + w * chunk
		ce := min(cs + chunk, range_end)
		if cs >= ce { continue }
		_sort_send_one(p, w, Sort_Bucket_Work{
			kind            = .Scatter_Str_Byte,
			shift           = shift,
			start           = cs,
			end             = ce,
			str_entries_src = src_buf,
			str_entries_dst = dst_buf,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)
}

@(private="file")
_collect_str_bucket_jobs :: proc(
	p:                 ^Morsel_Pipeline,
	bucket_offs:       ^[256]u32,
	bucket_cnts:       ^[256]u32,
	total_n:           int,
	src_is_a:          bool,
	prefix_bytes_done: u32,
	depth:             int,
	jobs:              ^[_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job,
	job_count:         ^int,
	reserved_siblings := 0,
) {
	skew_thresh := max(total_n / 2, SORT_PARALLEL_PARTITION_MIN)
	for b in 0..<256 {
		cnt := int(bucket_cnts[b])
		if cnt == 0 { continue }
		b_start := int(bucket_offs[b])
		b_end   := b_start + cnt
		rem_sib := 255 - b

		if cnt > skew_thresh && prefix_bytes_done < 8 && depth < 2 && p.num_workers > 1 &&
		   job_count^ + 256 + reserved_siblings + rem_sib <= _MAX_SORT_BUCKET_JOBS {
			cur_src := src_is_a ? p.str_entries_a : p.str_entries_b
			cur_dst := src_is_a ? p.str_entries_b : p.str_entries_a
			p0 := cur_src[b_start].prefix
			diff_p: u64 = 0
			for i in b_start + 1..<b_end {
				diff_p |= cur_src[i].prefix ~ p0
			}
			cur_shift := int(8 - prefix_bytes_done) * 8
			next_shift: int = -1
			for s := cur_shift - 8; s >= 0; s -= 8 {
				if ((diff_p >> u32(s)) & 0xFF) != 0 {
					next_shift = s
					break
				}
			}
			if next_shift < 0 {
				if job_count^ >= len(jobs) { panic("_collect_str_bucket_jobs: exceeded _MAX_SORT_BUCKET_JOBS") }
				jobs[job_count^] = _Sort_Bucket_Job{start = b_start, end = b_end, rem_mask = 8, src_is_a = src_is_a}
				job_count^ += 1
				continue
			}
			sub_offs: [256]u32
			sub_cnts: [256]u32
			_run_str_byte_count_scatter_wave(p, b_start, b_end, cur_src, cur_dst, u32(next_shift), &sub_offs, &sub_cnts)
			sub_done := u32(8 - next_shift / 8)
			_collect_str_bucket_jobs(p, &sub_offs, &sub_cnts, total_n, !src_is_a, sub_done, depth + 1, jobs, job_count, reserved_siblings + rem_sib)
			continue
		}

		if job_count^ >= len(jobs) { panic("_collect_str_bucket_jobs: exceeded _MAX_SORT_BUCKET_JOBS") }
		jobs[job_count^] = _Sort_Bucket_Job{start = b_start, end = b_end, rem_mask = prefix_bytes_done, src_is_a = src_is_a}
		job_count^ += 1
	}
}

@(private="file")
_dispatch_u64_bucket_jobs :: proc(
	p:         ^Morsel_Pipeline,
	jobs:      []_Sort_Bucket_Job,
	out_i64:   []i64,
	is_umbra:  bool,
	umbra_arr: ^oa.Umbra_Array,
) {
	nw := p.num_workers
	worker_loads: [256]int
	acc := _Sort_Done_Acc{}
	sent := 0
	kind: Sort_Task_Kind = is_umbra ? .Sort_Bucket_Umbra : .Sort_Bucket_U64

	for job in jobs {
		min_w := 0
		if nw > 1 && nw <= len(worker_loads) {
			min_load := worker_loads[0]
			for w in 1..<nw {
				if worker_loads[w] < min_load {
					min_load = worker_loads[w]
					min_w = w
				}
			}
			worker_loads[min_w] += job.end - job.start
		} else {
			min_w = sent % nw
		}

		src_buf := job.src_is_a ? p.sort_entries_a : p.sort_entries_b
		dst_buf := job.src_is_a ? p.sort_entries_b : p.sort_entries_a
		_sort_send_one(p, min_w, Sort_Bucket_Work{
			kind        = kind,
			rem_mask    = job.rem_mask,
			start       = job.start,
			end         = job.end,
			entries_src = src_buf,
			entries_dst = dst_buf,
			out_i64     = out_i64,
			umbra_arr   = umbra_arr,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)
}

@(private="file")
_dispatch_str_bucket_jobs :: proc(
	p:       ^Morsel_Pipeline,
	jobs:    []_Sort_Bucket_Job,
	offsets: [^]i32,
	bytes:   [^]u8,
	arr_off: int,
	out_i64: []i64,
) {
	nw := p.num_workers
	worker_loads: [256]int
	acc := _Sort_Done_Acc{}
	sent := 0

	for job in jobs {
		min_w := 0
		if nw > 1 && nw <= len(worker_loads) {
			min_load := worker_loads[0]
			for w in 1..<nw {
				if worker_loads[w] < min_load {
					min_load = worker_loads[w]
					min_w = w
				}
			}
			worker_loads[min_w] += job.end - job.start
		} else {
			min_w = sent % nw
		}

		src_buf := job.src_is_a ? p.str_entries_a : p.str_entries_b
		dst_buf := job.src_is_a ? p.str_entries_b : p.str_entries_a
		_sort_send_one(p, min_w, Sort_Bucket_Work{
			kind            = .Sort_Bucket_Str,
			rem_mask        = job.rem_mask,
			start           = job.start,
			end             = job.end,
			out_i64         = out_i64,
			str_offsets     = offsets,
			str_bytes       = bytes,
			str_arr_off     = arr_off,
			str_entries_src = src_buf,
			str_entries_dst = dst_buf,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)
}

@(private="file")
_pipeline_sort_indices_u32_impl :: proc(
	p:         ^Morsel_Pipeline,
	arr:       ^oa.Array,
	is_signed: bool,
	allocator: mem.Allocator,
) -> (result: oa.Array, err: mem.Allocator_Error) {
	n := arr.length
	if n <= 0 {
		buf := oa.buffer_make_uninit(0, allocator) or_return
		result = oa.Array{type = oa.Int64_Type{}, length = 0, null_count = 0, buffers = {{}, buf, {}}}
		return
	}
	if p == nil || !p.initialized {
		return oa.compute_sort_indices(arr, allocator)
	}

	data_buf := oa.buffer_make_uninit(n * size_of(i64), allocator) or_return
	out_i64 := (cast([^]i64)data_buf.data)[:n]
	raw_u32 := mem.ptr_offset(cast([^]u32)arr.buffers[1].data, arr.offset)
	nc := oa.array_null_count(arr)

	if nc == 0 {
		diff_mask := _compute_diff_mask_u32(raw_u32, n)
		if diff_mask == 0 {
			for i in 0..<n {
				out_i64[i] = i64(i)
			}
			result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
			return
		}

		if _ensure_u64_sort_scratch(p, n) != .None {
			oa.buffer_free(&data_buf)
			return oa.compute_sort_indices(arr, allocator)
		}

		if n < SORT_PARALLEL_PARTITION_MIN || p.num_workers <= 1 {
			ea := p.sort_entries_a[:n]
			if is_signed {
				for i in 0..<n {
					ea[i] = (u64(raw_u32[i] ~ 0x8000_0000) << 32) | u64(u32(i))
				}
			} else {
				for i in 0..<n {
					ea[i] = (u64(raw_u32[i]) << 32) | u64(u32(i))
				}
			}
			oa.radix_sort_bucket_u64(ea, p.sort_entries_b[:n], out_i64, diff_mask)
			result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
			return
		}

		msb_idx := _top_set_bit_u32(diff_mask)
		part_shift := u32(msb_idx >= 7 ? msb_idx - 7 : 0)
		below_mask := part_shift < 32 ? ((u32(1) << part_shift) - 1) : max(u32)
		rem_mask := diff_mask & below_mask

		nw := _sort_scatter_workers(p, n)
		chunk := (n + nw - 1) / nw
		c_kind: Sort_Task_Kind = is_signed ? .Count_U32_Signed : .Count_U32_Unsigned
		s_kind: Sort_Task_Kind = is_signed ? .Scatter_U32_Signed : .Scatter_U32_Unsigned

		was_parking := _sort_begin_unparked(p)

		acc := _Sort_Done_Acc{}
		sent := 0
		for w in 0..<nw {
			cs := w * chunk
			ce := min(cs + chunk, n)
			if cs >= ce {
				p.sort_worker_hists[w] = {}
				continue
			}
			_sort_send_one(p, w, Sort_Bucket_Work{
				kind    = c_kind,
				shift   = part_shift,
				start   = cs,
				end     = ce,
				raw_u32 = raw_u32,
			}, &acc, &sent)
		}
		_sort_flush_and_wait(p, &acc, sent)

		bucket_offs: [256]u32
		bucket_cnts: [256]u32
		_prefix_sum_sort_buckets(p, nw, 0, &bucket_offs, &bucket_cnts)

		acc = {}
		sent = 0
		for w in 0..<nw {
			cs := w * chunk
			ce := min(cs + chunk, n)
			if cs >= ce { continue }
			_sort_send_one(p, w, Sort_Bucket_Work{
				kind        = s_kind,
				shift       = part_shift,
				start       = cs,
				end         = ce,
				raw_u32     = raw_u32,
				entries_dst = p.sort_entries_a,
			}, &acc, &sent)
		}
		_sort_flush_and_wait(p, &acc, sent)

		jobs: [_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job
		job_count := 0
		_collect_u64_bucket_jobs(p, &bucket_offs, &bucket_cnts, n, true, rem_mask, 0, &jobs, &job_count)
		_dispatch_u64_bucket_jobs(p, jobs[:job_count], out_i64, false, nil)
		_sort_end_unparked(p, was_parking)

		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	// Nullable path (nulls placed last in original ascending order)
	vbits := arr.buffers[0].data
	n_valid := n - nc
	if n_valid <= 0 {
		for i in 0..<n {
			out_i64[i] = i64(i)
		}
		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	if _ensure_u64_sort_scratch(p, n_valid) != .None {
		oa.buffer_free(&data_buf)
		return oa.compute_sort_indices(arr, allocator)
	}

	pack_buf := p.sort_entries_b[:n_valid]
	vi := 0
	ni := n_valid
	k0: u32 = 0
	diff_mask: u32 = 0
	if is_signed {
		for i in 0..<n {
			if oa.bitmap_get(vbits, arr.offset + i) {
				k := raw_u32[i] ~ 0x8000_0000
				if vi == 0 {
					k0 = k
				} else {
					diff_mask |= k ~ k0
				}
				pack_buf[vi] = (u64(k) << 32) | u64(u32(i))
				vi += 1
			} else {
				out_i64[ni] = i64(i)
				ni += 1
			}
		}
	} else {
		for i in 0..<n {
			if oa.bitmap_get(vbits, arr.offset + i) {
				k := raw_u32[i]
				if vi == 0 {
					k0 = k
				} else {
					diff_mask |= k ~ k0
				}
				pack_buf[vi] = (u64(k) << 32) | u64(u32(i))
				vi += 1
			} else {
				out_i64[ni] = i64(i)
				ni += 1
			}
		}
	}

	if diff_mask == 0 {
		for i in 0..<n_valid {
			out_i64[i] = i64(u32(pack_buf[i]))
		}
		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	if n_valid < SORT_PARALLEL_PARTITION_MIN || p.num_workers <= 1 {
		oa.radix_sort_bucket_u64(pack_buf, p.sort_entries_a[:n_valid], out_i64[:n_valid], diff_mask)
		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	msb_idx := _top_set_bit_u32(diff_mask)
	part_shift := u32(msb_idx >= 7 ? msb_idx - 7 : 0)
	below_mask := part_shift < 32 ? ((u32(1) << part_shift) - 1) : max(u32)
	rem_mask := diff_mask & below_mask

	was_parking := _sort_begin_unparked(p)
	bucket_offs: [256]u32
	bucket_cnts: [256]u32
	_run_u64_count_scatter_wave(p, 0, n_valid, p.sort_entries_b, p.sort_entries_a, part_shift, &bucket_offs, &bucket_cnts)

	jobs: [_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job
	job_count := 0
	_collect_u64_bucket_jobs(p, &bucket_offs, &bucket_cnts, n_valid, true, rem_mask, 0, &jobs, &job_count)
	_dispatch_u64_bucket_jobs(p, jobs[:job_count], out_i64[:n_valid], false, nil)
	_sort_end_unparked(p, was_parking)

	result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
	return
}

// Race-free, zero-merge-pass Sleipnir actor parallel signed `i32` MSB-bucket radix sort.
pipeline_sort_indices_i32 :: proc(
	p:         ^Morsel_Pipeline,
	arr:       ^oa.Array,
	allocator := context.allocator,
) -> (oa.Array, mem.Allocator_Error) {
	if arr == nil {
		return {}, .Invalid_Argument
	}
	if _, ok := arr.type.(oa.Int32_Type); !ok {
		return oa.compute_sort_indices(arr, allocator)
	}
	return _pipeline_sort_indices_u32_impl(p, arr, true, allocator)
}

// Race-free, zero-merge-pass Sleipnir actor parallel unsigned `u32` MSB-bucket radix sort.
pipeline_sort_indices_u32 :: proc(
	p:         ^Morsel_Pipeline,
	arr:       ^oa.Array,
	allocator := context.allocator,
) -> (oa.Array, mem.Allocator_Error) {
	if arr == nil {
		return {}, .Invalid_Argument
	}
	if _, ok := arr.type.(oa.UInt32_Type); !ok {
		return oa.compute_sort_indices(arr, allocator)
	}
	return _pipeline_sort_indices_u32_impl(p, arr, false, allocator)
}

// Race-free, zero-merge-pass Sleipnir actor parallel MSB-bucket Arrow `String_Type` sort.
pipeline_sort_indices_str :: proc(
	p:         ^Morsel_Pipeline,
	arr:       ^oa.Array,
	allocator := context.allocator,
) -> (result: oa.Array, err: mem.Allocator_Error) {
	if arr == nil {
		return {}, .Invalid_Argument
	}
	if _, ok := arr.type.(oa.String_Type); !ok {
		return oa.compute_sort_indices(arr, allocator)
	}
	n := arr.length
	if n <= 0 {
		buf := oa.buffer_make_uninit(0, allocator) or_return
		result = oa.Array{type = oa.Int64_Type{}, length = 0, null_count = 0, buffers = {{}, buf, {}}}
		return
	}
	if p == nil || !p.initialized {
		return oa.compute_sort_indices(arr, allocator)
	}

	data_buf := oa.buffer_make_uninit(n * size_of(i64), allocator) or_return
	out_i64 := (cast([^]i64)data_buf.data)[:n]
	offsets := cast([^]i32)arr.buffers[1].data
	bytes := arr.buffers[2].data
	arr_off := arr.offset
	nc := oa.array_null_count(arr)

	if nc == 0 {
		if _ensure_str_sort_scratch(p, n) != .None {
			oa.buffer_free(&data_buf)
			return oa.compute_sort_indices(arr, allocator)
		}

		if n < SORT_PARALLEL_PARTITION_MIN || p.num_workers <= 1 {
			ea := p.str_entries_a[:n]
			for i in 0..<n {
				si := int(offsets[arr_off + i])
				ei := int(offsets[arr_off + i + 1])
				ea[i] = oa.Str_Sort_Entry{
					prefix = oa.str_entry_prefix(bytes, si, ei),
					idx    = i32(i),
					slen   = i32(ei - si),
				}
			}
			oa.string_sort_range_into(offsets, bytes, arr_off, ea, p.str_entries_b[:n], out_i64, 0)
			result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
			return
		}

		nw := _sort_scatter_workers(p, n)
		chunk := (n + nw - 1) / nw

		was_parking := _sort_begin_unparked(p)

		acc := _Sort_Done_Acc{}
		sent := 0
		for w in 0..<nw {
			cs := w * chunk
			ce := min(cs + chunk, n)
			if cs >= ce {
				p.sort_worker_hists[w] = {}
				continue
			}
			_sort_send_one(p, w, Sort_Bucket_Work{
				kind        = .Count_Str_Lead,
				start       = cs,
				end         = ce,
				str_offsets = offsets,
				str_bytes   = bytes,
				str_arr_off = arr_off,
			}, &acc, &sent)
		}
		_sort_flush_and_wait(p, &acc, sent)

		bucket_offs: [256]u32
		bucket_cnts: [256]u32
		_prefix_sum_sort_buckets(p, nw, 0, &bucket_offs, &bucket_cnts)

		acc = {}
		sent = 0
		for w in 0..<nw {
			cs := w * chunk
			ce := min(cs + chunk, n)
			if cs >= ce { continue }
			_sort_send_one(p, w, Sort_Bucket_Work{
				kind            = .Scatter_Str_Lead,
				start           = cs,
				end             = ce,
				str_offsets     = offsets,
				str_bytes       = bytes,
				str_arr_off     = arr_off,
				str_entries_dst = p.str_entries_a,
			}, &acc, &sent)
		}
		_sort_flush_and_wait(p, &acc, sent)

		jobs: [_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job
		job_count := 0
		_collect_str_bucket_jobs(p, &bucket_offs, &bucket_cnts, n, true, 1, 0, &jobs, &job_count)
		_dispatch_str_bucket_jobs(p, jobs[:job_count], offsets, bytes, arr_off, out_i64)
		_sort_end_unparked(p, was_parking)

		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	// Nullable strings (nulls placed last in original ascending order)
	vbits := arr.buffers[0].data
	n_valid := n - nc
	if n_valid <= 0 {
		for i in 0..<n {
			out_i64[i] = i64(i)
		}
		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	if _ensure_str_sort_scratch(p, n_valid) != .None {
		oa.buffer_free(&data_buf)
		return oa.compute_sort_indices(arr, allocator)
	}

	pack_buf := p.str_entries_b[:n_valid]
	vi := 0
	ni := n_valid
	for i in 0..<n {
		if oa.bitmap_get(vbits, arr_off + i) {
			si := int(offsets[arr_off + i])
			ei := int(offsets[arr_off + i + 1])
			pack_buf[vi] = oa.Str_Sort_Entry{
				prefix = oa.str_entry_prefix(bytes, si, ei),
				idx    = i32(i),
				slen   = i32(ei - si),
			}
			vi += 1
		} else {
			out_i64[ni] = i64(i)
			ni += 1
		}
	}

	if n_valid < SORT_PARALLEL_PARTITION_MIN || p.num_workers <= 1 {
		oa.string_sort_range_into(offsets, bytes, arr_off, pack_buf, p.str_entries_a[:n_valid], out_i64[:n_valid], 0)
		result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
		return
	}

	was_parking := _sort_begin_unparked(p)
	bucket_offs: [256]u32
	bucket_cnts: [256]u32
	_run_str_byte_count_scatter_wave(p, 0, n_valid, p.str_entries_b, p.str_entries_a, 56, &bucket_offs, &bucket_cnts)

	jobs: [_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job
	job_count := 0
	_collect_str_bucket_jobs(p, &bucket_offs, &bucket_cnts, n_valid, true, 1, 0, &jobs, &job_count)
	_dispatch_str_bucket_jobs(p, jobs[:job_count], offsets, bytes, arr_off, out_i64[:n_valid])
	_sort_end_unparked(p, was_parking)

	result = oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
	return
}

// Dispatch Arrow array parallel sort indices by concrete data type, falling back to
// single-threaded `compute_sort_indices` for types not yet specialized on Sleipnir actors.
pipeline_sort_indices :: proc(
	p:         ^Morsel_Pipeline,
	arr:       ^oa.Array,
	allocator := context.allocator,
) -> (oa.Array, mem.Allocator_Error) {
	if arr == nil {
		return {}, .Invalid_Argument
	}
	#partial switch _ in arr.type {
	case oa.Int32_Type:
		return pipeline_sort_indices_i32(p, arr, allocator)
	case oa.UInt32_Type:
		return pipeline_sort_indices_u32(p, arr, allocator)
	case oa.String_Type:
		return pipeline_sort_indices_str(p, arr, allocator)
	}
	return oa.compute_sort_indices(arr, allocator)
}

// Race-free, zero-merge-pass Sleipnir actor parallel `Umbra_Array` sort returning `Int64_Type` indices.
pipeline_umbra_sort_indices :: proc(
	p:         ^Morsel_Pipeline,
	a:         ^oa.Umbra_Array,
	allocator := context.allocator,
) -> oa.Array {
	if a == nil || a.length <= 0 || p == nil || !p.initialized || a.length < SORT_PARALLEL_PARTITION_MIN || p.num_workers <= 1 {
		return oa.umbra_sort_indices(a, allocator)
	}
	n := a.length
	data_buf, b_err := oa.buffer_make_uninit(n * size_of(i64), allocator)
	if b_err != .None {
		return oa.umbra_sort_indices(a, allocator)
	}
	if _ensure_u64_sort_scratch(p, n) != .None {
		oa.buffer_free(&data_buf)
		return oa.umbra_sort_indices(a, allocator)
	}

	out_i64 := (cast([^]i64)data_buf.data)[:n]
	nw := _sort_scatter_workers(p, n)
	chunk := (n + nw - 1) / nw

	was_parking := _sort_begin_unparked(p)

	acc := _Sort_Done_Acc{}
	sent := 0
	for w in 0..<nw {
		cs := w * chunk
		ce := min(cs + chunk, n)
		if cs >= ce {
			p.sort_worker_hists[w] = {}
			continue
		}
		_sort_send_one(p, w, Sort_Bucket_Work{
			kind      = .Count_Umbra,
			start     = cs,
			end       = ce,
			umbra_arr = a,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)

	bucket_offs: [256]u32
	bucket_cnts: [256]u32
	_prefix_sum_sort_buckets(p, nw, 0, &bucket_offs, &bucket_cnts)

	acc = {}
	sent = 0
	for w in 0..<nw {
		cs := w * chunk
		ce := min(cs + chunk, n)
		if cs >= ce { continue }
		_sort_send_one(p, w, Sort_Bucket_Work{
			kind        = .Scatter_Umbra,
			start       = cs,
			end         = ce,
			umbra_arr   = a,
			entries_dst = p.sort_entries_a,
		}, &acc, &sent)
	}
	_sort_flush_and_wait(p, &acc, sent)

	jobs: [_MAX_SORT_BUCKET_JOBS]_Sort_Bucket_Job
	job_count := 0
	_collect_umbra_bucket_jobs(p, &bucket_offs, &bucket_cnts, n, true, 24, 0, &jobs, &job_count)
	_dispatch_u64_bucket_jobs(p, jobs[:job_count], out_i64, true, a)
	_sort_end_unparked(p, was_parking)

	return oa.Array{type = oa.Int64_Type{}, length = n, null_count = 0, buffers = {{}, data_buf, {}}}
}
