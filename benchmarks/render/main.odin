package bench_render

import "core:encoding/json"
import "core:fmt"
import "core:math"
import vmem "core:mem/virtual"
import "core:os"
import "core:strings"

DEFAULT_HUGI_BIN :: "/home/pauli/.local/bin/hugi"
DEFAULT_DB_PATH :: "benchmarks/history.hugidb"
RENDERS_DIR :: "benchmarks/renders"
README_PATH :: "benchmarks/README.md"

PALETTE :: [7]string{
	"#58a6ff",
	"#3fb950",
	"#f78166",
	"#d2a8ff",
	"#ffa657",
	"#79c0ff",
	"#ff7b72",
}

Group_Meta :: struct {
	key:   string,
	title: string,
	desc:  string,
}

GROUP_META :: [6]Group_Meta{
	{"builders", "`builders` — Contiguous Primitive Array Builders", "Typed contiguous primitive builders with `0 B` unnecessary intermediate copies."},
	{"compute", "`compute` — Single-Threaded, Multi-Threaded & Actor Compute Kernels", "SIMD reductions, min/max, sparse/dense filters, casts, take, and radix index sorts."},
	{"strings", "`strings` — Variable-Length String Build, Scan, Filter & Sort", "Offset-indexed string buffers, vectorized prefix/equality scans, and actor string sorts."},
	{"fusion", "`fusion` — Fused Multi-Predicate & Filter-Aggregate Operators", "Single-pass `sum_where`, `agg_where`, and two-predicate fused pipelines without materializing bitmaps."},
	{"encoding", "`encoding` — `Dict_Array`, `RLE_Array` & `Umbra_Array` Compressed Kernels", "Dictionary value coding, constant-time RLE reductions, and 12B German-style Umbra string sorting."},
	{"ipc", "`ipc` — Zero-Copy Vectored `writev` & Lazy `mmap` Arrow IPC", "Zero-copy IPC serialization and memory-mapped Arrow file reads with zero buffer copies."},
}

Hugi_Record :: struct {
	commit:             string,
	lang:               string,
	group_name:         string,
	name:               string,
	full_name:          string,
	samples:            int,
	p50_ns:             f64,
	p95_ns:             f64,
	cv_pct:             f64,
	ops_per_sec:        f64,
	mb_per_sec:         f64,
	allocs_per_op:      f64,
	alloc_bytes_per_op: f64,
}

Svg_Tick_View :: struct {
	pos:      string,
	text_pos: string,
	label:    string,
}

Bar_View :: struct {
	label:    string,
	text_y:   string,
	rect_y:   string,
	width:    string,
	val_x:    string,
	val_y:    string,
	val_text: string,
	color:    string,
}

Bar_Chart_View :: struct {
	title:       string,
	subtitle:    string,
	width:       int,
	height:      int,
	pad_l:       int,
	pad_t:       int,
	pw:          int,
	ph:          int,
	plot_bottom: int,
	label_x:     int,
	x_label_y:   int,
	x_ticks:     []Svg_Tick_View,
	bars:        []Bar_View,
}

Grouped_Bar_View :: struct {
	label:         string,
	label_y:       string,
	odin_y:        string,
	odin_w:        string,
	odin_val_x:    string,
	odin_text_y:   string,
	odin_val_text: string,
	cpp_y:         string,
	cpp_w:         string,
	cpp_val_x:     string,
	cpp_text_y:    string,
	cpp_val_text:  string,
	badge_y:       string,
	speedup:       string,
}

Grouped_Bar_Chart_View :: struct {
	title:       string,
	subtitle:    string,
	odin_label:  string,
	cpp_label:   string,
	odin_color:  string,
	cpp_color:   string,
	width:       int,
	height:      int,
	pad_l:       int,
	pad_t:       int,
	pw:          int,
	ph:          int,
	plot_bottom: int,
	label_x:     int,
	x_label_y:   int,
	badge_x:     int,
	x_ticks:     []Svg_Tick_View,
	groups:      []Grouped_Bar_View,
}

Report_Row_View :: struct {
	name:   string,
	median: string,
	p95:    string,
	ops:    string,
	cv:     string,
	vs_cpp: string,
	allocs: string,
}

Report_Group_View :: struct {
	title:    string,
	desc:     string,
	has_desc: bool,
	count:    int,
	rows:     []Report_Row_View,
}

Suite_Report_View :: struct {
	commit:            string,
	cpp_commit:        string,
	total_benchmarks:  int,
	cpp_count:         int,
	total_samples_str: string,
	group_count:       int,
	groups:            []Report_Group_View,
}

Bar_Spec :: struct {
	full_name: string,
	label:     string,
	color:     string,
}

Pair_Spec :: struct {
	odin_name: string,
	cpp_name:  string,
	label:     string,
}

grouped_bar_chart_tmpl := compiled("templates/grouped_bar_chart.temple.twig", Grouped_Bar_Chart_View)
bar_chart_tmpl := compiled("templates/bar_chart.temple.twig", Bar_Chart_View)
suite_report_tmpl := compiled("templates/suite_report.temple.twig", Suite_Report_View)

@(require_results)
fmt_commas_u64 :: proc(val: u64, allocator := context.allocator) -> string {
	buf: [32]byte
	pos := len(buf)
	v := val
	digits := 0
	if v == 0 {
		pos -= 1
		buf[pos] = '0'
	} else {
		for v > 0 {
			if digits > 0 && digits % 3 == 0 {
				pos -= 1
				buf[pos] = ','
			}
			pos -= 1
			buf[pos] = byte('0' + (v % 10))
			v /= 10
			digits += 1
		}
	}
	return strings.clone(string(buf[pos:]), allocator)
}

@(require_results)
fmt_ns :: proc(ns: f64, allocator := context.allocator) -> string {
	if ns < 0.0005 {
		return strings.clone("< 1 ps", allocator)
	}
	if ns < 1.0 {
		return fmt.aprintf("%.2f ps", ns * 1000.0, allocator = allocator)
	}
	if ns < 1_000.0 {
		return fmt.aprintf("%.2f ns", ns, allocator = allocator)
	}
	if ns < 1_000_000.0 {
		return fmt.aprintf("%.2f us", ns / 1_000.0, allocator = allocator)
	}
	if ns < 1_000_000_000.0 {
		return fmt.aprintf("%.2f ms", ns / 1_000_000.0, allocator = allocator)
	}
	return fmt.aprintf("%.3f s", ns / 1_000_000_000.0, allocator = allocator)
}

@(require_results)
fmt_axis_ns :: proc(ns: f64, allocator := context.allocator) -> string {
	if ns <= 0 {
		return strings.clone("0", allocator)
	}
	raw := fmt_ns(ns, context.temp_allocator)
	if s, ok := strings.replace_all(raw, ".000 ", " ", context.temp_allocator); ok {
		raw = s
	}
	if s, ok := strings.replace_all(raw, ".00 ", " ", context.temp_allocator); ok {
		raw = s
	}
	return strings.clone(raw, allocator)
}

@(require_results)
fmt_bytes :: proc(b: f64, allocator := context.allocator) -> string {
	if b < 1024.0 {
		return fmt.aprintf("%.0f B", b, allocator = allocator)
	}
	if b < 1024.0 * 1024.0 {
		return fmt.aprintf("%.2f KiB", b / 1024.0, allocator = allocator)
	}
	return fmt.aprintf("%.2f MiB", b / (1024.0 * 1024.0), allocator = allocator)
}

@(require_results)
fmt_tp_or_ops :: proc(rec: Hugi_Record, allocator := context.allocator) -> string {
	if rec.mb_per_sec >= 1024.0 * 1024.0 {
		return fmt.aprintf("%.2f TB/s", rec.mb_per_sec / (1024.0 * 1024.0), allocator = allocator)
	}
	if rec.mb_per_sec >= 1024.0 {
		return fmt.aprintf("%.2f GB/s", rec.mb_per_sec / 1024.0, allocator = allocator)
	}
	if rec.mb_per_sec > 0 {
		return fmt.aprintf("%.1f MB/s", rec.mb_per_sec, allocator = allocator)
	}
	if rec.ops_per_sec >= 1_000_000_000.0 {
		return fmt.aprintf("%.2fB/s", rec.ops_per_sec / 1_000_000_000.0, allocator = allocator)
	}
	if rec.ops_per_sec >= 1_000_000.0 {
		return fmt.aprintf("%.2fM/s", rec.ops_per_sec / 1_000_000.0, allocator = allocator)
	}
	if rec.ops_per_sec >= 1_000.0 {
		return fmt.aprintf("%.1fK/s", rec.ops_per_sec / 1_000.0, allocator = allocator)
	}
	return fmt.aprintf("%.0f/s", rec.ops_per_sec, allocator = allocator)
}

@(require_results)
exec_hugi :: proc(args: []string, allocator := context.allocator) -> (stdout: []byte, ok: bool) {
	hugi_bin := DEFAULT_HUGI_BIN if os.exists(DEFAULT_HUGI_BIN) else "hugi"
	cmd := make([dynamic]string, 0, len(args) + 1, context.temp_allocator)
	append(&cmd, hugi_bin)
	for a in args {
		append(&cmd, a)
	}
	state, out, stderr, exec_err := os.process_exec(os.Process_Desc{command = cmd[:]}, allocator)
	if exec_err != nil && hugi_bin != "hugi" {
		delete(out, allocator)
		delete(stderr, allocator)
		cmd[0] = "hugi"
		state, out, stderr, exec_err = os.process_exec(os.Process_Desc{command = cmd[:]}, allocator)
	}
	defer delete(stderr, allocator)
	if exec_err != nil || !state.success {
		delete(out, allocator)
		return nil, false
	}
	return out, true
}

@(require_results)
render_grouped_bar_svg :: proc(
	title: string,
	subtitle: string,
	specs: []Pair_Spec,
	odin_map: map[string]Hugi_Record,
	cpp_map: map[string]Hugi_Record,
	out_path: string,
	allocator := context.allocator,
) -> bool {
	width, pad_l, pad_r, pad_t, pad_b, row_h := 980, 230, 290, 92, 42, 42
	Matched_Pair :: struct {
		spec: Pair_Spec,
		odin: Hugi_Record,
		cpp:  Hugi_Record,
	}
	matched := make([dynamic]Matched_Pair, 0, len(specs), allocator)
	min_v, max_v := max(f64), 0.0
	for sp in specs {
		or, ok_o := odin_map[sp.odin_name]
		cr, ok_c := cpp_map[sp.cpp_name]
		if ok_o && ok_c && or.p50_ns > 0 && cr.p50_ns > 0 {
			append(&matched, Matched_Pair{sp, or, cr})
			min_v = min(min_v, min(or.p50_ns, cr.p50_ns))
			max_v = max(max_v, max(or.p50_ns, cr.p50_ns))
		}
	}
	if len(matched) == 0 || max_v <= 0 {
		return false
	}

	ph := len(matched) * row_h + 14
	height := pad_t + ph + pad_b
	pw := width - pad_l - pad_r
	lo := int(math.floor(math.log10(min_v)))
	hi := int(math.ceil(math.log10(max_v)))
	if lo == hi { hi += 1 }

	x_ticks := make([]Svg_Tick_View, hi - lo + 1, allocator)
	for e := lo; e <= hi; e += 1 {
		t := f64(e - lo) / f64(hi - lo)
		x_ticks[e - lo] = Svg_Tick_View{
			pos   = fmt.aprintf("%.1f", f64(pad_l) + t * f64(pw), allocator = allocator),
			label = fmt_axis_ns(math.pow10(f64(e)), allocator),
		}
	}

	groups := make([]Grouped_Bar_View, len(matched), allocator)
	for item, idx in matched {
		ry := f64(pad_t + 10 + idx * row_h)
		to := (math.log10(item.odin.p50_ns) - f64(lo)) / f64(hi - lo)
		tc := (math.log10(item.cpp.p50_ns) - f64(lo)) / f64(hi - lo)
		wo := max(4.0, to * f64(pw))
		wc := max(4.0, tc * f64(pw))
		ratio := item.cpp.p50_ns / item.odin.p50_ns
		spd := fmt.aprintf("%.2fx faster", ratio, allocator = allocator) if ratio >= 1.0 else fmt.aprintf("%.2fx", ratio, allocator = allocator)

		groups[idx] = Grouped_Bar_View{
			label         = item.spec.label,
			label_y       = fmt.aprintf("%.1f", ry + 20.0, allocator = allocator),
			odin_y        = fmt.aprintf("%.1f", ry + 2.0, allocator = allocator),
			odin_w        = fmt.aprintf("%.1f", wo, allocator = allocator),
			odin_val_x    = fmt.aprintf("%.1f", f64(pad_l) + wo + 8.0, allocator = allocator),
			odin_text_y   = fmt.aprintf("%.1f", ry + 12.0, allocator = allocator),
			odin_val_text = fmt.aprintf("%s (%s)", fmt_ns(item.odin.p50_ns, context.temp_allocator), fmt_tp_or_ops(item.odin, context.temp_allocator), allocator = allocator),
			cpp_y         = fmt.aprintf("%.1f", ry + 18.0, allocator = allocator),
			cpp_w         = fmt.aprintf("%.1f", wc, allocator = allocator),
			cpp_val_x     = fmt.aprintf("%.1f", f64(pad_l) + wc + 8.0, allocator = allocator),
			cpp_text_y    = fmt.aprintf("%.1f", ry + 28.0, allocator = allocator),
			cpp_val_text  = fmt.aprintf("%s (%s)", fmt_ns(item.cpp.p50_ns, context.temp_allocator), fmt_tp_or_ops(item.cpp, context.temp_allocator), allocator = allocator),
			badge_y       = fmt.aprintf("%.1f", ry + 20.0, allocator = allocator),
			speedup       = spd,
		}
	}

	view := Grouped_Bar_Chart_View{
		title       = title,
		subtitle    = subtitle,
		odin_label  = "Skadi (Odin)",
		cpp_label   = "Arrow C++",
		odin_color  = PALETTE[1],
		cpp_color   = PALETTE[2],
		width       = width,
		height      = height,
		pad_l       = pad_l,
		pad_t       = pad_t,
		pw          = pw,
		ph          = ph,
		plot_bottom = pad_t + ph,
		label_x     = pad_l - 10,
		x_label_y   = pad_t + ph + 18,
		badge_x     = width - 18,
		x_ticks     = x_ticks,
		groups      = groups,
	}
	sb := strings.builder_make(0, grouped_bar_chart_tmpl.approx_bytes, context.temp_allocator)
	if _, err := grouped_bar_chart_tmpl.with(strings.to_writer(&sb), view); err != nil {
		return false
	}
	return os.write_entire_file(out_path, transmute([]byte)strings.to_string(sb)) == nil
}

@(require_results)
render_bar_svg :: proc(
	title: string,
	subtitle: string,
	specs: []Bar_Spec,
	by_full: map[string]Hugi_Record,
	out_path: string,
	allocator := context.allocator,
) -> bool {
	width, pad_l, pad_r, pad_t, pad_b, row_h := 960, 260, 230, 68, 42, 34
	matched := make([dynamic]struct{ spec: Bar_Spec, rec: Hugi_Record }, 0, len(specs), allocator)
	min_v, max_v := max(f64), 0.0
	for sp in specs {
		if rec, ok := by_full[sp.full_name]; ok && rec.p50_ns > 0 {
			append(&matched, struct{ spec: Bar_Spec, rec: Hugi_Record }{sp, rec})
			min_v = min(min_v, rec.p50_ns)
			max_v = max(max_v, rec.p50_ns)
		}
	}
	if len(matched) == 0 || max_v <= 0 {
		return false
	}

	ph := len(matched) * row_h + 12
	height := pad_t + ph + pad_b
	pw := width - pad_l - pad_r
	lo := int(math.floor(math.log10(min_v)))
	hi := int(math.ceil(math.log10(max_v)))
	if lo == hi { hi += 1 }

	x_ticks := make([]Svg_Tick_View, hi - lo + 1, allocator)
	for e := lo; e <= hi; e += 1 {
		t := f64(e - lo) / f64(hi - lo)
		x_ticks[e - lo] = Svg_Tick_View{
			pos   = fmt.aprintf("%.1f", f64(pad_l) + t * f64(pw), allocator = allocator),
			label = fmt_axis_ns(math.pow10(f64(e)), allocator),
		}
	}

	bars := make([]Bar_View, len(matched), allocator)
	for item, idx in matched {
		ry := pad_t + 10 + idx * row_h
		t := (math.log10(item.rec.p50_ns) - f64(lo)) / f64(hi - lo)
		w_bar := max(4.0, t * f64(pw))
		dur := fmt_ns(item.rec.p50_ns, context.temp_allocator)
		tp := fmt_tp_or_ops(item.rec, context.temp_allocator)
		bars[idx] = Bar_View{
			label    = item.spec.label,
			text_y   = fmt.aprintf("%.1f", f64(ry) + 16.0, allocator = allocator),
			rect_y   = fmt.aprintf("%.1f", f64(ry) + 3.0, allocator = allocator),
			width    = fmt.aprintf("%.1f", w_bar, allocator = allocator),
			val_x    = fmt.aprintf("%.1f", f64(pad_l) + w_bar + 8.0, allocator = allocator),
			val_y    = fmt.aprintf("%.1f", f64(ry) + 17.0, allocator = allocator),
			val_text = fmt.aprintf("%s (%s)", dur, tp, allocator = allocator),
			color    = item.spec.color,
		}
	}

	view := Bar_Chart_View{
		title       = title,
		subtitle    = subtitle,
		width       = width,
		height      = height,
		pad_l       = pad_l,
		pad_t       = pad_t,
		pw          = pw,
		ph          = ph,
		plot_bottom = pad_t + ph,
		label_x     = pad_l - 10,
		x_label_y   = pad_t + ph + 18,
		x_ticks     = x_ticks,
		bars        = bars,
	}
	sb := strings.builder_make(0, bar_chart_tmpl.approx_bytes, context.temp_allocator)
	if _, err := bar_chart_tmpl.with(strings.to_writer(&sb), view); err != nil {
		return false
	}
	return os.write_entire_file(out_path, transmute([]byte)strings.to_string(sb)) == nil
}

@(require_results)
cpp_match_for_odin :: proc(odin_name: string, cpp_map: map[string]Hugi_Record, allocator := context.allocator) -> string {
	cand := odin_name
	if cr, ok := cpp_map[cand]; ok && cr.p50_ns > 0 {
		return strings.clone(cand, allocator)
	}
	if strings.has_suffix(odin_name, "_st") {
		base := odin_name[:len(odin_name) - 3]
		if _, ok := cpp_map[base]; ok {
			return strings.clone(base, allocator)
		}
	}
	return ""
}

@(require_results)
build_suite_report_view :: proc(
	odin_recs: []Hugi_Record,
	cpp_recs: []Hugi_Record,
	odin_commit: string,
	cpp_commit: string,
	allocator := context.allocator,
) -> Suite_Report_View {
	cpp_map := make(map[string]Hugi_Record, len(cpp_recs), context.temp_allocator)
	for rec in cpp_recs { cpp_map[rec.name] = rec }

	grouped := make(map[string][dynamic]Hugi_Record, context.temp_allocator)
	extra := make([dynamic]string, context.temp_allocator)
	total_samples := u64(0)
	for rec in odin_recs {
		total_samples += u64(max(rec.samples, 0))
		g := rec.group_name if len(rec.group_name) > 0 else "default"
		if g not_in grouped {
			grouped[g] = make([dynamic]Hugi_Record, context.temp_allocator)
			known := false
			for gm in GROUP_META { if gm.key == g { known = true; break } }
			if !known { append(&extra, g) }
		}
		append(&grouped[g], rec)
	}
	ordered := make([dynamic]string, 0, len(grouped), context.temp_allocator)
	for gm in GROUP_META { if gm.key in grouped { append(&ordered, gm.key) } }
	for eg in extra { append(&ordered, eg) }

	groups := make([]Report_Group_View, len(ordered), allocator)
	for gkey, gi in ordered {
		recs := grouped[gkey][:]
		title, desc := gkey, ""
		for gm in GROUP_META { if gm.key == gkey { title, desc = gm.title, gm.desc; break } }
		rows := make([]Report_Row_View, len(recs), allocator)
		for rec, ri in recs {
			alloc_str: string
			if rec.allocs_per_op > 0 || rec.alloc_bytes_per_op > 0 {
				alloc_str = fmt.aprintf("%.1f (%s)", rec.allocs_per_op, fmt_bytes(rec.alloc_bytes_per_op, context.temp_allocator), allocator = allocator)
			} else {
				alloc_str = strings.clone("**0**", allocator)
			}
			vs_cpp := strings.clone("—", allocator)
			cpp_key := cpp_match_for_odin(rec.name, cpp_map, context.temp_allocator)
			if len(cpp_key) > 0 {
				if cr, ok := cpp_map[cpp_key]; ok && cr.p50_ns > 0 && rec.p50_ns > 0 {
					vs_cpp = fmt.aprintf("**%.2fx** (%s)", cr.p50_ns / rec.p50_ns, fmt_ns(cr.p50_ns, context.temp_allocator), allocator = allocator)
				}
			}
			rows[ri] = Report_Row_View{
				name   = strings.clone(rec.name, allocator),
				median = fmt_ns(rec.p50_ns, allocator),
				p95    = fmt_ns(rec.p95_ns, allocator),
				ops    = fmt_tp_or_ops(rec, allocator),
				cv     = fmt.aprintf("%.1f%%", rec.cv_pct, allocator = allocator),
				vs_cpp = vs_cpp,
				allocs = alloc_str,
			}
		}
		groups[gi] = Report_Group_View{
			title    = strings.clone(title, allocator),
			desc     = strings.clone(desc, allocator),
			has_desc = len(desc) > 0,
			count    = len(recs),
			rows     = rows,
		}
	}
	return Suite_Report_View{
		commit            = strings.clone(odin_commit, allocator),
		cpp_commit        = strings.clone(cpp_commit, allocator),
		total_benchmarks  = len(odin_recs),
		cpp_count         = len(cpp_recs),
		total_samples_str = fmt_commas_u64(total_samples, allocator),
		group_count       = len(groups),
		groups            = groups,
	}
}

@(require_results)
render_all :: proc(db_path: string, allocator := context.allocator) -> bool {
	if !os.exists(db_path) {
		return false
	}
	arena: vmem.Arena
	if vmem.arena_init_growing(&arena) != nil { return false }
	defer vmem.arena_destroy(&arena)
	arena_alloc := vmem.arena_allocator(&arena)

	raw_odin, ok_odin := exec_hugi({"export", "--db", db_path, "--lang", "odin", "--format", "json", "--out", "-"}, arena_alloc)
	if !ok_odin { return false }
	odin_recs: []Hugi_Record
	if json.unmarshal(raw_odin, &odin_recs, allocator = arena_alloc) != nil || len(odin_recs) == 0 {
		return false
	}

	raw_cpp, ok_cpp := exec_hugi({"export", "--db", db_path, "--lang", "cpp", "--format", "json", "--out", "-"}, arena_alloc)
	if !ok_cpp { return false }
	cpp_recs: []Hugi_Record
	if json.unmarshal(raw_cpp, &cpp_recs, allocator = arena_alloc) != nil || len(cpp_recs) == 0 {
		return false
	}

	if !os.exists(RENDERS_DIR) && os.make_directory(RENDERS_DIR) != nil {
		return false
	}

	odin_map := make(map[string]Hugi_Record, len(odin_recs), arena_alloc)
	for rec in odin_recs { odin_map[rec.name] = rec }
	cpp_map := make(map[string]Hugi_Record, len(cpp_recs), arena_alloc)
	for rec in cpp_recs { cpp_map[rec.name] = rec }

	odin_commit := odin_recs[0].commit
	cpp_commit := cpp_recs[0].commit
	sub_cmp := fmt.tprintf("Skadi %s vs Arrow C++ %s · -o:speed -microarch:native", odin_commit, cpp_commit)

	p1 := [?]Pair_Spec{
		{"array_build_10m_i32", "array_build_10m_i32", "array_build_10m_i32"},
		{"sum_10m_f64_st", "sum_10m_f64", "sum_10m_f64 (ST)"},
		{"sum_10m_f64_nulls_st", "sum_10m_f64_nulls", "sum_10m_f64_nulls (ST)"},
		{"sum_10m_i32_st", "sum_10m_i32", "sum_10m_i32 (ST)"},
		{"min_max_10m_i32_st", "min_max_10m_i32", "min_max_10m_i32 (ST)"},
		{"filter_10m_i32_st", "filter_10m_i32", "filter_10m_i32 (ST)"},
		{"filter_10m_i32_sparse_st", "filter_10m_i32_sparse", "filter_10m_i32_sparse (ST)"},
		{"compare_gt_10m_i32", "compare_gt_10m_i32", "compare_gt_10m_i32"},
		{"arith_add_10m_f64", "arith_add_10m_f64", "arith_add_10m_f64"},
		{"cast_10m_i32_f64", "cast_10m_i32_f64", "cast_10m_i32_f64"},
		{"sort_indices_1m_i32", "sort_indices_1m_i32", "sort_indices_1m_i32"},
	}
	if !render_grouped_bar_svg("Skadi (Odin) vs Arrow C++: Builders & Compute Kernels", sub_cmp, p1[:], odin_map, cpp_map, fmt.tprintf("%s/odin_vs_cpp_compute.svg", RENDERS_DIR), arena_alloc) {
		return false
	}

	p2 := [?]Pair_Spec{
		{"string_build_1m", "string_build_1m", "string_build_1m"},
		{"string_scan_1m", "string_scan_1m", "string_scan_1m"},
		{"filter_1m_string_st", "filter_1m_string", "filter_1m_string (ST)"},
		{"sort_indices_100k_str", "sort_indices_100k_str", "sort_indices_100k_str"},
		{"sum_where_10m_f64", "sum_where_10m_f64", "sum_where_10m_f64"},
		{"agg_where_10m_f64", "agg_where_10m_f64", "agg_where_10m_f64"},
		{"sel_2pred_sum_10m_f64", "sel_2pred_sum_10m_f64", "sel_2pred_sum_10m_f64"},
		{"dict_vc_1m_str", "dict_vc_1m_str", "dict_vc_1m_str"},
		{"umbra_sort_100k_str", "umbra_sort_100k_str", "umbra_sort_100k_str"},
		{"ipc_write_10m_i32", "ipc_write_10m_i32", "ipc_write_10m_i32"},
		{"ipc_roundtrip_10m_i32", "ipc_roundtrip_10m_i32", "ipc_roundtrip_10m_i32"},
	}
	if !render_grouped_bar_svg("Skadi (Odin) vs Arrow C++: Fusion, Strings & IPC", sub_cmp, p2[:], odin_map, cpp_map, fmt.tprintf("%s/odin_vs_cpp_fusion_strings_ipc.svg", RENDERS_DIR), arena_alloc) {
		return false
	}

	sub_mt := fmt.tprintf("Skadi %s · Multi-Threaded & Sleipnir Actor Pipelines", odin_commit)
	s3 := [?]Bar_Spec{
		{"filter_10m_i32_sparse_mt", "filter_10m_i32_sparse (MT)", PALETTE[1]},
		{"sum_10m_i32_mt", "sum_10m_i32 (MT)", PALETTE[1]},
		{"min_max_10m_i32_mt", "min_max_10m_i32 (MT)", PALETTE[1]},
		{"dict_vc_1m_str_actor", "dict_vc_1m_str (Actor)", PALETTE[5]},
		{"sum_10m_f64_nulls_mt", "sum_10m_f64_nulls (MT)", PALETTE[5]},
		{"sum_10m_f64_mt", "sum_10m_f64 (MT)", PALETTE[5]},
		{"agg_where_10m_f64_actor", "agg_where_10m_f64 (Actor)", PALETTE[0]},
		{"filter_10m_i32_mt", "filter_10m_i32 (MT)", PALETTE[0]},
		{"sel_2pred_sum_10m_f64_mt", "sel_2pred_sum (MT)", PALETTE[0]},
		{"sel_2pred_sum_10m_f64_actor", "sel_2pred_sum (Actor)", PALETTE[3]},
		{"filter_1m_string_mt", "filter_1m_string (MT)", PALETTE[4]},
		{"take_1m_i32_mt", "take_1m_i32 (MT)", PALETTE[4]},
		{"sort_indices_1m_i32_actor", "sort_indices_1m_i32 (Actor)", PALETTE[2]},
		{"umbra_sort_100k_str_actor", "umbra_sort_100k_str (Actor)", PALETTE[2]},
		{"sort_indices_100k_str_actor", "sort_indices_100k_str (Actor)", PALETTE[6]},
	}
	if !render_bar_svg("Multi-Threaded & Sleipnir Actor Pipeline Kernels", sub_mt, s3[:], odin_map, fmt.tprintf("%s/actor_and_mt_speedups.svg", RENDERS_DIR), arena_alloc) {
		return false
	}

	rep := build_suite_report_view(odin_recs, cpp_recs, odin_commit, cpp_commit, arena_alloc)
	sb := strings.builder_make(0, suite_report_tmpl.approx_bytes, allocator)
	defer strings.builder_destroy(&sb)
	if _, err := suite_report_tmpl.with(strings.to_writer(&sb), rep); err != nil { return false }
	if os.write_entire_file(README_PATH, transmute([]byte)strings.to_string(sb)) != nil { return false }

	fmt.printfln("bench_render: rendered 3 SVGs and %d Odin (+%d C++) rows -> %s", rep.total_benchmarks, rep.cpp_count, README_PATH)
	free_all(context.temp_allocator)
	return true
}

main :: proc() {
	if !render_all(DEFAULT_DB_PATH, context.allocator) {
		os.exit(1)
	}
}
