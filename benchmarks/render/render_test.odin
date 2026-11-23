package bench_render

import "core:strings"
import "core:testing"

@(test)
test_render_report_and_grouped_bar :: proc(t: ^testing.T) {
	odin_recs := [?]Hugi_Record{
		{commit = "6bef9d5", lang = "odin", group_name = "compute", name = "sum_10m_f64_st", full_name = "sum_10m_f64_st", samples = 7, p50_ns = 0.36, p95_ns = 0.40, cv_pct = 2.0, mb_per_sec = 21331.8, allocs_per_op = 0, alloc_bytes_per_op = 0},
	}
	cpp_recs := [?]Hugi_Record{
		{commit = "6bef9d509d5b", lang = "cpp", group_name = "compute", name = "sum_10m_f64", full_name = "sum_10m_f64", samples = 7, p50_ns = 0.47, p95_ns = 0.50, cv_pct = 2.0, mb_per_sec = 17438.7, allocs_per_op = 0, alloc_bytes_per_op = 0},
	}

	rep := build_suite_report_view(odin_recs[:], cpp_recs[:], "6bef9d5", "6bef9d509d5b", context.temp_allocator)
	testing.expect_value(t, rep.total_benchmarks, 1)
	testing.expect_value(t, rep.cpp_count, 1)
	testing.expect(t, strings.contains(rep.groups[0].rows[0].vs_cpp, "1.31x"), "computes C++ speedup ratio")

	sb := strings.builder_make(0, 1024, context.temp_allocator)
	_, err := suite_report_tmpl.with(strings.to_writer(&sb), rep)
	testing.expect(t, err == nil, "suite_report template renders without error")
	out := strings.to_string(sb)
	testing.expect(t, strings.contains(out, "<!-- BEGIN_BENCHMARK_TABLE -->"), "has benchmark table marker")
	free_all(context.temp_allocator)
}
