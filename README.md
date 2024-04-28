# 1brc.rs

This is a Rust solution to the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc), which involves
reading one billion rows of data and producing some aggregations.

This solution runs in about four seconds on an M1 Pro, with results verified against the reference implementation.

While the original challenge is limited to Java, and no external libraries, this is Rust, and a small selection of
libraries have been used. One could copy or reimplement the code, but I don't see the point.

Nothing fancy about building it, just run `cargo build --release` and then run the binary with the path to the data file
as the first argument.

## Implementation Notes

The obvious first optimization is to memory-map the file instead of actually reading it. In fact, for most of this
program I only deal in pointers to the memory-mapped file.

The parsing and aggregation is parallelized using Rayon, splitting the input into equal chunks. Each thread aggregates
its own result set to avoid locking or other kinds of synchronization, with a subsequent merge step.

When parsing input lines, I opportunistically skip ahead when searching for the semicolon or the end of the line, which
is surprisingly effective. I also only do one scan over every line to find the bounds of the values before parsing. Part
of what makes this fast is looking at as few bytes as possible. Skipping a single byte on every line improves
performance by about 5%.

I also assume the input is safe utf-8, and use `str::from_utf8_unchecked` to avoid the overhead of checking each byte.

I use an external library to slightly improve float parsing performance, though the gains are marginal. While the
reference uses doubles throughout, I use `f32` for min and max to reduce the memory footprint as they are bounded to
±99.9. I have tried using `i16` to store the value times 10, but the back-and-forth conversions actually cost more than
the reduction in size saves. The idea was that `Entry` could be brought down to 128 bytes, which would be two cache
lines, but it didn't pan out.

Results are stored in `FxHashMap` instances, using string slices as keys to avoid allocations. I have tried various
different hash functions, and this seems to be the fastest one for this particular workload.

All heap-allocated data structures are pre-allocated to avoid repeated allocations.

Sorting is done last, as the resulting dataset is comparatively small at ≤ 10k elements. I use unstable sorting because
it's faster, and I know for a fact we don't have equal elements.

To avoid locking overhead, `stdout` is locked once while I write out all the results.