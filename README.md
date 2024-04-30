# 1brc.rs

This is a Rust solution to the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc), which involves
reading one billion rows of data and producing some aggregations.

This solution runs in about 2.5 seconds on an M1 Pro, with results verified against the baseline Java implementation.
For comparison, the baseline Java implementation runs for 240 seconds on the same machine.

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

All slice access is using unchecked variants, which is faster because the bounds checks are skipped, but also means the
program will probably segfault if there is a logic bug. I also assume the input is safe utf-8, and
use `str::from_utf8_unchecked` to avoid the overhead of checking each byte.

Because temperature readings are limited to ±99.9, I'm reading just the digits without the decimal point and parse those
into an `i16`, effectively scaling the reading up by 10. This is faster than floating point numbers both for parsing and
for aggregation. In fact, I only use floating point numbers to calculate the mean.

Going even further, I have implemented my own `i16` parser that parses directly from the bytes, skipping over the
decimal point, and assumes the data is valid. This is a 20-25% speedup over copying just the digits into a 4-byte array
and using `str::parse`.

Results are stored in `AHashMap` instances, using string slices as keys to avoid allocations. I have tried various
different hash functions, and this seems to be the fastest one for this particular workload. It's important to note that
raw hashing speed isn't the only thing that matters, quality of the resulting hash affects hash map lookup performance
as well. With higher quality hashes, the map can arrive at the right data faster.

All heap-allocated data structures are pre-allocated to avoid repeated allocations.

Sorting is done last, as the resulting dataset is comparatively small at ≤ 10k elements. I use unstable sorting because
it's faster, and I know for a fact we don't have equal elements.

To avoid locking overhead, `stdout` is locked once while I write out all the results.

## Things I Tried That Didn't Work

For a long time I was actually using floating point numbers for min/mean/max, but using integers and scaling by 10 is
faster.

Rayon has parallel `extend` and `sort` methods for vectors, but for the number of unique stations we have, those are
actually slower than just doing the work on one thread.

Using `BTreeMap` to avoid filling a vector at the end to get a sorted list of stations is much slower than the hash map
approach. I didn't expect it to be faster, but I was surprised by how big the difference was. Turns out SwissTable with
a good hash function is really fast.

Buffering the output is slower than not to, probably because we're writing just one big line anyway, so it wouldn't
flush before we're done writing anyway.

In a personal first, I have tried profile-guided optimization (via `cargo pgo`), but the result was actually slower than
a regular release build.

I'm aware that there are tricks to scan the input lines faster using branchless code, but I haven't been
able to derive that myself yet. I've tried both memchr and Stringzilla to make use of vectorization, but both were
slower than my current approach.

I have tried replacing various of the hot increments by one with unchecked variants, but without having looked at the
assembly I'm suspecting that the compiler already elides those checks because it can prove that they're in bounds.