use std::{env::args, fs::File, io::stdout, io::Write, time::Instant};

use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

/// Number of threads to use for processing the input.
/// This should be adjusted based on the number of cores available.
const THREADS: usize = 10;

fn main() {
    let start = Instant::now();

    let path = args().nth(1).expect("missing input file");
    let fp = File::open(path).expect("failed to open input file");
    let input = unsafe { Mmap::map(&fp).expect("failed to map file") };

    let chunk_size = input.len() / THREADS;
    let cities = (0..THREADS)
        .into_par_iter()
        .map(|thread| process_chunk(&input, thread * chunk_size, (1 + thread) * chunk_size))
        .reduce(
            || {
                let mut map = FxHashMap::default();
                map.reserve(10_000);
                map
            },
            merge_results,
        );

    // Beyond this point we're dealing with only 10k elements, at which point Rayon's parallel
    // iterators incur more overhead than they gain, so single-threaded operations are used.

    // The challenge states that there are at most 10_000 cities, so we can pre-allocate.
    let mut result = Vec::with_capacity(10_000);
    result.extend(cities);
    let result_count = result.len();
    result.sort_unstable_by_key(|x| x.0);

    let mut lock = stdout().lock();
    write!(lock, "{{").unwrap();
    result.into_iter().enumerate().for_each(
        |(
            idx,
            (
                city,
                Entry {
                    min,
                    max,
                    count,
                    sum,
                },
            ),
        )| {
            let mut mean = (sum / count as f64 * 10.).round() / 10.;
            // Round negative zero to positive zero to match Java behaviour.
            if mean == -0. {
                mean = 0.;
            }
            write!(
                lock,
                "{}={}/{}/{}{}",
                unsafe { std::str::from_utf8_unchecked(city) },
                min,
                mean,
                max,
                if idx == result_count - 1 { "" } else { "," }
            )
            .unwrap();
        },
    );
    writeln!(lock, "}}").unwrap();

    let elapsed = start.elapsed();
    eprintln!("Elapsed: {} ms", elapsed.as_millis());
}

fn process_chunk(input: &Mmap, from: usize, to: usize) -> FxHashMap<&[u8], Entry> {
    let mut head = from;

    // If starting in the middle, skip the first complete line, move head to the first character of
    // the next line. The previous chunk will include the line that straddles the boundary.
    if head != 0 {
        while input[head] != b'\n' {
            head += 1;
        }
        head += 1
    };

    // The challenge states that there are at most 10_000 cities, so we can pre-allocate.
    let mut cities: FxHashMap<&[u8], Entry> = FxHashMap::default();
    cities.reserve(10_000);

    while head < to {
        // We know the first byte on the line has to be a name, so we don't need to look at it.
        let mut tail = head + 1;

        // We then search first for the semicolon.
        let mut semicolon = 0;
        loop {
            match input[tail] {
                b';' => {
                    semicolon = tail;
                    // After the semicolon, there are at least three bytes of temperature reading.
                    // We continue searching for the end of the line.
                    tail += 4;
                }
                b'\n' => {
                    break;
                }
                _ => {
                    tail += 1;
                }
            }
        }

        let city = &input[head..semicolon];
        let reading = fast_float::parse(&input[semicolon + 1..tail]).unwrap();

        upsert_entry(
            &mut cities,
            city,
            Entry {
                min: reading,
                max: reading,
                sum: reading as f64,
                count: 1,
            },
        );

        // Move head onto the first character of the next line.
        head = tail + 1;
    }

    cities
}

#[inline]
fn merge_results<'a>(
    mut a: FxHashMap<&'a [u8], Entry>,
    b: FxHashMap<&'a [u8], Entry>,
) -> FxHashMap<&'a [u8], Entry> {
    b.into_iter().for_each(|(city, entry)| {
        upsert_entry(&mut a, city, entry);
    });
    a
}

#[inline]
fn upsert_entry<'a>(cities: &mut FxHashMap<&'a [u8], Entry>, city: &'a [u8], entry: Entry) {
    if let Some(Entry {
        ref mut min,
        ref mut max,
        ref mut sum,
        ref mut count,
    }) = cities.get_mut(city)
    {
        *min = min.min(entry.min);
        *max = max.max(entry.max);
        *sum += entry.sum;
        *count += entry.count;
    } else {
        cities.insert(city, entry);
    }
}

struct Entry {
    min: f32,
    max: f32,
    sum: f64,
    count: u32,
}
