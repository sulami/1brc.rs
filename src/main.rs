use std::{env::args, fs::File, io::stdout, io::Write, time::Instant};

use ahash::AHashMap;
use memmap2::Mmap;
use rayon::prelude::*;

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
        .reduce_with(merge_results)
        .unwrap();

    // The challenge states that there are at most 10_000 cities, so we can pre-allocate.
    let mut result = Vec::with_capacity(10_000);
    result.extend(cities);
    let result_count = result.len();
    result.sort_unstable_by_key(|x| x.0);

    let mut lock = stdout().lock();
    write!(lock, "{{").unwrap();
    for (idx, (city, entry)) in result.into_iter().enumerate() {
        let mean = (entry.sum as f64 / entry.count as f64).round() as i16;
        write!(lock, "{}=", unsafe { std::str::from_utf8_unchecked(city) }).unwrap();
        write_i16_as_float(&mut lock, entry.min);
        write!(lock, "/").unwrap();
        write_i16_as_float(&mut lock, mean);
        write!(lock, "/").unwrap();
        write_i16_as_float(&mut lock, entry.max);
        if idx != result_count - 1 {
            write!(lock, ",").unwrap();
        }
    }
    writeln!(lock, "}}").unwrap();

    let elapsed = start.elapsed();
    eprintln!("Elapsed: {} ms", elapsed.as_millis());
}

fn process_chunk(input: &[u8], from: usize, to: usize) -> AHashMap<&[u8], Entry> {
    let mut head = from;

    // If starting in the middle, skip the first complete line, move head to the first character of
    // the next line. The previous chunk will include the line that straddles the boundary.
    if head != 0 {
        while unsafe { *input.get_unchecked(head) } != b'\n' {
            head += 1;
        }
        head += 1
    };

    // The challenge states that there are at most 10_000 cities, so we can pre-allocate.
    let mut cities: AHashMap<&[u8], Entry> = AHashMap::default();
    cities.reserve(10_000);

    while head < to {
        // We know the first byte on the line has to be a name, so we don't need to look at it yet.
        let mut tail = head + 1;

        // We first search for the semicolon, which is the end of the city name.
        while unsafe { input.get_unchecked(tail) } != &b';' {
            tail += 1;
        }
        let semicolon = tail;

        // Get the city name.
        let city = unsafe { input.get_unchecked(head..semicolon) };

        // After the semicolon, there are 3-5 bytes of temperature reading, depending on the sign
        // and the number of digits. Step onto the first of those bytes.
        tail += 1;

        // Parse the temperature reading into tenths of degrees.
        let reading = parse_i16(input, &mut tail);

        // Add the new reading.
        insert_reading(&mut cities, city, reading);

        // Move head onto the first character of the next line.
        head = tail + 1;
    }

    cities
}

#[inline]
fn merge_results<'a>(
    mut a: AHashMap<&'a [u8], Entry>,
    b: AHashMap<&'a [u8], Entry>,
) -> AHashMap<&'a [u8], Entry> {
    b.into_iter().for_each(|(city, entry)| {
        upsert_entry(&mut a, city, entry);
    });
    a
}

#[inline]
fn upsert_entry<'a>(cities: &mut AHashMap<&'a [u8], Entry>, city: &'a [u8], entry: Entry) {
    if let Some(Entry {
        ref mut min,
        ref mut max,
        ref mut sum,
        ref mut count,
    }) = cities.get_mut(city)
    {
        *min = (*min).min(entry.min);
        *max = (*max).max(entry.max);
        *sum += entry.sum;
        *count += entry.count;
    } else {
        cities.insert(city, entry);
    }
}

#[inline]
fn insert_reading<'a>(cities: &mut AHashMap<&'a [u8], Entry>, city: &'a [u8], reading: i16) {
    cities
        .entry(city)
        .and_modify(
            |Entry {
                 min,
                 max,
                 sum,
                 count,
             }| {
                *min = (*min).min(reading);
                *max = (*max).max(reading);
                *sum += reading as i64;
                *count += 1;
            },
        )
        .or_insert_with(|| Entry {
            min: reading,
            max: reading,
            sum: reading as i64,
            count: 1,
        });
}

struct Entry {
    /// Minimum reading, in tenths of a degree (x10).
    min: i16,
    /// Maximum reading, in tenths of a degree (x10).
    max: i16,
    /// Sum of all readings, in tenths of a degree (x10).
    sum: i64,
    /// Number of readings.
    count: u32,
}

/// Writes an i16 as a float with one decimal digit.
#[inline]
fn write_i16_as_float(mut destination: impl Write, value: i16) {
    let abs_value = value.abs();
    write!(
        destination,
        "{sign}{int}.{frac}",
        sign = if value < 0 { "-" } else { "" },
        int = abs_value / 10,
        frac = abs_value % 10
    )
    .unwrap();
}

/// Parses a byte slice as i16, assuming it's non-empty and valid.
/// Skips over the decimal point and records exactly one fractional digit.
/// Uses the passed ptr reference into the input as the read head.
/// We really, really need this to be inlined, and rustc makes us ask for it.
#[inline(always)]
fn parse_i16(input: &[u8], ptr: &mut usize) -> i16 {
    // Check if the first byte is a minus. If so, record that fact and step ahead.
    let negative = unsafe { *input.get_unchecked(*ptr) } == b'-';
    if negative {
        *ptr += 1;
    };

    // Read the temperature reading digit by digit, assuming they're valid.
    let mut reading = 0_i16;
    loop {
        reading *= 10;
        let byte = unsafe { *input.get_unchecked(*ptr) };
        if byte == b'.' {
            // If we find the decimal point, we know there is only one more digit to go.
            // We actually skip the decimal point because we record tenths of degrees to
            // avoid floating point operations.
            reading += unsafe { *input.get_unchecked(*ptr + 1) as i16 } - 48;
            *ptr += 2;
            break;
        } else {
            reading += (byte as i16) - 48;
            *ptr += 1;
        }
    }

    if negative {
        reading *= -1;
    }

    reading
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_i16_as_float() {
        let mut buf = Vec::new();
        write_i16_as_float(&mut buf, 123);
        assert_eq!(buf, b"12.3");
    }

    #[test]
    fn test_write_negative_i16_as_float() {
        let mut buf = Vec::new();
        write_i16_as_float(&mut buf, -123);
        assert_eq!(buf, b"-12.3");
    }

    #[test]
    fn test_write_small_negative_i16_as_float() {
        let mut buf = Vec::new();
        write_i16_as_float(&mut buf, -1);
        assert_eq!(buf, b"-0.1");
    }

    #[test]
    fn test_process_chunk_one_line() {
        let input = b"City;-12.3\n";
        let cities = process_chunk(input, 0, input.len());
        assert_eq!(cities.len(), 1);
        let entry = cities.get(&input[0..4]).unwrap();
        assert_eq!(entry.min, -123);
        assert_eq!(entry.max, -123);
        assert_eq!(entry.sum, -123);
        assert_eq!(entry.count, 1);
    }

    #[test]
    fn test_process_chunk_two_lines() {
        let input = b"City1;-12.3\nCity2;12.3\n";
        let cities = process_chunk(input, 0, input.len());
        assert_eq!(cities.len(), 2);
        let entry = cities.get(&input[0..5]).unwrap();
        assert_eq!(entry.min, -123);
        assert_eq!(entry.max, -123);
        assert_eq!(entry.sum, -123);
        assert_eq!(entry.count, 1);
        let entry = cities.get(&input[12..17]).unwrap();
        assert_eq!(entry.min, 123);
        assert_eq!(entry.max, 123);
        assert_eq!(entry.sum, 123);
        assert_eq!(entry.count, 1);
    }

    #[test]
    fn test_process_chunk_two_lines_same_city() {
        let input = b"City;-1.2\nCity;0.0\n";
        let cities = process_chunk(input, 0, input.len());
        assert_eq!(cities.len(), 1);
        let entry = cities.get(&input[0..4]).unwrap();
        assert_eq!(entry.min, -12);
        assert_eq!(entry.max, 0);
        assert_eq!(entry.sum, -12);
        assert_eq!(entry.count, 2);
    }

    #[test]
    fn test_parse_i16() {
        assert_eq!(123, parse_i16(b"12.3", &mut 0));
        assert_eq!(-123, parse_i16(b"-12.3", &mut 0));
    }

    #[test]
    fn test_parse_i16_updates_ptr() {
        let mut ptr = 0;
        parse_i16(b"1.1\nfoo", &mut ptr);
        assert_eq!(3, ptr);
    }
}
