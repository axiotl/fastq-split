use clap::Parser;
use flate2::read::GzDecoder;
use regex::Regex;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;

static LINES_PER_READ: usize = 4;

enum ReadType {
    R1,
    R2,
}

fn get_reader(path: PathBuf) -> Box<dyn BufRead> {
    let sample = File::open(&path).unwrap();
    let regex = Regex::new(r"\.gz$").unwrap();
    if regex.is_match(&path.to_string_lossy()) {
        let decoder = GzDecoder::new(sample);
        return Box::new(BufReader::new(decoder));
    }
    Box::new(BufReader::new(sample))
}

fn get_file_name(index: usize, read_type: &ReadType, prefix: &str) -> String {
    match read_type {
        ReadType::R1 => format!("{}_{}_R1.fastq", prefix, index),
        ReadType::R2 => format!("{}_{}_R2.fastq", prefix, index),
    }
}

fn chunk_half(
    path: &PathBuf,
    output_dir: &PathBuf,
    lines_per_chunk: usize,
    read_type: ReadType,
    prefix: String,
) {
    let reader = get_reader(path.to_path_buf());
    let mut lines_read = 0;
    let mut chunk_index = 0;
    let mut output_path = PathBuf::from(output_dir);
    output_path.push(get_file_name(chunk_index, &read_type, &prefix));
    if !output_dir.exists() {
        std::fs::create_dir_all(&output_dir).unwrap();
    }
    let mut writer = File::create(&output_path).unwrap();
    for line in reader.lines() {
        writer.write_all(line.unwrap().as_bytes()).unwrap();
        writer.write_all(b"\n").unwrap();
        lines_read += 1;
        if lines_read == lines_per_chunk {
            chunk_index += 1;
            lines_read = 0;
            output_path.pop();
            output_path.push(get_file_name(chunk_index, &read_type, &prefix));
            writer = File::create(&output_path).unwrap();
        }
    }
}

/// Chunk fastq files into smaller files
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to r1 fastq file
    #[arg(long)]
    r1: String,

    /// Path to r2 fastq file
    #[arg(long)]
    r2: String,

    /// Number of reads per chunk
    #[arg(short, long, default_value_t = 1000)]
    size: usize,

    /// File prefix for output
    #[arg(short, long, default_value = "chunk")]
    prefix: String,

    /// Output directory
    #[arg(short, long, default_value = "chunks")]
    output: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let r1 = PathBuf::from(&args.r1);
    let r2 = PathBuf::from(&args.r2);

    let lines_per_chunk = args.size * LINES_PER_READ;
    let output_dir_r1 = PathBuf::from(&args.output);
    let output_dir_r2 = output_dir_r1.clone();

    let prefix_r1 = args.prefix;
    let prefix_r2 = prefix_r1.clone();

    let handle1 = tokio::spawn(async move {
        chunk_half(
            &r1,
            &output_dir_r1.clone(),
            lines_per_chunk,
            ReadType::R1,
            prefix_r1,
        );
    });

    let handle2 = tokio::spawn(async move {
        chunk_half(
            &r2,
            &output_dir_r2.clone(),
            lines_per_chunk,
            ReadType::R2,
            prefix_r2,
        );
    });

    handle1.await.unwrap();
    handle2.await.unwrap();
}
