#[macro_use] extern crate structopt;
#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate csv;
extern crate core;

use structopt::StructOpt;
use std::path::PathBuf;
use std::env;
use std::process;
use std::path::Path;
use csv::Reader;
use serde::Deserialize;
use std::fmt::Debug;
use serde::de::DeserializeOwned;
use serde::Deserializer;
use std::str::FromStr;

#[derive(StructOpt, Debug)]
struct Arguments {
    /// Do not download anything, just print reference information.
    #[structopt(short = "p", long = "print")]
    print: bool,

    /// The location of the CSV files. If not set, assumed to
    /// be the current directory.
    #[structopt(name = "data_directory", parse(from_os_str))]
    data_directory: Option<PathBuf>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Index {
    id: u32,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Source {
    id: u32,
    name: String,
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Sector {
    id: u32,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Stock {
    id: u32,
    symbol: String,
    name: String,
    yahoo_symbol: String,
    digital_look_name: String,
    csi: Option<u32>,
    source_id: u32
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Price {
    id: u32,
    stock_id: u32,
    date: String,
    price: f32,
    prev_price: f32,
    #[serde(deserialize_with = "deserialize_optional")]
    fifty_two_week_high: Option<f32>,
    #[serde(deserialize_with = "deserialize_optional")]
    fifty_two_week_low: Option<f32>,
    #[serde(deserialize_with = "deserialize_optional")]
    market_cap_in_millions: Option<f32>,
    #[serde(deserialize_with = "deserialize_optional")]
    sector_id: Option<u32>,
    #[serde(deserialize_with = "deserialize_optional")]
    index_id: Option<u32>,
}

fn main() {
    let args = Arguments::from_args();

    let data_dir = args.data_directory.unwrap_or(
        env::current_dir().expect("Could not determine current directory. Try passing the data directory explicitly."));

    must_exist(&data_dir);
    if !data_dir.is_dir() {
        eprintln!("The data directory {:?} is not a directory.", data_dir);
        process::exit(1);
    }

    println!("Starting to read data files from {:?}", data_dir);

    let mut file = data_dir.clone();
    file.push("index.csv");
    let stock_indexes: Vec<Index> = read_csv(&file, args.print).expect("Could not read index.csv");

    let mut file = data_dir.clone();
    file.push("source.csv");
    let stock_sources: Vec<Source> = read_csv(&file, args.print).expect("Could not read source.csv");

    let mut file = data_dir.clone();
    file.push("sector.csv");
    let stock_sectors: Vec<Sector> = read_csv(&file, args.print).expect("Could not read sector.csv");

    let mut file = data_dir.clone();
    file.push("stock.csv");
    let stocks: Vec<Stock> = read_csv(&file, args.print).expect("Could not read stock.csv");

    let mut file = data_dir.clone();
    file.push("price.csv");
    let mut prices: Vec<Price> = read_csv(&file, args.print).expect("Could not read price.csv");

    println!("Data files read successfully. Beginning stock price download.");
}

fn read_csv<T: Debug + DeserializeOwned>(path: &Path, print: bool) -> std::io::Result<Vec<T>>
{
    must_exist_and_be_file(&path);
    print!("Reading {:?}...", path);

    let mut rdr = Reader::from_path(path)?;
    let mut results: Vec<T> = Vec::new();

    for result in rdr.deserialize() {
        let record: T = result?;
        if print {
            println!("{:?}", record);
        }
        results.push(record);
    }

    println!("done.");

    Ok(results)
}

fn must_exist(path: &Path) {
    if !path.exists() {
        eprintln!("The path {:?} does not exist.", path);
        process::exit(1);
    }
}

fn must_be_file(path: &Path) {
    if !path.exists() {
        eprintln!("The path {:?} is not a file.", path);
        process::exit(1);
    }
}

fn must_exist_and_be_file(path: &Path) {
    must_exist(path);
    must_be_file(path);
}

fn deserialize_optional<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
    where D: serde::Deserializer<'de>,
          T: FromStr
{
    use serde::Deserialize;
    use serde::de::Error;

    let s: &str = Deserialize::deserialize(de)?;

    if s.is_empty() || s == "NULL" || s == "null" || s == "Null" {
        return Ok(None);
    } else {
        match s.parse::<T>() {
            Ok(parsed_value) => return Ok(Some(parsed_value)),
            Err(e) => return Err(D::Error::custom(format!("Could not parse '{}' into the desired type.", s)))
        }
    }
}
