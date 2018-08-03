#[macro_use] extern crate structopt;
#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate csv;
extern crate core;
extern crate reqwest;
extern crate scraper;
extern crate kuchiki;

use structopt::StructOpt;
use std::path::PathBuf;
use std::env;
use std::process;
use std::path::Path;
use csv::Reader;
use std::fmt::Debug;
use serde::de::DeserializeOwned;
use std::str::FromStr;
use scraper::{Html, Selector};
use core::fmt;
use std::error;
use kuchiki::traits::*;
use kuchiki::NodeRef;
use kuchiki::NodeDataRef;
use kuchiki::ElementData;

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

#[derive(Debug)]
enum StockPriceError {
    CannotParseDocument(String),
    Request(reqwest::Error)
}

impl fmt::Display for StockPriceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StockPriceError::CannotParseDocument(ref msg) => write!(f, "Cannot parse document, {}", msg),
            StockPriceError::Request(ref e) => std::fmt::Display::fmt(e, f)
        }
    }
}

impl error::Error for StockPriceError {
    fn description(&self) -> &str {
        match *self {
            StockPriceError::CannotParseDocument(ref msg) => "Cannot parse document",
            // This already impls `Error`, so defer to its own implementation.
            StockPriceError::Request(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            StockPriceError::CannotParseDocument(ref msg) => None,
            // The cause is the underlying implementation error type. Is implicitly
            // cast to the trait object `&error::Error`. This works because the
            // underlying type already implements the `Error` trait.
            StockPriceError::Request(ref e) => Some(e),
        }
    }
}

// Implement the conversion from `reqwest::Error` to `StockPriceError`.
// This will be automatically called by `?` if a `reqwest::Error`
// needs to be converted into a `StockPriceError`.
impl From<reqwest::Error> for StockPriceError {
    fn from(err: reqwest::Error) -> StockPriceError {
        StockPriceError::Request(err)
    }
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
    let new_prices = download_prices(&stocks);
}

fn download_prices(stocks: &[Stock]) -> Vec<Price> {
    let mut prices = Vec::new();

    for stock in stocks {
        match download_price(stock) {
            Ok(price) => prices.push(price),
            Err(e) => eprintln!("Could not download price {}, error is {}", stock.symbol, e)
        }
    }

    prices
}

fn download_price(stock: &Stock) -> Result<Price, StockPriceError> {
    let url = format!("http://www.digitallook.com/equity/{}", stock.digital_look_name);

    println!("Downloading {} from {}", stock.symbol, url);
    let body = reqwest::get(&url)?.text()?;
    println!("  Document downloaded.");

    let document = kuchiki::parse_html().one(body);

    // See https://docs.rs/crate/kuchiki/0.7.0/source/examples/find_matches.rs.
    // Find the h2 node that contains something like "BP Market Data".
    // There should only be 1.
    let headings = document.select("h2").unwrap().filter(|n| {
        // as_node get the underlying NodeRef which has the interesting methods.
        let node = n.as_node();
        let text_node = node.first_child().unwrap();
        // Let's get the actual text in this text node. A text node wraps around
        // a RefCell<String>, so we need to call borrow() to get a &str out.
        let text = text_node.as_text().unwrap().borrow();
        text.ends_with("Market Data")
    }).collect::<Vec<_>>();

    if headings.len() == 0 {
        return Err(StockPriceError::CannotParseDocument(
            format!("Could not locate Market Data heading for {}", stock.symbol)));
    } else if headings.len() > 1 {
        return Err(StockPriceError::CannotParseDocument(
            format!("Found {} headings ending with 'Market Data' for {}, expected only 1", headings.len(), stock.symbol)));
    }

    let heading: &NodeRef = headings[0].as_node();
    //println!("heading = {:?}", heading);
    let heading_parent = heading.parent().unwrap();
    //println!("heading_parent = {:?}", heading_parent);
    let table_body = heading_parent.select_first("tbody").unwrap();
    let tbody = table_body.as_node();
    //println!("table_body = {:#?}", tbody);
    let table_rows = tbody.select("tr").unwrap().collect::<Vec<_>>();
    //println!("Got {} rows", table_rows.len());

    let price = extract_data_from_row(&table_rows, "Share Price");

    let price = Price {
        id: 0,
        stock_id: stock.id,
        date: "today".to_string(),
        price: 0.0,
        prev_price: 0.0,
        fifty_two_week_high: Some(0.1),
        fifty_two_week_low: Some(0.1),
        market_cap_in_millions: Some(0.1),
        sector_id: Some(0),
        index_id: Some(0)
    };

    Ok(price)
}

fn extract_data_from_row(rows: &[NodeDataRef<ElementData>], heading: &str) -> String {

    for row in rows {
        let row_node = row.as_node();
        let heading_cell = row_node.first_child().unwrap();
        let data_cell = row_node.first_child().unwrap();
    }

    "".to_string()
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
