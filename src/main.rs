#[macro_use] extern crate structopt;
#[macro_use] extern crate serde_derive;
extern crate csv;
extern crate reqwest;
extern crate serde;
extern crate chrono;

use structopt::StructOpt;
use std::path::PathBuf;
use std::env;
use std::process;
use std::path::Path;
use csv::Reader;
use std::fmt::Debug;
use serde::de::DeserializeOwned;
use std::str::FromStr;
use std::fmt;
use std::error;
use std::num::ParseFloatError;
use chrono::prelude::*;
use std::io;
use std::fs::File;
use std::io::Write;
use chrono::DateTime;

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
    stock_id: u32,
    date: String,
    price: f32,
    prev_price: f32,
    #[serde(deserialize_with = "deserialize_optional")]
    fifty_two_week_high: Option<f32>,
    #[serde(deserialize_with = "deserialize_optional")]
    fifty_two_week_low: Option<f32>
}

#[derive(Debug)]
enum StockPriceError {
    CannotParseDocument(String),
    CannotParseNumber(ParseFloatError),
    Request(reqwest::Error)
}

impl fmt::Display for StockPriceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StockPriceError::CannotParseDocument(ref msg) => write!(f, "Cannot parse document, {}", msg),
            StockPriceError::CannotParseNumber(ref e) => std::fmt::Display::fmt(e, f),
            StockPriceError::Request(ref e) => std::fmt::Display::fmt(e, f)
        }
    }
}

impl error::Error for StockPriceError {
    fn description(&self) -> &str {
        match *self {
            StockPriceError::CannotParseDocument(ref msg) => msg,
            StockPriceError::CannotParseNumber(ref e) => e.description(),
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
            StockPriceError::CannotParseNumber(ref e) => Some(e),
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

impl From<ParseFloatError> for StockPriceError {
    fn from(err: ParseFloatError) -> StockPriceError {
        StockPriceError::CannotParseNumber(err)
    }
}

trait StringExtensions {
    fn chomp(&mut self, s: &str) -> Result<(), StockPriceError>;
}

impl StringExtensions for String {
    fn chomp(&mut self, s: &str) -> Result<(), StockPriceError> {
        match self.find(s) {
            Some(idx) => {
                let len = s.len();
                *self = self[idx + len..].to_string();
                Ok(())
            },
            None => Err(StockPriceError::CannotParseDocument(format!("Cannot find {} in string", s)))
        }
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

    // We don't actually need this.
//    let mut file = data_dir.clone();
//    file.push("index.csv");
//    let stock_indexes: Vec<Index> = read_csv(&file, args.print).expect("Could not read index.csv");

    let mut file = data_dir.clone();
    file.push("source.csv");
    let stock_sources: Vec<Source> = read_csv(&file, args.print).expect("Could not read source.csv");

    // We don't actually need this.
//    let mut file = data_dir.clone();
//    file.push("sector.csv");
//    let stock_sectors: Vec<Sector> = read_csv(&file, args.print).expect("Could not read sector.csv");

    let mut file = data_dir.clone();
    file.push("stock.csv");
    let mut stocks: Vec<Stock> = read_csv(&file, args.print).expect("Could not read stock.csv");
    stocks.sort_by(|a,b| a.symbol.cmp(&b.symbol));
    let stocks = stocks.into_iter().take(2).collect::<Vec<_>>();

    let mut file = data_dir.clone();
    file.push("price.csv");
    let mut prices: Vec<Price> = read_csv(&file, args.print).expect("Could not read price.csv");

    println!("Data files read successfully. Beginning stock price download.");
    let (new_prices, errors) = download_prices(&stocks, &stock_sources);

    eprintln!("\n\nGot the following errors:");
    for error in &errors {
        eprintln!("{}", error);
    }

    println!("Writing Quicken prices.csv");
    write_quicken_prices(&new_prices, &stocks).expect("Could not write Quicken prices file.");
    println!("Succeeded in writing Quicken prices.csv");
}

fn write_quicken_prices(prices: &[Price], stocks: &[Stock]) -> io::Result<()> {
    let path = Path::new("prices.csv");
    let mut file = File::create(&path)?;

    // TODO: Replace the string with a DateTime.
    for price in prices {
        let stock = stocks.iter().find(|s| s.id == price.stock_id).expect("Could not find Stock the Price is for.");
        let date = Utc.datetime_from_str(&price.date, "%Y-%m-%d %H:%M:%S.000").unwrap();
        let date_out = date.format("%e/%m/%Y").to_string();
        let date_out = date_out.trim();
        writeln!(file, "{},{:.1},{}", stock.symbol, price.price, date_out)?;
    }

    Ok(())
}

fn download_prices(stocks: &[Stock], sources: &[Source]) -> (Vec<Price>, Vec<String>) {
    let mut prices = Vec::new();
    let mut errors = Vec::new();

    for stock in stocks {
        let source = sources.iter().find(|s| s.id == stock.source_id)
            .expect(&format!("Cannot find Source for Stock {}", stock.symbol));

        match download_price(stock, source) {
            Ok(price) => prices.push(price),
            Err(e) => {
                errors.push(format!("Could not download price {}, error is {}", stock.symbol, e))
            }
        }
    }

    (prices, errors)
}

fn download_price(stock: &Stock, source: &Source) -> Result<Price, StockPriceError> {
    let url = format!("{}{}", source.url, stock.digital_look_name);

    println!("Downloading {} from {}", stock.symbol, url);
    let mut body = reqwest::get(&url)?.text()?;
    println!("  Document downloaded.");

    body.chomp("Market Data</h2>")?;
    body.chomp("precio_ultima_cotizacion")?;
    body.chomp(">")?;
    let price = extract_pence(&body)?;
    println!("  Got price of {}", price);

    body.chomp("variacion_puntos")?;
    body.chomp(">")?;
    body.chomp(">")?;
    let price_change_today = extract_pence(&body)?;
    println!("  Got price_change_today of {}", price_change_today);

    body.chomp("High 52 week range")?;
    body.chomp("<td>")?;
    let fifty_two_high = extract_pence(&body)?;
    println!("  Got 52 week high of {}", fifty_two_high);

    body.chomp("Low 52 week range")?;
    body.chomp("<td>")?;
    let fifty_two_low = extract_pence(&body)?;
    println!("  Got 52 week low of {}", fifty_two_low);

    // Ok unless we run past midnight, which is not a concern for this program.
    let utc: DateTime<Utc> = Utc::now();

    let price = Price {
        stock_id: stock.id,
        date: utc.format("%Y-%m-%d 00:00:00.000").to_string(),
        price: price,
        prev_price: price - price_change_today,
        fifty_two_week_high: Some(fifty_two_high),
        fifty_two_week_low: Some(fifty_two_low)
    };

    println!("GOT {:#?}", price);

    Ok(price)
}

fn extract_up_to_next_tag(s: &str) -> Result<&str, StockPriceError> {
    match s.find('<') {
        Some(idx) => Ok(&s[..idx]),
        None => Err(StockPriceError::CannotParseDocument("Cannot find next '<' character".to_string()))
    }
}

fn extract_pence(s: &str) -> Result<f32, StockPriceError> {
    let mut s = extract_up_to_next_tag(s)?.to_string();
    s.retain(|c| !c.is_ascii_whitespace());
    s.to_lowercase();
    if s.len() == 0 || s == "n/a" {
        return Ok(0.0);
    }

    s = s.chars().take_while(|c| c.is_ascii_digit() || *c == '.' || *c == ',' || *c == '-').collect();
    s.retain(|c| c != ',');

    Ok(s.parse::<f32>()?)
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
            Err(_e) => return Err(D::Error::custom(format!("Could not parse '{}' into the desired type.", s)))
        }
    }
}


#[cfg(test)]
mod tests {
    use ::StringExtensions;
    use StockPriceError;
    use std::error::Error;

    #[test]
    fn chomp_when_pattern_exists_returns_following_text() {
        let mut s = "<h2>CLLN Market Data</h2> whatever".to_string();
        s.chomp("Data</h2>").unwrap();
        assert_eq!(s, " whatever");

        let mut s = "<h2>CLLN Market Data</h2> whatever".to_string();
        s.chomp("h2>").unwrap();
        assert_eq!(s, "CLLN Market Data</h2> whatever");

        let mut s = "<h2>CLLN Market Data</h2> whatever".to_string();
        s.chomp("whatever").unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn chomp_when_pattern_does_not_exists_returns_error() {
        let mut s = "hello world".to_string();
        match s.chomp("BLAH") {
            Ok(_) => panic!("chomp should NOT return Ok."),
            Err(e) => {
                let expected = "Cannot parse document, Cannot find BLAH in string";
                let actual = format!("{}", e);
                assert_eq!(expected, actual);
            }
        }
    }
}
