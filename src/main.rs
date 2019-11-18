extern crate chrono;
extern crate csv;
extern crate reqwest;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate structopt;

// TODO: Remove the command line arguments

use chrono::prelude::*;
use csv::Reader;
use serde::de::DeserializeOwned;
use std::env;
use std::error;
use std::fmt;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::Cursor;
use std::io::Write;
use std::num::ParseFloatError;
use std::path::Path;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Arguments {
    /// Do not download anything, just print reference information.
    #[structopt(short = "p", long = "print")]
    print: bool,

    /// Number of stock prices to download. Defaults to all.
    #[structopt(short = "n", long = "number", default_value = "100000")]
    num_stocks: usize,

    /// The location of the CSV files. If not set, assumed to
    /// be the current directory.
    #[structopt(short = "d", name = "data_directory", parse(from_os_str))]
    data_directory: Option<PathBuf>,

    /// The output directory.
    #[structopt(short = "o", name = "output_directory", parse(from_os_str))]
    output_directory: Option<PathBuf>,
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
    source_id: u32,
    enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Price {
    stock_id: u32,
    #[serde(with = "my_date_format")]
    date: Date<Utc>,
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

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            StockPriceError::CannotParseDocument(_) => None,
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
    // These data files are embedded into the binary, meaning we do not need to ship them as
    // supporting files (but if anything changes, we need to rebuild the program.)
    //let stock_market_indexes = include_bytes!("data/index.csv");
    //let stock_market_sectors = include_bytes!("data/sector.csv");
    let download_sources = include_bytes!("data/source.csv");
    let stocks = include_bytes!("data/stock.csv");

    // Turn the embedded byte arrays into more reasonable data structures.
    let mut cursor = Cursor::new(&download_sources[..]);
    let download_sources: Vec<Source> = read_csv(&mut cursor, true).expect("Could not read source.csv");

    let mut cursor = Cursor::new(&stocks[..]);
    let mut stocks: Vec<Stock> = read_csv(&mut cursor, true).expect("Could not read stock.csv");
    stocks.sort_by(|a,b| a.symbol.cmp(&b.symbol));
    let stocks = stocks.into_iter().filter(|s| s.enabled).collect::<Vec<_>>();

    println!("Data files read successfully. Beginning download of {} prices.", stocks.len());
    let (new_prices, errors) = download_prices(&stocks, &download_sources);

    println!("Writing output files.");
    let output_dir = env::current_dir().expect("Could not determine current directory, so cannot write any output");
    write_quicken_prices(&output_dir, &new_prices, &stocks, 100.0).expect("Could not write Quicken prices file.");
    write_stock_prices(&output_dir, &new_prices, &stocks).expect("Could not write Stock prices file (for shares.ods).");
    write_errors(&output_dir, &errors).expect("Could not write errors file.");
}

fn read_csv<T: Debug + DeserializeOwned>(rdr: &mut Cursor<&[u8]>, print: bool) -> std::io::Result<Vec<T>>
{
    let mut results: Vec<T> = Vec::new();
    let mut rdr = Reader::from_reader(rdr);
    for result in rdr.deserialize() {
        let record: T = result?;
        if print {
            println!("{:?}", record);
        }
        results.push(record);
    }

    Ok(results)
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

    // Utc::today is Ok unless we run past midnight, which is not a concern for this program.
    let mut price = Price {
        stock_id: stock.id,
        date: Utc::today(),
        price: 0.0,
        prev_price: 0.0,
        fifty_two_week_high: None,
        fifty_two_week_low: None
    };

    if source.id == 1 {
        // A Digital Look equity.
        body.chomp("Market Data</h2>")?;
        body.chomp("precio_ultima_cotizacion")?;
        body.chomp(">")?;
        price.price = extract_pence(&body)?;
        println!("  Got price of {}", price.price);

        body.chomp("variacion_puntos")?;
        body.chomp(">")?;
        body.chomp(">")?;
        let price_change_today = extract_pence(&body)?;
        price.prev_price = price.price - price_change_today;
        println!("  Got price_change_today of {}", price_change_today);

        body.chomp("High 52 week range")?;
        body.chomp("<td>")?;
        price.fifty_two_week_high = Some(extract_pence(&body)?);
        println!("  Got 52 week high of {:?}", price.fifty_two_week_high);

        body.chomp("Low 52 week range")?;
        body.chomp("<td>")?;
        price.fifty_two_week_low = Some(extract_pence(&body)?);
        println!("  Got 52 week low of {:?}", price.fifty_two_week_low);
    } else if source.id == 2 {
        // A Digital Look ETF.
        body.chomp("Detailed Price Data</h2>")?;
        body.chomp("<td>Price:</td>")?;
        body.chomp(">")?;
        price.price = extract_pence(&body)?;
        println!("  Got price of {}", price.price);

        body.chomp("<td>Change:</td>")?;
        body.chomp("<td>")?;
        body.chomp(">")?;
        let price_change_today = extract_pence(&body)?;
        price.prev_price = price.price - price_change_today;
        println!("  Got price_change_today of {}", price_change_today);

        body.chomp("52 week High")?;
        body.chomp("<td>")?;
        price.fifty_two_week_high = Some(extract_pence(&body)?);
        println!("  Got 52 week high of {:?}", price.fifty_two_week_high);

        body.chomp("52 week Low")?;
        body.chomp("<td>")?;
        price.fifty_two_week_low = Some(extract_pence(&body)?);
        println!("  Got 52 week low of {:?}", price.fifty_two_week_low);
    }

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






fn write_errors(output_dir: &Path, errors: &[String]) -> io::Result<()> {
    if errors.len() > 0 {
        let mut path = output_dir.to_path_buf();
        path.push("errors.txt");
        let mut file = File::create(&path)?;

        eprintln!("\n\nGot {} errors.", errors.len());
        for error in errors {
            eprintln!("{}", error);
            writeln!(file, "{}", error)?;
        }

        println!("Succeeded in writing {:?}", path);
    }

    Ok(())
}

fn write_quicken_prices(
    output_dir: &Path,
    prices: &[Price],
    stocks: &[Stock],
    factor: f32
    ) -> io::Result<()> {
    if prices.len() > 0 {
        let mut path = output_dir.to_path_buf();
        path.push("qp.csv");
        let mut file = File::create(&path)?;

        println!("\n\nWriting {:?}", path);

        for price in prices {
            let stock = stocks.iter().find(|s| s.id == price.stock_id).expect("Could not find Stock the Price is for.");
            writeln!(file, "{},{:.3},{}/{:02}/{}", stock.symbol, price.price / factor,
                     price.date.day(), price.date.month(), price.date.year())?;
        }

        println!("Succeeded in writing {:?}", path);
    }

    Ok(())
}

fn write_stock_prices(output_dir: &Path, prices: &[Price], stocks: &[Stock]) -> io::Result<()> {
    if prices.len() > 0 {
        let mut path = output_dir.to_path_buf();
        path.push("stockdata.csv");
        let mut file = File::create(&path)?;

        println!("\n\nWriting {:?}", path);

        for price in prices {
            let stock = stocks.iter().find(|s| s.id == price.stock_id).expect("Could not find Stock the Price is for.");
            writeln!(file, "{},{:.2},{}/{:02}/{},{:.2}", stock.symbol, price.price,
                     price.date.day(), price.date.month(), price.date.year(), price.prev_price)?;
        }

        println!("Succeeded in writing {:?}", path);
    }

    Ok(())
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

mod my_date_format {
    use chrono::{Date, Datelike, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S.%3f";

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Date<Utc>, D::Error>
        where D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
            .map(|dt| Utc.ymd(dt.year(), dt.month(), dt.day()))
    }
}

#[cfg(test)]
mod tests {
    use ::StringExtensions;

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
    fn chomp_when_pattern_does_not_exist_returns_error() {
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
