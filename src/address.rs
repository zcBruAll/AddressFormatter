use std::convert::TryFrom;

use anyhow::{Error, Ok};
use regex::Regex;
use lazy_static::lazy_static;

use crate::db::SqlClient;

lazy_static! {
    static ref ZIP_RE: Regex = Regex::new(r"^(\d{4})(?:[-\s]?(\d{2}))?$")
        .expect("invalid ZIP regex");
    static ref HOUSE_RE: Regex = Regex::new(r"(?x)
        ^\s*
        (?P<street>.+?)               # 1: street name (minimal)
        \s+
        (?P<number>                   # 2: house number block
            \d{1,4}                   #    1-4 digit base
            (?:[\/\-\u00BD]\d+)?      #    optional '/n', '-n' or '½n' fractions
            (?:[A-Za-z]{1,2})?        #    optional 1-2 letter suffix
        )
        \s*$
    ").expect("invalid house-number regex");
}


#[derive(Debug, Clone)]
pub struct UnstructuredAddress {
    pub id:    String,
    pub lines: [Option<String>; 6],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostalCode {
    pub code:   [u8; 4],
    pub suffix: Option<[char; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AddressLine {
    Street { street: String, house_number: String },
    PoBox  { box_number: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructuredAddress {
    pub id:         String,
    pub title:      Option<String>,
    pub name:       Option<String>,
    pub lastname:   Option<String>,
    pub firstname:  Option<String>,
    pub compl1:     Option<String>,
    pub compl2:     Option<String>,
    pub address:    AddressLine,
    pub postal:     PostalCode,
    pub city:       String,
    pub country:    String,
}

fn clean(s: Option<String>) -> Option<String> {
    s.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

pub fn get_unstructured_addresses(
    table: &str,
    col_id: &str,
    col_lines: &[&str],
    client: &SqlClient,
) -> Result<Vec<UnstructuredAddress>, Error> {
    let mut select_items = Vec::with_capacity(7);
    select_items.push(col_id.to_string());

    // for each of the six “lines” slots, pick the real column or NULL
    for idx in 0..6 {
        if let Some(col) = col_lines.get(idx) {
            // real column name
            select_items.push(col.to_string());
        } else {
            // pad with NULL so row.get() still sees the right number of columns
            select_items.push(format!("NULL AS line{}", idx + 1));
        }
    }

    let query = format!(
        "SELECT {} FROM {}",
        select_items.join(", "),
        table
    );

    // Execute and propagate any error
    let rows = client.execute_query(&query, false)?;  

    // Preallocate result Vec
    let mut out = Vec::with_capacity(rows.len());

    // Map each DB row
    for row in rows {
        let addr = UnstructuredAddress {
            id: row.get(0).cloned().expect("No ID retrieved").trim().to_string(),
            lines: [
                clean(row.get(1).cloned()),
                clean(row.get(2).cloned()),
                clean(row.get(3).cloned()),
                clean(row.get(4).cloned()),
                clean(row.get(5).cloned()),
                clean(row.get(6).cloned()),
            ],
        };
        out.push(addr);
    }

    Ok(out)
}

fn parse_zip(raw: &str) -> Option<PostalCode> {
    // trim whitespace first
    let raw = raw.trim();
    ZIP_RE.captures(raw).map(|caps| {
        let code_digits = caps.get(1).unwrap().as_str();
        // parse “8001” → [8,0,0,1]
        let mut code = [0u8; 4];
        for (i, ch) in code_digits.chars().enumerate() {
            code[i] = ch.to_digit(10).unwrap() as u8;
        }
        let suffix = caps.get(2).map(|m| {
            let s = m.as_str();
            let mut arr = [' '; 2];
            for (i, ch) in s.chars().enumerate() {
                arr[i] = ch;
            }
            arr
        });
        PostalCode { code, suffix }
    })
}

fn parse_street_house(raw: &str) -> Option<(String, String)> {
    let raw = raw.trim();
    HOUSE_RE.captures(raw).and_then(|caps| {
        let street = caps.name("street")?.as_str().trim().to_string();
        let number = caps.name("number")?.as_str().trim().to_string();
        Some((street, number))
    })
}

impl TryFrom<UnstructuredAddress> for StructuredAddress {
    type Error = Error;

    fn try_from(raw: UnstructuredAddress) -> Result<Self, Error> {
        // Collect the non-empty lines
        let lines: Vec<String> = raw
            .lines
            .into_iter()
            .filter_map(|opt| opt)
            .collect();

        println!("{lines:#?}");

        // Name parsing (line 0)
        let full_name = lines.get(0).cloned().unwrap_or_default();

        // Approximative name parsing
        let mut name_parts = full_name.splitn(2, ' ');
        let lastname  = name_parts.next().unwrap_or("").to_string();
        let firstname = name_parts.next().unwrap_or("").to_string();

        // after you’ve extracted the street line:
        let street_line = lines.get(1).map(String::as_str).unwrap_or("");
        let (street, house_number) = parse_street_house(street_line)
        .unwrap_or_else(|| ("".into(), "".into()));

        let (zip_code, city) = if let Some(pc_line) = lines.get(2) {
            let mut parts = pc_line.splitn(2, ' ');
            let z = parts.next().unwrap_or("").to_string();
            let c = parts.next().unwrap_or("").to_string();
            (z, c)
        } else {
            (String::new(), String::new())
        };

        let postal = parse_zip(&zip_code).unwrap_or(PostalCode { code: [0;4], suffix: None });

        let country = lines.get(3).cloned().unwrap_or_else(|| "CH".into());

        // Wrap up into a StructuredAddress
        Ok(StructuredAddress {
            id:        raw.id,            // carry over the raw ID
            title:     None,              // no title parsing yet
            name:      Some(full_name),   
            lastname:  Some(lastname),
            firstname: Some(firstname),
            compl1:    None,              // skipping complement lines for now
            compl2:    None,
            address:   AddressLine::Street { street, house_number },
            postal,
            city,
            country,
        })
    }
}
