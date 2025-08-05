use std::convert::TryFrom;

use anyhow::{Error, Ok};
use regex::Regex;
use lazy_static::lazy_static;

use crate::db::SqlClient;

lazy_static! {
    static ref TITLE_RE: Regex = Regex::new(r"(?i)^\s*(?P<title>FRAU|HERR|MADAME|MONSIEUR|MR|MS|M|MME)\s*$")
        .expect("Invalid title regex");

    static ref ZIP_RE: Regex = Regex::new(r"^(\d{4})(?:[-\s]?(\d{2}))?$")
        .expect("invalid ZIP regex");
    static ref HOUSE_RE: Regex = Regex::new(r"(?ix) # i = case-insensitive, x = extended
        ^\s*
        (?P<street>.+?)                         # 1: street name
        \s*
        (?P<number>                             # 2: house number block
            [1-9]\d{0,3}                        # main number: 1-9999
            (?:
                (?:(?:bis|ter|quater|quinquies) # optional Latin suffix
                |[A-Za-z]                       # or single letter suffix
                )
            )?
            (?:\/[1-9]\d{0,3})?                 # optional “/apartment” (1-9999)
        )
        \s*$
        ").expect("invalid house-number regex");

    static ref POSTAL_BOX_RE: Regex = Regex::new(r"(?ix)
        ^\s*
        (?:P\.O\.\s*Box
          |Postfach
          |Case\s+Postale
          |Casella\s+Postale
          |CP
        )
        \s+
        (\d{1,4})
        \s*$
        ").expect("Invalid postal box regex");
}


#[derive(Debug, Clone)]
pub struct UnstructuredAddress {
    pub id:    String,
    pub lines: [Option<String>; 6],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostalCode {
    pub code:   u16,
    pub suffix: Option<u8>,
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

impl TryFrom<UnstructuredAddress> for StructuredAddress {
    type Error = Error;

    fn try_from(raw: UnstructuredAddress) -> Result<Self, Error> {
        // Collect the non-empty lines
        let lines: Vec<String> = raw
            .lines
            .into_iter()
            .filter_map(|opt| opt)
            .collect();

        let mut line_offset = 0;
        let mut title: String = "".to_owned();

        if let Some(first_line) = lines.get(0) {
            if let Some(caps) = TITLE_RE.captures(first_line) {
                title = caps.name("title").unwrap().as_str().to_owned();
                line_offset += 1;
            }
        }

        // Name parsing (line 0)
        let full_name = lines.get(0 + line_offset).cloned().unwrap_or_default();

        // Approximative name parsing
        let mut name_parts = full_name.splitn(2, ' ');
        let lastname: String;
        let firstname: String;
        if let Some(caps) = TITLE_RE.captures(name_parts.clone().next().unwrap_or("")) {
            title = caps.name("title").unwrap().as_str().to_owned();
            let mut real_name_parts = name_parts.next().unwrap_or("").splitn(2, ' ');
            lastname  = real_name_parts.next().unwrap_or("").to_string();
            firstname = real_name_parts.next().unwrap_or("").to_string();
        } else {
            lastname  = name_parts.next().unwrap_or("").to_string();
            firstname = name_parts.next().unwrap_or("").to_string();
        }

        let mut compl1: String = "".to_owned();
        let mut compl2: String = "".to_owned();
        let mut street_or_pobox: String = "".to_owned();
        let mut address: AddressLine = AddressLine::Street { street: "".to_owned(), house_number: "".to_owned() };
        let mut postal: PostalCode = PostalCode { code: 0, suffix: None };
        let mut city: String = "".to_owned();

        let mut idx = 1 + line_offset;
        while let Some(line) = lines.get(idx) {
            idx += 1;

            let mut locality_parts = line.splitn(2, ' ');

            if street_or_pobox.is_empty() && let Some(caps) = POSTAL_BOX_RE.captures(line) {
                street_or_pobox = caps.get(0).unwrap().as_str().trim().to_string();
                address = AddressLine::PoBox { box_number: street_or_pobox.clone() };
            } else if street_or_pobox.is_empty() && let Some(caps) = HOUSE_RE.captures(line) {
                street_or_pobox = caps.name("street").unwrap().as_str().trim().to_string();
                let house_number = caps.name("number").unwrap().as_str().trim().to_string();

                address = AddressLine::Street { street: street_or_pobox.clone(), house_number: house_number };
            } else if city.is_empty() && let Some(caps) = ZIP_RE.captures(locality_parts.next().unwrap()) {
                let code = caps.get(1).unwrap().as_str().parse().unwrap();
        
                let suffix = caps.get(2).map(|m| m.as_str().parse::<u8>().unwrap());
                postal = PostalCode { code, suffix };

                city = locality_parts.next().unwrap_or("").to_owned();
            } else if compl1.is_empty() {
                compl1 = line.to_string();
            } else if compl2.is_empty() {
                compl2 = line.to_string();
            }
        }
        /*
        // after you’ve extracted the street line:
        let street_line = lines.get(1 + line_offset).map(String::as_str).unwrap_or("");
        let (street, house_number) = parse_street_house(street_line)
        .unwrap_or_else(|| ("".into(), "".into()));

        let (zip_code, city) = if let Some(pc_line) = lines.get(2 + line_offset) {
            let mut parts = pc_line.splitn(2, ' ');
            let z = parts.next().unwrap_or("").to_string();
            let c = parts.next().unwrap_or("").to_string();
            (z, c)
        } else {
            (String::new(), String::new())
        };

        let postal = parse_zip(&zip_code).unwrap_or(PostalCode { code: 0, suffix: None });
        */

        let country = lines.get(3 + line_offset).cloned().unwrap_or_else(|| "CH".into());

        /*println!("title:'{}'", title);
        println!("name:'{lastname} {firstname}'");
        println!("{:#?}", address);
        println!("compl1:'{}'", compl1);
        println!("postal:'{:04} - {:02}' - city:'{}'", postal.code, postal.suffix.unwrap_or_default(), city);*/

        // Wrap up into a StructuredAddress
        Ok(StructuredAddress {
            id:        raw.id,
            title:     Some(title),
            name:      Some("{lastname} {firstname}".to_owned()),   
            lastname:  Some(lastname),
            firstname: Some(firstname),
            compl1:    Some(compl1),
            compl2:    Some(compl2),
            address,
            postal,
            city,
            country,
        })
    }
}
