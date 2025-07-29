use std::convert::TryFrom;

use anyhow::{Error, Ok};

use crate::db::SqlClient;

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

impl TryFrom<UnstructuredAddress> for StructuredAddress {
    type Error = Error;

    fn try_from(raw: UnstructuredAddress) -> Result<Self, Error> {
        // Collect the non-empty lines
        let lines: Vec<String> = raw
            .lines
            .into_iter()
            .filter_map(|opt| opt)
            .collect();

        // Name parsing (line 0)
        let full_name = lines.get(0).cloned().unwrap_or_default();
        let mut name_parts = full_name.splitn(2, ' ');
        let lastname  = name_parts.next().unwrap_or("").to_string();
        let firstname = name_parts.next().unwrap_or("").to_string();

        // Street & house number (line 1)
        let (street, house_number) = if let Some(addr_line) = lines.get(1) {
            // split on *last* space
            if let Some(idx) = addr_line.rfind(' ') {
                let (st, hn) = addr_line.split_at(idx);
                (st.to_string(), hn.trim().to_string())
            } else {
                // no space found → entire thing is “street”, no house number
                (addr_line.clone(), String::new())
            }
        } else {
            (String::new(), String::new())
        };

        // Zip & city (line 2)
        let (zip_code, city) = if let Some(pc_line) = lines.get(2) {
            let mut parts = pc_line.splitn(2, ' ');
            let z = parts.next().unwrap_or("").to_string();
            let c = parts.next().unwrap_or("").to_string();
            (z, c)
        } else {
            (String::new(), String::new())
        };

        // Country (line 3)
        let country = lines.get(3).cloned().unwrap_or_else(|| "CH".into());

        // Build the PostalCode type (very naively: first 4 digits, no suffix)
        let mut code = [0u8; 4];
        for (i, ch) in zip_code.chars().filter_map(|c| c.to_digit(10)).enumerate().take(4) {
            code[i] = ch as u8;
        }
        let postal = PostalCode { code, suffix: None };

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
