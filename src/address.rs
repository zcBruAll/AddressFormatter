use std::convert::TryFrom;

use anyhow::{Error, Ok};
use regex::Regex;
use lazy_static::lazy_static;

use crate::db::{SqlClient, n_opt, sql_quote};

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

pub fn save_structured_address(
    table: &str,
    addr: &StructuredAddress,
    client: &SqlClient,
) -> anyhow::Result<u64> {
    let (street, house_number, po_box) = match &addr.address {
        AddressLine::Street { street, house_number } => {
            (Some(street.as_str()), Some(house_number.as_str()), None)
        }
        AddressLine::PoBox { box_number } => {
            (None, None, Some(box_number.as_str()))
        }
    };

    let postal_code = addr.postal.code;
    let postal_suffix = addr.postal.suffix.map(|s| s.to_string());

    println!("{:?}", &addr);

    let sql = format!(
        "INSERT INTO {table} \
        (ID_FPR_PAYREL, FPR_PAYEMENT_DOMAIN, FPR_ACCOUNT_OWNER_NAME, \
        FPR_ACCOUNT_OWNER_ADRESS_LINE1, FPR_ACCOUNT_OWNER_ADRESS_LINE2, \
        FPR_STREET, FPR_BUILDING_NUMBER, FPR_POST_CODE, FPR_TOWN_NAME, \
        FPR_ACCOUNT_OWNER_ADDRESS_COUNTRY, FPR_ACCOUNT_TYPE, FPR_ACCOUNT_NO, \
        FPR_CURRENCY, FPR_PAYMENT_POOL, FPR_ACCOUNT_NO_REF, FPR_VALIDITY_START, \
        FPR_VALIDITY_END, FPR_STATE, FPR_SOURCE, FPR_VALID, FPR_USR_LOG_I, \
        FPR_DTE_LOG_I, FPR_USR_LOG_U, FPR_DTE_LOG_U, OLD_TBL_ID, OLD_ID_ADRESSE, \
        RIP_PERSON_ID, RIP_PERSON_BPC_ID, PAC_PAYEMENT_ADRESS_ID, PAC_VERSION_ADR)
        {id_fpr_payrel}, '{fpr_payment_domain}', '{fpr_account_owner_name}', \
        '{fpr_account_owner_address_line1}', '{fpr_account_owner_address_line2}', \
        '{fpr_street}', '{fpr_building_number}', {fpr_post_code}, '{fpr_town_name}', \
        '{fpr_account_owner_address_country}', '{fpr_account_type}', '{fpr_account_no}', \
        '{fpr_currency}', {fpr_payment_pool}, '{fpr_account_no_ref}', {fpr_validity_start}, \
        {fpr_validity_end}, '{fpr_state}', '{fpr_source}', {fpr_valid}, '{fpr_usr_log_i}', \
        {fpr_dte_log_i}, '{fpr_usr_log_u}', {fpr_dte_log_u}, {old_tbl_id}, {old_id_address}, \
        {rip_person_id}, {rip_person_bpc_id}, {pac_payment_address_id}, {pac_version_adr} \
        FROM {table} AS t",
        table       = table,
        id_fpr_payrel = "SELECT ISNULL(MAX(t.ID_FPR_PAYREL),0)+1",

        fpr_payment_domain              = "FCF".to_string(),//n_opt(fpr_payment_domain.as_deref()),
        fpr_account_owner_name          = n_opt(addr.name.as_deref()),
        fpr_account_owner_address_line1 = n_opt(addr.compl1.as_deref()),
        fpr_account_owner_address_line2 = n_opt(addr.compl2.as_deref()),
        fpr_street                      = n_opt(street.as_deref()),
        fpr_building_number             = n_opt(house_number.as_deref()),
        fpr_town_name                   = n_opt(Some(&addr.city)),
        fpr_account_owner_address_country = n_opt(Some(&addr.country)),
        fpr_account_type                = "TRAN_CH",//n_opt(fpr_account_type.as_deref()),
        fpr_account_no                  = "CH00",//n_opt(fpr_account_no.as_deref()),
        fpr_currency                    = "CHF",//n_opt(fpr_currency.as_deref()),
        fpr_account_no_ref              = "NULL",//n_opt(fpr_account_no_ref.as_deref()),
        fpr_state                       = "ACTIVE",//n_req(&fpr_state),     // NOT NULL (<=10 chars)
        fpr_source                      = "OTH",//n_opt(fpr_source.as_deref()),
        fpr_usr_log_i                   = "FORMAT",//n_req(&fpr_usr_log_i), // NOT NULL (<=15 chars)
        fpr_usr_log_u                   = "FORMAT",//n_req(&fpr_usr_log_u), // NOT NULL (<=15 chars)

        fpr_post_code         = postal_code,//num_opt(fpr_post_code),
        fpr_payment_pool      = 0,//num_opt(fpr_payment_pool),
        fpr_valid             = 1,//num_opt(fpr_valid),
        old_tbl_id            = addr.id,//num_opt(old_tbl_id),
        old_id_address        = "NULL",//num_opt(old_id_address),
        rip_person_id         = 0,//num_opt(rip_person_id),
        rip_person_bpc_id     = 0,//num_opt(rip_person_bpc_id),
        pac_payment_address_id= "NULL",//num_opt(pac_payment_address_id),
        pac_version_adr       = "NULL",//num_opt(pac_version_adr),
    
        fpr_dte_log_i     = "GETDATE()",//date_opt(Some(&fpr_dte_log_i)),
        fpr_dte_log_u     = "GETDATE()",//date_opt(Some(&fpr_dte_log_u)),
        fpr_validity_start= "GETDATE()",//date_opt(Some(&fpr_validity_start)),
        fpr_validity_end  = "NULL",//date_opt(fpr_validity_end.as_deref()),
    );

    println!("{}", &sql);

    client.execute_non_query(&sql).map_err(Into::into)
}

pub fn update_addr_pay_id(
    table: &str,
    id_field: &str,
    id_ref_field: &str,
    ref_table: &str,
    ref_id_field: &str,
    ref_link_field: &str,
    ref_main_id: &str,
    client: &SqlClient) -> anyhow::Result<u64> {

    let sql = format!(
        "UPDATE {table} \
        SET {id_ref_field} = ( \
            SELECT ad.{ref_id_field} \
            FROM {ref_table} ad \
            WHERE ad.{ref_link_field} = '{ref_main_id}' \
        ) \
        WHERE {id_field} = '{ref_main_id}'"
    );

    client.execute_non_query(&sql).map_err(Into::into)
}

impl TryFrom<UnstructuredAddress> for StructuredAddress {
    type Error = Error;

    fn try_from(raw: UnstructuredAddress) -> Result<Self, Error> {
        // Collect the non-empty lines
        let lines: Vec<String> = raw
            .lines
            .into_iter()
            .filter_map(|opt| opt)
            .filter(|s| !s.is_empty())
            .collect();

        let mut line_offset = 0;
        let mut title: String = String::new();

        if let Some(first_line) = lines.get(0) {
            if TITLE_RE.is_match(first_line) {
                title = first_line.to_string();
                line_offset += 1;
            }
        }

        // Name parsing (line 0)
        let full_name = lines.get(0 + line_offset).cloned().unwrap_or_default();

        // Split name and handle possible inline title
        let mut parts = full_name.split_whitespace();
        let first_token = parts.next().unwrap_or("");

        // Check if the first token is a title
        let (lastname, firstname) = if TITLE_RE.is_match(first_token) {
            // Only overwrite title if not already set by separate line
            if title.is_empty() {
                title = first_token.to_string();
            }

            let last = parts.next().unwrap_or("").to_string();
            let first = parts.next().unwrap_or("").to_string();
            (last, first)
        } else {
            let last = first_token.to_string();
            let first = parts.next().unwrap_or("").to_string();
            (last, first)
        };

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
                line_offset += 1;
            } else if compl2.is_empty() {
                compl2 = line.to_string();
                line_offset += 1;
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
            name: Some(format!("{} {}", lastname, firstname)),
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
