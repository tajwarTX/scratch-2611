//! Writes the two output tables as uncompressed Parquet.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::errors::Result;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;

const USER_SCHEMA: &str = "message user_daily {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";

const ROW_GROUP: usize = 1 << 20;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(ROW_GROUP)
            .build(),
    )
}

type Rg<'a> = SerializedRowGroupWriter<'a, BufWriter<File>>;

fn bytes_col(rg: &mut Rg, values: &[ByteArray], def: Option<&[i16]>) -> Result<()> {
    let mut c = rg.next_column()?.expect("column");
    c.typed::<ByteArrayType>().write_batch(values, def, None)?;
    c.close()
}

fn i32_col(rg: &mut Rg, values: &[i32]) -> Result<()> {
    let mut c = rg.next_column()?.expect("column");
    c.typed::<Int32Type>().write_batch(values, None, None)?;
    c.close()
}

fn i64_col(rg: &mut Rg, values: &[i64]) -> Result<()> {
    let mut c = rg.next_column()?.expect("column");
    c.typed::<Int64Type>().write_batch(values, None, None)?;
    c.close()
}

pub struct UserOut<'a> {
    pub user_id: &'a [ByteArray],
    pub day: &'a [i32],
    pub sessions: &'a [i64],
    pub events: &'a [i64],
    pub revenue: &'a [i64],
    /// Definition levels (1 present, 0 null) and the present values only.
    pub top_def: &'a [i16],
    pub top: &'a [ByteArray],
}

pub fn write_user_daily(path: &Path, cols: &UserOut) -> Result<()> {
    let schema = Arc::new(parse_message_type(USER_SCHEMA)?);
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props())?;
    let n = cols.day.len();
    let mut start = 0;
    let mut top_start = 0;
    while start < n {
        let end = (start + ROW_GROUP).min(n);
        let defs = &cols.top_def[start..end];
        let present = defs.iter().filter(|&&d| d == 1).count();
        let mut rg = w.next_row_group()?;
        bytes_col(&mut rg, &cols.user_id[start..end], None)?;
        i32_col(&mut rg, &cols.day[start..end])?;
        i64_col(&mut rg, &cols.sessions[start..end])?;
        i64_col(&mut rg, &cols.events[start..end])?;
        i64_col(&mut rg, &cols.revenue[start..end])?;
        bytes_col(&mut rg, &cols.top[top_start..top_start + present], Some(defs))?;
        rg.close()?;
        top_start += present;
        start = end;
    }
    w.close()?;
    Ok(())
}

pub struct CampaignOut {
    pub campaign_id: Vec<ByteArray>,
    pub channel: Vec<ByteArray>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing: Vec<i64>,
    pub revenue: Vec<i64>,
    pub assisted: Vec<i64>,
}

pub fn write_campaign_daily(path: &Path, c: &CampaignOut) -> Result<()> {
    let schema = Arc::new(parse_message_type(CAMPAIGN_SCHEMA)?);
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props())?;
    if !c.day.is_empty() {
        let mut rg = w.next_row_group()?;
        bytes_col(&mut rg, &c.campaign_id, None)?;
        bytes_col(&mut rg, &c.channel, None)?;
        i32_col(&mut rg, &c.day)?;
        i64_col(&mut rg, &c.sessions)?;
        i64_col(&mut rg, &c.purchasing)?;
        i64_col(&mut rg, &c.revenue)?;
        i64_col(&mut rg, &c.assisted)?;
        rg.close()?;
    }
    w.close()?;
    Ok(())
}
