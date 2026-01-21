//! Implementations of [`Source`] for generating data in delimiter-separated values (DSV) format.
//!
//! Historically this module produced comma-separated output via `tpchgen::csv`. This is still used
//! when the delimiter is `,` to preserve exact output. When using a different delimiter (e.g.
//! `\t`), this module writes DSV output directly.

use super::generate::Source;
use std::io::Write;
use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    Customer, CustomerGenerator, LineItem, LineItemGenerator, Nation, NationGenerator, Order,
    OrderGenerator, Part, PartGenerator, PartSupp, PartSuppGenerator, Region, RegionGenerator,
    Supplier, SupplierGenerator,
};

fn write_delimited_header(buffer: &mut Vec<u8>, delimiter: u8, comma_header: &'static str) {
    if delimiter == b',' {
        buffer.extend_from_slice(comma_header.as_bytes());
    } else {
        for &b in comma_header.as_bytes() {
            buffer.push(if b == b',' { delimiter } else { b });
        }
    }
    buffer.push(b'\n');
}

fn write_dsv_str_field(buffer: &mut Vec<u8>, delimiter: u8, value: &str) {
    let needs_quotes = value
        .as_bytes()
        .iter()
        .any(|&b| b == delimiter || b == b'"' || b == b'\n' || b == b'\r');

    if !needs_quotes {
        buffer.extend_from_slice(value.as_bytes());
        return;
    }

    buffer.push(b'"');
    for &b in value.as_bytes() {
        if b == b'"' {
            buffer.push(b'"');
            buffer.push(b'"');
        } else {
            buffer.push(b);
        }
    }
    buffer.push(b'"');
}

fn write_nation_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: Nation<'static>) {
    write!(buffer, "{}", row.n_nationkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.n_name);
    buffer.push(delimiter);
    write!(buffer, "{}", row.n_regionkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.n_comment);
    buffer.push(b'\n');
}

fn write_region_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: Region<'static>) {
    write!(buffer, "{}", row.r_regionkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.r_name);
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.r_comment);
    buffer.push(b'\n');
}

fn write_part_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: Part<'static>) {
    write!(buffer, "{}", row.p_partkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.p_name).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.p_mfgr).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.p_brand).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.p_type);
    buffer.push(delimiter);
    write!(buffer, "{}", row.p_size).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.p_container);
    buffer.push(delimiter);
    write!(buffer, "{}", row.p_retailprice).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.p_comment);
    buffer.push(b'\n');
}

fn write_supplier_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: Supplier) {
    write!(buffer, "{}", row.s_suppkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.s_name).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.s_address).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.s_nationkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.s_phone).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.s_acctbal).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, &row.s_comment);
    buffer.push(b'\n');
}

fn write_customer_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: Customer<'static>) {
    write!(buffer, "{}", row.c_custkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.c_name).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.c_address).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.c_nationkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.c_phone).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.c_acctbal).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.c_mktsegment);
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.c_comment);
    buffer.push(b'\n');
}

fn write_partsupp_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: PartSupp<'static>) {
    write!(buffer, "{}", row.ps_partkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.ps_suppkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.ps_availqty).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.ps_supplycost).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.ps_comment);
    buffer.push(b'\n');
}

fn write_orders_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: Order<'static>) {
    write!(buffer, "{}", row.o_orderkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.o_custkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.o_orderstatus).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.o_totalprice).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.o_orderdate).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.o_orderpriority);
    buffer.push(delimiter);
    write!(buffer, "{}", row.o_clerk).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.o_shippriority).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.o_comment);
    buffer.push(b'\n');
}

fn write_lineitem_dsv(buffer: &mut Vec<u8>, delimiter: u8, row: LineItem<'static>) {
    write!(buffer, "{}", row.l_orderkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_partkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_suppkey).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_linenumber).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_quantity).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_extendedprice).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_discount).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_tax).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.l_returnflag);
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.l_linestatus);
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_shipdate).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_commitdate).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write!(buffer, "{}", row.l_receiptdate).expect("writing to memory is infallible");
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.l_shipinstruct);
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.l_shipmode);
    buffer.push(delimiter);
    write_dsv_str_field(buffer, delimiter, row.l_comment);
    buffer.push(b'\n');
}

/// Define a Source that writes the table in CSV format
macro_rules! define_csv_source {
    ($SOURCE_NAME:ident, $GENERATOR_TYPE:ty, $FORMATTER:ty, $write_row:path) => {
        pub struct $SOURCE_NAME {
            inner: $GENERATOR_TYPE,
            delimiter: u8,
        }

        impl $SOURCE_NAME {
            pub fn new(inner: $GENERATOR_TYPE, delimiter: u8) -> Self {
                Self { inner, delimiter }
            }
        }

        impl Source for $SOURCE_NAME {
            fn header(&self, buffer: Vec<u8>) -> Vec<u8> {
                let mut buffer = buffer;
                write_delimited_header(&mut buffer, self.delimiter, <$FORMATTER>::header());
                buffer
            }

            fn create(self, mut buffer: Vec<u8>) -> Vec<u8> {
                if self.delimiter == b',' {
                    for item in self.inner.iter() {
                        let formatter = <$FORMATTER>::new(item);
                        writeln!(&mut buffer, "{formatter}")
                            .expect("writing to memory is infallible");
                    }
                } else {
                    for item in self.inner.iter() {
                        $write_row(&mut buffer, self.delimiter, item);
                    }
                }
                buffer
            }
        }
    };
}

// Define .csv sources for all tables
define_csv_source!(
    NationCsvSource,
    NationGenerator<'static>,
    NationCsv,
    write_nation_dsv
);
define_csv_source!(
    RegionCsvSource,
    RegionGenerator<'static>,
    RegionCsv,
    write_region_dsv
);
define_csv_source!(
    PartCsvSource,
    PartGenerator<'static>,
    PartCsv,
    write_part_dsv
);
define_csv_source!(
    SupplierCsvSource,
    SupplierGenerator<'static>,
    SupplierCsv,
    write_supplier_dsv
);
define_csv_source!(
    PartSuppCsvSource,
    PartSuppGenerator<'static>,
    PartSuppCsv,
    write_partsupp_dsv
);
define_csv_source!(
    CustomerCsvSource,
    CustomerGenerator<'static>,
    CustomerCsv,
    write_customer_dsv
);
define_csv_source!(
    OrderCsvSource,
    OrderGenerator<'static>,
    OrderCsv,
    write_orders_dsv
);
define_csv_source!(
    LineItemCsvSource,
    LineItemGenerator<'static>,
    LineItemCsv,
    write_lineitem_dsv
);
