use arccstr::ArcCStr;

use chrono::{self, NaiveDateTime};

use nom_sql::Literal;

use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Add, Deref, DerefMut, Div, Mul, Sub};

const FLOAT_PRECISION: f64 = 1000_000_000.0;
const TINYTEXT_WIDTH: usize = 15;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
///
/// Note that cloning a `DataType` using the `Clone` trait is possible, but may result in cache
/// contention on the reference counts for de-duplicated strings. Use `DataType::deep_clone` to
/// clone the *value* of a `DataType` without danger of contention.
#[derive(Eq, Clone, Serialize, Deserialize)]
#[warn(variant_size_differences)]
pub enum DataType {
    /// An empty value.
    None,
    /// A 32-bit numeric value.
    Int(i32),
    /// A 64-bit numeric value.
    BigInt(i64),
    /// A fixed point real value. The first field is the integer part, while the second is the
    /// fractional and must be between -999999999 and 999999999.
    Real(i64, i32),
    /// A reference-counted string-like value.
    Text(ArcCStr),
    /// A tiny string that fits in a pointer
    TinyText([u8; TINYTEXT_WIDTH]),
    /// A timestamp for date/time types.
    Timestamp(NaiveDateTime),
}

impl DataType {
    pub fn to_string(&self) -> String {
        match *self {
            DataType::None => String::from("*"),
            DataType::Text(..) | DataType::TinyText(..) => {
                let text: Cow<str> = self.into();
                format!("{}", text)
            }
            DataType::Int(n) => format!("{}", n),
            DataType::BigInt(n) => format!("{}", n),
            DataType::Real(i, frac) => {
                if i == 0 && frac < 0 {
                    // We have to insert the negative sign ourselves.
                    format!("{}", format!("-0.{:09}", frac.abs()))
                } else {
                    format!("{}", format!("{}.{:09}", i, frac.abs()))
                }
            }
            DataType::Timestamp(ts) => format!("{}", format!("{}", ts.format("%c"))),
        }
    }
}

impl DataType {
    /// Clone the value contained within this `DataType`.
    ///
    /// This method crucially does not cause cache-line conflicts with the underlying data-store
    /// (i.e., the owner of `self`), at the cost of requiring additional allocation and copying.
    pub fn deep_clone(&self) -> Self {
        match *self {
            DataType::Text(ref cstr) => DataType::Text(ArcCStr::from(&**cstr)),
            ref dt => dt.clone(),
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &DataType) -> bool {
        unsafe {
            use std::{mem, slice};
            // if the two datatypes are byte-for-byte identical, they're the same.
            let a: &[u8] = slice::from_raw_parts(mem::transmute(self), mem::size_of::<Self>());
            let b: &[u8] = slice::from_raw_parts(mem::transmute(other), mem::size_of::<Self>());
            if a == b {
                return true;
            }
        }

        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a == b,
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a == b,
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
                let a: Cow<str> = self.into();
                let b: Cow<str> = other.into();
                a == b
            }
            (&DataType::BigInt(a), &DataType::BigInt(b)) => a == b,
            (&DataType::Int(a), &DataType::Int(b)) => a == b,
            (&DataType::BigInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::BigInt(..)) => {
                let a: i64 = self.into();
                let b: i64 = other.into();
                a == b
            }
            (&DataType::Real(ai, af), &DataType::Real(bi, bf)) => ai == bi && af == bf,
            (&DataType::Timestamp(tsa), &DataType::Timestamp(tsb)) => tsa == tsb,
            (&DataType::None, &DataType::None) => true,

            _ => false,
        }
    }
}

use std::cmp::Ordering;
impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &DataType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataType {
    fn cmp(&self, other: &DataType) -> Ordering {
        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a.cmp(b),
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a.cmp(b),
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
                let a: Cow<str> = self.into();
                let b: Cow<str> = other.into();
                a.cmp(&b)
            }
            (&DataType::BigInt(a), &DataType::BigInt(ref b)) => a.cmp(b),
            (&DataType::Int(a), &DataType::Int(b)) => a.cmp(&b),
            (&DataType::BigInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::BigInt(..)) => {
                let a: i64 = self.into();
                let b: i64 = other.into();
                a.cmp(&b)
            }
            (&DataType::Real(ai, af), &DataType::Real(ref bi, ref bf)) => {
                ai.cmp(bi).then_with(|| af.cmp(bf))
            }
            (&DataType::Timestamp(tsa), &DataType::Timestamp(ref tsb)) => tsa.cmp(tsb),
            (&DataType::None, &DataType::None) => Ordering::Equal,

            // order Ints, Reals, Text, Timestamps, None
            (&DataType::Int(..), _) | (&DataType::BigInt(..), _) => Ordering::Greater,
            (&DataType::Real(..), _) => Ordering::Greater,
            (&DataType::Text(..), _) | (&DataType::TinyText(..), _) => Ordering::Greater,
            (&DataType::Timestamp(..), _) => Ordering::Greater,
            (&DataType::None, _) => Ordering::Greater,
        }
    }
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The default derived hash function also hashes the variant tag, which turns out to be
        // rather expensive. This version could (but probably won't) have a higher rate of
        // collisions, but the decreased overhead is worth it.
        match *self {
            DataType::None => {}
            DataType::Int(..) | DataType::BigInt(..) => {
                let n: i64 = self.into();
                n.hash(state)
            }
            DataType::Real(i, f) => {
                i.hash(state);
                f.hash(state);
            }
            DataType::Text(..) | DataType::TinyText(..) => {
                let t: Cow<str> = self.into();
                t.hash(state)
            }
            DataType::Timestamp(ts) => ts.hash(state),
        }
    }
}

impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::BigInt(s)
    }
}

impl From<i32> for DataType {
    fn from(s: i32) -> Self {
        DataType::Int(s as i32)
    }
}

impl From<usize> for DataType {
    fn from(s: usize) -> Self {
        DataType::Int(s as i32)
    }
}

impl From<f64> for DataType {
    fn from(f: f64) -> Self {
        if !f.is_finite() {
            panic!();
        }

        let mut i = f.trunc() as i64;
        let mut frac = (f.fract() * FLOAT_PRECISION).round() as i32;
        if frac == 1000_000_000 {
            i += 1;
            frac = 0;
        } else if frac == -1000_000_000 {
            i -= 1;
            frac = 0;
        }

        DataType::Real(i, frac)
    }
}

impl<'a> From<&'a Literal> for DataType {
    fn from(l: &'a Literal) -> Self {
        match *l {
            Literal::Null => DataType::None,
            Literal::Integer(i) => i.into(),
            Literal::String(ref s) => s.as_str().into(),
            Literal::CurrentTimestamp => {
                let ts = chrono::Local::now().naive_local();
                DataType::Timestamp(ts)
            }
            Literal::FixedPoint(ref r) => DataType::Real(r.integral as i64, r.fractional as i32),
            _ => unimplemented!(),
        }
    }
}

impl From<Literal> for DataType {
    fn from(l: Literal) -> Self {
        match l {
            Literal::Null => DataType::None,
            Literal::Integer(i) => i.into(),
            Literal::String(s) => s.as_str().into(),
            Literal::CurrentTimestamp => {
                let ts = chrono::Local::now().naive_local();
                DataType::Timestamp(ts)
            }
            Literal::FixedPoint(r) => DataType::Real(r.integral as i64, r.fractional as i32),
            _ => unimplemented!(),
        }
    }
}

use std::borrow::Cow;
impl<'a> Into<Cow<'a, str>> for &'a DataType {
    fn into(self) -> Cow<'a, str> {
        match *self {
            DataType::Text(ref s) => s.to_string_lossy(),
            DataType::TinyText(ref bts) => {
                if bts[TINYTEXT_WIDTH - 1] == 0 {
                    // NULL terminated CStr
                    use std::ffi::CStr;
                    let null = bts.iter().position(|&i| i == 0).unwrap() + 1;
                    CStr::from_bytes_with_nul(&bts[0..null])
                        .unwrap()
                        .to_string_lossy()
                } else {
                    // String is exactly eight bytes
                    String::from_utf8_lossy(&bts[..])
                }
            }
            _ => unreachable!(),
        }
    }
}

impl<'a> Into<String> for &'a DataType {
    fn into(self) -> String {
        let cow: Cow<str> = self.into();
        cow.to_string()
    }
}

impl Into<String> for DataType {
    fn into(self) -> String {
        (&self).into()
    }
}

impl Into<i64> for DataType {
    fn into(self) -> i64 {
        match self {
            DataType::BigInt(s) => s,
            DataType::Int(s) => s as i64,
            _ => unreachable!(),
        }
    }
}

impl<'a> Into<i64> for &'a DataType {
    fn into(self) -> i64 {
        match *self {
            DataType::BigInt(s) => s,
            DataType::Int(s) => s as i64,
            _ => unreachable!(),
        }
    }
}

impl Into<i32> for DataType {
    fn into(self) -> i32 {
        if let DataType::Int(s) = self {
            s
        } else {
            unreachable!();
        }
    }
}

impl<'a> Into<f64> for &'a DataType {
    fn into(self) -> f64 {
        match self {
            &DataType::Real(i, f) => i as f64 + (f as f64) / FLOAT_PRECISION,
            &DataType::Int(i) => i as f64,
            &DataType::BigInt(i) => i as f64,
            _ => unreachable!(),
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        let len = s.as_bytes().len();
        if len <= TINYTEXT_WIDTH {
            let mut bytes = [0; TINYTEXT_WIDTH];
            if len != 0 {
                let bts = &mut bytes[0..len];
                bts.copy_from_slice(s.as_bytes());
            }
            DataType::TinyText(bytes)
        } else {
            use std::convert::TryFrom;
            DataType::Text(ArcCStr::try_from(s).unwrap())
        }
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        DataType::from(s.to_owned())
    }
}

// Performs an arithmetic operation on two numeric DataTypes,
// returning a new DataType as the result.
macro_rules! arithmetic_operation (
    ($op:tt, $first:ident, $second:ident) => (
        match ($first, $second) {
            (&DataType::None, _) | (_, &DataType::None) => DataType::None,
            (&DataType::Int(a), &DataType::Int(b)) => (a $op b).into(),
            (&DataType::BigInt(a), &DataType::BigInt(b)) => (a $op b).into(),
            (&DataType::Int(a), &DataType::BigInt(b)) => ((a as i64) $op b).into(),
            (&DataType::BigInt(a), &DataType::Int(b)) => (a $op (b as i64)).into(),

            (first @ &DataType::Int(..), second @ &DataType::Real(..)) |
            (first @ &DataType::Real(..), second @ &DataType::Int(..)) |
            (first @ &DataType::Real(..), second @ &DataType::Real(..)) => {
                let a: f64 = first.into();
                let b: f64 = second.into();
                (a $op b).into()
            }
            (first, second) => panic!(
                format!(
                    "can't {} a {:?} and {:?}",
                    stringify!($op),
                    first,
                    second,
                )
            ),
        }
    );
);

impl<'a, 'b> Add<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn add(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(+, self, other)
    }
}

impl<'a, 'b> Sub<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn sub(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(-, self, other)
    }
}

impl<'a, 'b> Mul<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn mul(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(*, self, other)
    }
}

impl<'a, 'b> Div<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn div(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(/, self, other)
    }
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "None"),
            DataType::Text(..) => {
                let text: Cow<str> = self.into();
                write!(f, "Text({:?})", text)
            }
            DataType::TinyText(..) => {
                let text: Cow<str> = self.into();
                write!(f, "TinyText({:?})", text)
            }
            DataType::Timestamp(ts) => write!(f, "Timestamp({:?})", ts),
            DataType::Real(..) => write!(f, "Real({})", self),
            DataType::Int(n) => write!(f, "Int({})", n),
            DataType::BigInt(n) => write!(f, "BigInt({})", n),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "*"),
            DataType::Text(..) | DataType::TinyText(..) => {
                let text: Cow<str> = self.into();
                write!(f, "\"{}\"", text)
            }
            DataType::Int(n) => write!(f, "{}", n),
            DataType::BigInt(n) => write!(f, "{}", n),
            DataType::Real(i, frac) => {
                if i == 0 && frac < 0 {
                    // We have to insert the negative sign ourselves.
                    write!(f, "{}", format!("-0.{:09}", frac.abs()))
                } else {
                    write!(f, "{}", format!("{}.{:09}", i, frac.abs()))
                }
            }
            DataType::Timestamp(ts) => write!(f, "{}", format!("{}", ts.format("%c"))),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Operation {
    Add,
    Sub,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Modification {
    Set(DataType),
    Apply(Operation, DataType),
    None,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum TableOperation {
    Insert(Vec<DataType>),
    Delete {
        key: Vec<DataType>,
    },
    InsertOrUpdate {
        row: Vec<DataType>,
        update: Vec<Modification>,
    },
    Update {
        set: Vec<Modification>,
        key: Vec<DataType>,
    },
}

impl TableOperation {
    pub fn row(&self) -> Option<&[DataType]> {
        match *self {
            TableOperation::Insert(ref r) => Some(r),
            TableOperation::InsertOrUpdate { ref row, .. } => Some(row),
            _ => None,
        }
    }
}

impl From<Vec<DataType>> for TableOperation {
    fn from(other: Vec<DataType>) -> Self {
        TableOperation::Insert(other)
    }
}

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[warn(variant_size_differences)]
pub enum Record {
    Positive(Vec<DataType>),
    Negative(Vec<DataType>),
}

impl Record {
    pub fn rec(&self) -> &[DataType] {
        match *self {
            Record::Positive(ref v) | Record::Negative(ref v) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn extract(self) -> (Vec<DataType>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
        }
    }
}

impl Deref for Record {
    type Target = Vec<DataType>;
    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) | Record::Negative(ref r) => r,
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) | Record::Negative(ref mut r) => r,
        }
    }
}

impl From<Vec<DataType>> for Record {
    fn from(other: Vec<DataType>) -> Self {
        Record::Positive(other)
    }
}

impl From<(Vec<DataType>, bool)> for Record {
    fn from(other: (Vec<DataType>, bool)) -> Self {
        if other.1 {
            Record::Positive(other.0)
        } else {
            Record::Negative(other.0)
        }
    }
}

impl Into<Vec<Record>> for Records {
    fn into(self) -> Vec<Record> {
        self.0
    }
}

use std::iter::FromIterator;
impl FromIterator<Record> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Record>,
    {
        Records(iter.into_iter().collect())
    }
}
impl FromIterator<Vec<DataType>> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Vec<DataType>>,
    {
        Records(iter.into_iter().map(Record::Positive).collect())
    }
}

impl IntoIterator for Records {
    type Item = Record;
    type IntoIter = ::std::vec::IntoIter<Record>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
impl<'a> IntoIterator for &'a Records {
    type Item = &'a Record;
    type IntoIter = ::std::slice::Iter<'a, Record>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Represents a set of records returned from a query.
pub type Datas = Vec<Vec<DataType>>;

#[derive(Clone, Default, PartialEq, Debug, Serialize, Deserialize)]
pub struct Records(Vec<Record>);

impl Deref for Records {
    type Target = Vec<Record>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Records {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Into<Records> for Record {
    fn into(self) -> Records {
        Records(vec![self])
    }
}

impl Into<Records> for Vec<Record> {
    fn into(self) -> Records {
        Records(self)
    }
}

impl Into<Records> for Vec<Vec<DataType>> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

impl Into<Records> for Vec<(Vec<DataType>, bool)> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

pub trait SizeOf {
    fn deep_size_of(&self) -> usize;
    fn size_of(&self) -> usize;
}

impl SizeOf for DataType {
    fn deep_size_of(&self) -> usize {
        use std::mem::size_of_val;

        let inner = match *self {
            DataType::Text(ref t) => size_of_val(t) as usize + t.to_bytes().len() as usize,
            _ => 0usize,
        };

        self.size_of() + inner
    }

    fn size_of(&self) -> usize {
        use std::mem::size_of;

        // doesn't include data if stored externally
        size_of::<DataType>() as usize
    }
}

impl SizeOf for Vec<DataType> {
    fn deep_size_of(&self) -> usize {
        use std::mem::size_of_val;

        size_of_val(self) as usize + self.iter().fold(0usize, |acc, d| acc + d.deep_size_of())
    }

    fn size_of(&self) -> usize {
        use std::mem::{size_of, size_of_val};

        size_of_val(self) as usize + size_of::<DataType>() as usize * self.len() as usize
    }
}
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn real_to_string() {
//         let a: DataType = (2.5).into();
//         let b: DataType = (-2.01).into();
//         let c: DataType = (-0.012345678).into();
//         assert_eq!(a.to_string(), "2.500000000");
//         assert_eq!(b.to_string(), "-2.010000000");
//         assert_eq!(c.to_string(), "-0.012345678");
//     }
//
//     #[test]
//     fn real_to_float() {
//         let original = 2.5;
//         let data_type: DataType = original.into();
//         let converted: f64 = (&data_type).into();
//         assert_eq!(original, converted);
//     }
//
//     #[test]
//     fn add_data_types() {
//         assert_eq!(&DataType::from(1) + &DataType::from(2), 3.into());
//         assert_eq!(&DataType::from(1.5) + &DataType::from(2), (3.5).into());
//         assert_eq!(&DataType::from(2) + &DataType::from(1.5), (3.5).into());
//         assert_eq!(&DataType::from(1.5) + &DataType::from(2.5), (4.0).into());
//         assert_eq!(&DataType::BigInt(1) + &DataType::BigInt(2), 3.into());
//         assert_eq!(&DataType::from(1) + &DataType::BigInt(2), 3.into());
//         assert_eq!(&DataType::BigInt(2) + &DataType::from(1), 3.into());
//     }

//     #[test]
//     fn subtract_data_types() {
//         assert_eq!(&DataType::from(2) - &DataType::from(1), 1.into());
//         assert_eq!(&DataType::from(3.5) - &DataType::from(2), (1.5).into());
//         assert_eq!(&DataType::from(2) - &DataType::from(1.5), (0.5).into());
//         assert_eq!(&DataType::from(3.5) - &DataType::from(2.0), (1.5).into());
//         assert_eq!(&DataType::BigInt(1) - &DataType::BigInt(2), (-1).into());
//         assert_eq!(&DataType::from(1) - &DataType::BigInt(2), (-1).into());
//         assert_eq!(&DataType::BigInt(2) - &DataType::from(1), 1.into());
//     }
//
//     #[test]
//     fn multiply_data_types() {
//         assert_eq!(&DataType::from(2) * &DataType::from(1), 2.into());
//         assert_eq!(&DataType::from(3.5) * &DataType::from(2), (7.0).into());
//         assert_eq!(&DataType::from(2) * &DataType::from(1.5), (3.0).into());
//         assert_eq!(&DataType::from(3.5) * &DataType::from(2.0), (7.0).into());
//         assert_eq!(&DataType::BigInt(1) * &DataType::BigInt(2), 2.into());
//         assert_eq!(&DataType::from(1) * &DataType::BigInt(2), 2.into());
//         assert_eq!(&DataType::BigInt(2) * &DataType::from(1), 2.into());
//     }
//
//     #[test]
//     fn divide_data_types() {
//         assert_eq!(&DataType::from(2) / &DataType::from(1), 2.into());
//         assert_eq!(&DataType::from(7.5) / &DataType::from(2), (3.75).into());
//         assert_eq!(&DataType::from(7) / &DataType::from(2.5), (2.8).into());
//         assert_eq!(&DataType::from(3.5) / &DataType::from(2.0), (1.75).into());
//         assert_eq!(&DataType::BigInt(4) / &DataType::BigInt(2), 2.into());
//         assert_eq!(&DataType::from(4) / &DataType::BigInt(2), 2.into());
//         assert_eq!(&DataType::BigInt(4) / &DataType::from(2), 2.into());
//     }
//
//     #[test]
//     #[should_panic(expected = "can't + a TinyText(\"hi\") and Int(5)")]
//     fn add_invalid_types() {
//         let a: DataType = "hi".into();
//         let b: DataType = 5.into();
//         let _ = &a + &b;
//     }
//
//     #[test]
//     fn data_type_debug() {
//         let tiny_text: DataType = "hi".into();
//         let text: DataType = "I contain ' and \"".into();
//         let real: DataType = (-0.05).into();
//         let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
//         let int = DataType::Int(5);
//         let big_int = DataType::BigInt(5);
//         assert_eq!(format!("{:?}", tiny_text), "TinyText(\"hi\")");
//         assert_eq!(format!("{:?}", text), "Text(\"I contain \\' and \\\"\")");
//         assert_eq!(format!("{:?}", real), "Real(-0.050000000)");
//         assert_eq!(
//             format!("{:?}", timestamp),
//             "Timestamp(1970-01-01T00:00:00.042)"
//         );
//         assert_eq!(format!("{:?}", int), "Int(5)");
//         assert_eq!(format!("{:?}", big_int), "BigInt(5)");
//     }
//
//     #[test]
//     fn data_type_display() {
//         let tiny_text: DataType = "hi".into();
//         let text: DataType = "this is a very long text indeed".into();
//         let real: DataType = (-0.05).into();
//         let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
//         let int = DataType::Int(5);
//         let big_int = DataType::BigInt(5);
//         assert_eq!(format!("{}", tiny_text), "\"hi\"");
//         assert_eq!(format!("{}", text), "\"this is a very long text indeed\"");
//         assert_eq!(format!("{}", real), "-0.050000000");
//         assert_eq!(format!("{}", timestamp), "Thu Jan  1 00:00:00 1970");
//         assert_eq!(format!("{}", int), "5");
//         assert_eq!(format!("{}", big_int), "5");
//     }
//
//     #[test]
//     fn data_type_fungibility() {
//         use std::convert::TryFrom;
//
//         let txt1: DataType = "hi".into();
//         let txt12: DataType = "no".into();
//         let txt2: DataType = DataType::Text(ArcCStr::try_from("hi").unwrap());
//         let text: DataType = "this is a very long text indeed".into();
//         let text2: DataType = "this is another long text".into();
//         let real: DataType = (-0.05).into();
//         let real2: DataType = (-0.06).into();
//         let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
//         let time2 = DataType::Timestamp(NaiveDateTime::from_timestamp(1, 42_000_000));
//         let shrt = DataType::Int(5);
//         let shrt6 = DataType::Int(6);
//         let long = DataType::BigInt(5);
//         let long6 = DataType::BigInt(6);
//
//         assert_eq!(txt1, txt1);
//         assert_eq!(txt2, txt2);
//         assert_eq!(text, text);
//         assert_eq!(shrt, shrt);
//         assert_eq!(long, long);
//         assert_eq!(real, real);
//         assert_eq!(time, time);
//
//         // coercion
//         assert_eq!(txt1, txt2);
//         assert_eq!(txt2, txt1);
//         assert_eq!(shrt, long);
//         assert_eq!(long, shrt);
//
//         // negation
//         assert_ne!(txt1, txt12);
//         assert_ne!(txt1, text);
//         assert_ne!(txt1, real);
//         assert_ne!(txt1, time);
//         assert_ne!(txt1, shrt);
//         assert_ne!(txt1, long);
//
//         assert_ne!(txt2, txt12);
//         assert_ne!(txt2, text);
//         assert_ne!(txt2, real);
//         assert_ne!(txt2, time);
//         assert_ne!(txt2, shrt);
//         assert_ne!(txt2, long);
//
//         assert_ne!(text, text2);
//         assert_ne!(text, txt1);
//         assert_ne!(text, txt2);
//         assert_ne!(text, real);
//         assert_ne!(text, time);
//         assert_ne!(text, shrt);
//         assert_ne!(text, long);
//
//         assert_ne!(real, real2);
//         assert_ne!(real, txt1);
//         assert_ne!(real, txt2);
//         assert_ne!(real, text);
//         assert_ne!(real, time);
//         assert_ne!(real, shrt);
//         assert_ne!(real, long);
//
//         assert_ne!(time, time2);
//         assert_ne!(time, txt1);
//         assert_ne!(time, txt2);
//         assert_ne!(time, text);
//         assert_ne!(time, real);
//         assert_ne!(time, shrt);
//         assert_ne!(time, long);
//
//         assert_ne!(shrt, shrt6);
//         assert_ne!(shrt, txt1);
//         assert_ne!(shrt, txt2);
//         assert_ne!(shrt, text);
//         assert_ne!(shrt, real);
//         assert_ne!(shrt, time);
//         assert_ne!(shrt, long6);
//
//         assert_ne!(long, long6);
//         assert_ne!(long, txt1);
//         assert_ne!(long, txt2);
//         assert_ne!(long, text);
//         assert_ne!(long, real);
//         assert_ne!(long, time);
//         assert_ne!(long, shrt6);
//
//         use std::cmp::Ordering;
//         assert_eq!(txt1.cmp(&txt1), Ordering::Equal);
//         assert_eq!(txt2.cmp(&txt2), Ordering::Equal);
//         assert_eq!(text.cmp(&text), Ordering::Equal);
//         assert_eq!(shrt.cmp(&shrt), Ordering::Equal);
//         assert_eq!(long.cmp(&long), Ordering::Equal);
//         assert_eq!(real.cmp(&real), Ordering::Equal);
//         assert_eq!(time.cmp(&time), Ordering::Equal);
//
//         // coercion
//         assert_eq!(txt1.cmp(&txt2), Ordering::Equal);
//         assert_eq!(txt2.cmp(&txt1), Ordering::Equal);
//         assert_eq!(shrt.cmp(&long), Ordering::Equal);
//         assert_eq!(long.cmp(&shrt), Ordering::Equal);
//
//         // negation
//         assert_ne!(txt1.cmp(&txt12), Ordering::Equal);
//         assert_ne!(txt1.cmp(&text), Ordering::Equal);
//         assert_ne!(txt1.cmp(&real), Ordering::Equal);
//         assert_ne!(txt1.cmp(&time), Ordering::Equal);
//         assert_ne!(txt1.cmp(&shrt), Ordering::Equal);
//         assert_ne!(txt1.cmp(&long), Ordering::Equal);
//
//         assert_ne!(txt2.cmp(&txt12), Ordering::Equal);
//         assert_ne!(txt2.cmp(&text), Ordering::Equal);
//         assert_ne!(txt2.cmp(&real), Ordering::Equal);
//         assert_ne!(txt2.cmp(&time), Ordering::Equal);
//         assert_ne!(txt2.cmp(&shrt), Ordering::Equal);
//         assert_ne!(txt2.cmp(&long), Ordering::Equal);
//
//         assert_ne!(text.cmp(&text2), Ordering::Equal);
//         assert_ne!(text.cmp(&txt1), Ordering::Equal);
//         assert_ne!(text.cmp(&txt2), Ordering::Equal);
//         assert_ne!(text.cmp(&real), Ordering::Equal);
//         assert_ne!(text.cmp(&time), Ordering::Equal);
//         assert_ne!(text.cmp(&shrt), Ordering::Equal);
//         assert_ne!(text.cmp(&long), Ordering::Equal);
//
//         assert_ne!(real.cmp(&real2), Ordering::Equal);
//         assert_ne!(real.cmp(&txt1), Ordering::Equal);
//         assert_ne!(real.cmp(&txt2), Ordering::Equal);
//         assert_ne!(real.cmp(&text), Ordering::Equal);
//         assert_ne!(real.cmp(&time), Ordering::Equal);
//         assert_ne!(real.cmp(&shrt), Ordering::Equal);
//         assert_ne!(real.cmp(&long), Ordering::Equal);
//
//         assert_ne!(time.cmp(&time2), Ordering::Equal);
//         assert_ne!(time.cmp(&txt1), Ordering::Equal);
//         assert_ne!(time.cmp(&txt2), Ordering::Equal);
//         assert_ne!(time.cmp(&text), Ordering::Equal);
//         assert_ne!(time.cmp(&real), Ordering::Equal);
//         assert_ne!(time.cmp(&shrt), Ordering::Equal);
//         assert_ne!(time.cmp(&long), Ordering::Equal);
//
//         assert_ne!(shrt.cmp(&shrt6), Ordering::Equal);
//         assert_ne!(shrt.cmp(&txt1), Ordering::Equal);
//         assert_ne!(shrt.cmp(&txt2), Ordering::Equal);
//         assert_ne!(shrt.cmp(&text), Ordering::Equal);
//         assert_ne!(shrt.cmp(&real), Ordering::Equal);
//         assert_ne!(shrt.cmp(&time), Ordering::Equal);
//         assert_ne!(shrt.cmp(&long6), Ordering::Equal);
//
//         assert_ne!(long.cmp(&long6), Ordering::Equal);
//         assert_ne!(long.cmp(&txt1), Ordering::Equal);
//         assert_ne!(long.cmp(&txt2), Ordering::Equal);
//         assert_ne!(long.cmp(&text), Ordering::Equal);
//         assert_ne!(long.cmp(&real), Ordering::Equal);
//         assert_ne!(long.cmp(&time), Ordering::Equal);
//         assert_ne!(long.cmp(&shrt6), Ordering::Equal);
//
//         let hash = |dt: &DataType| {
//             use std::collections::hash_map::DefaultHasher;
//             use std::hash::{Hash, Hasher};
//             let mut s = DefaultHasher::new();
//             dt.hash(&mut s);
//             s.finish()
//         };
//
//         assert_eq!(hash(&txt1), hash(&txt1));
//         assert_eq!(hash(&txt2), hash(&txt2));
//         assert_eq!(hash(&text), hash(&text));
//         assert_eq!(hash(&shrt), hash(&shrt));
//         assert_eq!(hash(&long), hash(&long));
//         assert_eq!(hash(&real), hash(&real));
//         assert_eq!(hash(&time), hash(&time));
//
//         // coercion
//         assert_eq!(hash(&txt1), hash(&txt2));
//         assert_eq!(hash(&txt2), hash(&txt1));
//         assert_eq!(hash(&shrt), hash(&long));
//         assert_eq!(hash(&long), hash(&shrt));
//
//         // negation
//         assert_ne!(hash(&txt1), hash(&txt12));
//         assert_ne!(hash(&txt1), hash(&text));
//         assert_ne!(hash(&txt1), hash(&real));
//         assert_ne!(hash(&txt1), hash(&time));
//         assert_ne!(hash(&txt1), hash(&shrt));
//         assert_ne!(hash(&txt1), hash(&long));
//
//         assert_ne!(hash(&txt2), hash(&txt12));
//         assert_ne!(hash(&txt2), hash(&text));
//         assert_ne!(hash(&txt2), hash(&real));
//         assert_ne!(hash(&txt2), hash(&time));
//         assert_ne!(hash(&txt2), hash(&shrt));
//         assert_ne!(hash(&txt2), hash(&long));
//
//         assert_ne!(hash(&text), hash(&text2));
//         assert_ne!(hash(&text), hash(&txt1));
//         assert_ne!(hash(&text), hash(&txt2));
//         assert_ne!(hash(&text), hash(&real));
//         assert_ne!(hash(&text), hash(&time));
//         assert_ne!(hash(&text), hash(&shrt));
//         assert_ne!(hash(&text), hash(&long));
//
//         assert_ne!(hash(&real), hash(&real2));
//         assert_ne!(hash(&real), hash(&txt1));
//         assert_ne!(hash(&real), hash(&txt2));
//         assert_ne!(hash(&real), hash(&text));
//         assert_ne!(hash(&real), hash(&time));
//         assert_ne!(hash(&real), hash(&shrt));
//         assert_ne!(hash(&real), hash(&long));
//
//         assert_ne!(hash(&time), hash(&time2));
//         assert_ne!(hash(&time), hash(&txt1));
//         assert_ne!(hash(&time), hash(&txt2));
//         assert_ne!(hash(&time), hash(&text));
//         assert_ne!(hash(&time), hash(&real));
//         assert_ne!(hash(&time), hash(&shrt));
//         assert_ne!(hash(&time), hash(&long));
//
//         assert_ne!(hash(&shrt), hash(&shrt6));
//         assert_ne!(hash(&shrt), hash(&txt1));
//         assert_ne!(hash(&shrt), hash(&txt2));
//         assert_ne!(hash(&shrt), hash(&text));
//         assert_ne!(hash(&shrt), hash(&real));
//         assert_ne!(hash(&shrt), hash(&time));
//         assert_ne!(hash(&shrt), hash(&long6));
//
//         assert_ne!(hash(&long), hash(&long6));
//         assert_ne!(hash(&long), hash(&txt1));
//         assert_ne!(hash(&long), hash(&txt2));
//         assert_ne!(hash(&long), hash(&text));
//         assert_ne!(hash(&long), hash(&real));
//         assert_ne!(hash(&long), hash(&time));
//         assert_ne!(hash(&long), hash(&shrt6));
//     }
//
//     #[test]
//     fn data_type_mem_size() {
//         use std::convert::TryFrom;
//         use std::mem::{size_of, size_of_val};
//
//         let txt: DataType = DataType::Text(ArcCStr::try_from("hi").unwrap());
//         let shrt = DataType::Int(5);
//         let long = DataType::BigInt(5);
//         let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
//
//         let rec = vec![DataType::Int(5), "asdfasdfasdfasdf".into(), "asdf".into()];
//
//         // DataType should always use 16 bytes itself
//         assert_eq!(size_of::<DataType>(), 16);
//         assert_eq!(size_of_val(&txt), 16);
//         assert_eq!(size_of_val(&txt) as usize, txt.size_of());
//         assert_eq!(txt.deep_size_of(), txt.size_of() + 8 + 2); // DataType + ArcCStr's ptr + 2 chars
//         assert_eq!(size_of_val(&shrt), 16);
//         assert_eq!(size_of_val(&long), 16);
//         assert_eq!(size_of_val(&time), 16);
//         assert_eq!(size_of_val(&time) as usize, time.size_of());
//         assert_eq!(time.deep_size_of(), 16); // DataType + inline NaiveDateTime
//
//         assert_eq!(size_of_val(&rec), 24);
//         assert_eq!(rec.size_of(), 24 + 3 * 16);
//         assert_eq!(rec.deep_size_of(), 24 + 3 * 16 + (8 + 16));
//     }
// }
