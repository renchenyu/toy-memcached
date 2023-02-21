use std::io::Cursor;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use num_enum::TryFromPrimitive;

use crate::store::Value;

pub(crate) mod response_status {
    pub const MSG_KEY_NOT_FOUND: &str = "Not found";
    pub const MSG_KEY_EXISTS: &str = "Key exists";
    pub const MSG_VALUE_TOO_LARGE: &str = "Value too large";
    pub const MSG_INVALID_ARGUMENTS: &str = "Invalid arguments";
    pub const MSG_ITEM_NOT_STORED: &str = "Item not stored";
    pub const MSG_INCR_ON_NON_NUMERIC_VALUE: &str = "Incr on non-numeric value";
    pub const MSG_DECR_ON_NON_NUMERIC_VALUE: &str = "Decr on non-numeric value";
    pub const MSG_UNKNOWN_COMMAND: &str = "Unknown command";
    pub const MSG_OOM: &str = "Out of memory";
}

#[derive(TryFromPrimitive, Debug, Copy, Clone)]
#[repr(u8)]
pub(crate) enum Magic {
    Req = 0x80,
    Resp,
}

#[derive(TryFromPrimitive, Debug, Copy, Clone)]
#[repr(u16)]
pub(crate) enum Status {
    OK = 0x0000,
    KeyNotFound,
    KeyExists,
    ValueTooLarge,
    InvalidArguments,
    ItemNotStored,
    OpOnNan,
    UnknownCommand,
    OOM,
}

#[derive(TryFromPrimitive, Debug, Copy, Clone)]
#[repr(u8)]
pub(crate) enum Cmd {
    Get = 0x00,
    Set,
    Add,
    Replace,
    Delete,
    Incr,
    Decr,
    Quit,
    Flush,
    GetQ,
    NoOp,
    Version,
    GetK,
    GetKQ,
    Append,
    Prepend,
    Stat,
    SetQ,
    AddQ,
    ReplaceQ,
    DeleteQ,
    IncrQ,
    DecrQ,
    QuitQ,
    FlushQ,
    AppendQ,
    PrependQ,
}

impl Cmd {
    pub fn is_quiet(&self) -> bool {
        match &self {
            Self::GetQ
            | Self::GetKQ
            | Self::SetQ
            | Self::AddQ
            | Self::ReplaceQ
            | Self::DeleteQ
            | Self::IncrQ
            | Self::DecrQ
            | Self::QuitQ
            | Self::FlushQ
            | Self::AppendQ
            | Self::PrependQ => true,
            _ => false,
        }
    }
}

const HEADER_LEN: usize = 24;

#[derive(Debug)]
pub(crate) struct Header {
    pub magic: u8,
    pub opcode: u8,
    pub key_length: u16,

    pub extras_length: u8,
    pub data_type: u8,
    pub status: u16,

    pub body_length: u32,
    pub opaque: u32,
    pub cas: u64,
}

#[derive(Debug)]
pub(crate) enum PacketError {
    InComplete,
    Invalid,
}

pub(crate) enum FormatError {
    InvalidOpcode,
    InvalidArguments,
}

#[derive(Debug)]
pub(crate) struct Request {
    pub header: Header,
    pub extras: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

impl Request {
    pub fn get_value(&self) -> Value {
        assert_eq!(self.extras.len(), 8);

        let mut e = self.extras.clone();
        let extra = e.get_u32();
        let expire: u32 = e.get_u32();
        let mut expired_at = None;
        if expire != 0 {
            expired_at = Some(Instant::now() + Duration::new(expire as u64, 0));
        }

        Value {
            data: self.value.clone(),
            cas: self.header.cas,
            extra,
            expired_at,
            stored_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        }
    }

    pub fn check_parse(data: &[u8]) -> Result<(), PacketError> {
        if data.len() < HEADER_LEN {
            return Err(PacketError::InComplete);
        }

        let bl: [u8; 4] = (&data[8..12]).try_into().unwrap();
        let body_length = u32::from_be_bytes(bl);
        if data.len() < HEADER_LEN + body_length as usize {
            return Err(PacketError::InComplete);
        }

        let kl: [u8; 2] = (&data[2..4]).try_into().unwrap();
        let key_length = u16::from_be_bytes(kl) as u32;
        let extras_length = data[4] as u32;

        if key_length + extras_length > body_length {
            return Err(PacketError::Invalid);
        }

        Ok(())
    }

    fn parse_header(data: &mut Cursor<&[u8]>) -> Header {
        let magic = data.get_u8();
        let opcode = data.get_u8();
        let key_length = data.get_u16();
        let extras_length = data.get_u8();
        let data_type = data.get_u8();
        let status = data.get_u16();
        let body_length = data.get_u32();
        let opaque = data.get_u32();
        let cas = data.get_u64();
        Header {
            magic,
            opcode,
            key_length,
            extras_length,
            data_type,
            status,
            body_length,
            opaque,
            cas,
        }
    }

    pub fn parse(data: &mut Cursor<&[u8]>) -> Request {
        let header = Self::parse_header(data);
        let extras_len = header.extras_length as usize;
        let key_len = header.key_length as usize;
        let val_len = header.body_length as usize - extras_len - key_len;

        let extras = data.copy_to_bytes(extras_len);
        let key = data.copy_to_bytes(key_len);
        let value = data.copy_to_bytes(val_len);

        Request {
            header,
            extras,
            key,
            value,
        }
    }

    pub fn check_valid(&self) -> Result<(), FormatError> {
        let cmd = Cmd::try_from(self.header.opcode).map_err(|_| FormatError::InvalidOpcode)?;

        match cmd {
            Cmd::Get | Cmd::GetQ | Cmd::GetK | Cmd::GetKQ => self.check_valid_get(),
            Cmd::Set | Cmd::SetQ | Cmd::Add | Cmd::AddQ | Cmd::Replace | Cmd::ReplaceQ => {
                self.check_valid_set()
            }
            Cmd::Delete | Cmd::DeleteQ => self.check_valid_delete(),
            Cmd::Incr | Cmd::IncrQ | Cmd::Decr | Cmd::DecrQ => self.check_valid_incr(),
            Cmd::Quit | Cmd::QuitQ => self.check_valid_quit(),
            Cmd::Flush | Cmd::FlushQ => self.check_valid_flush(),
            Cmd::NoOp => self.check_valid_noop(),
            Cmd::Version => self.check_valid_version(),
            Cmd::Append | Cmd::AppendQ | Cmd::Prepend | Cmd::PrependQ => self.check_valid_append(),
            Cmd::Stat => self.check_valid_stat(),
        }
    }

    // For Get, Get Quietly, Get Key, Get Key Quietly
    fn check_valid_get(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MUST have key.
        // MUST NOT have value.
        if !self.extras.is_empty() || self.key.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For Set, Add, Replace
    fn check_valid_set(&self) -> Result<(), FormatError> {
        // MUST have extras.
        // MUST have key.
        // MUST have value.
        if self.extras.len() != 8 || self.key.is_empty() || self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For delete
    fn check_valid_delete(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MUST have key.
        // MUST NOT have value.
        if !self.extras.is_empty() || self.key.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For increment, decrement
    fn check_valid_incr(&self) -> Result<(), FormatError> {
        // MUST have extras
        // 8 byte value to add / subtract
        // 8 byte initial value (unsigned)
        // 4 byte expiration time

        // MUST have key.
        // MUST NOT have value.
        if self.extras.len() != 20 || self.key.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For Quit
    fn check_valid_quit(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MUST NOT have key.
        // MUST NOT have value.

        if !self.extras.is_empty() || !self.key.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For flush
    fn check_valid_flush(&self) -> Result<(), FormatError> {
        // MAY have extras.
        // MUST NOT have key.
        // MUST NOT have value.
        if (!self.extras.is_empty() && self.extras.len() != 4)
            || !self.key.is_empty()
            || !self.value.is_empty()
        {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For NoOp
    fn check_valid_noop(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MUST NOT have key.
        // MUST NOT have value.

        if !self.extras.is_empty() || !self.key.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For Version
    fn check_valid_version(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MUST NOT have key.
        // MUST NOT have value.

        if !self.extras.is_empty() || !self.key.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    // For Append, Prepend
    fn check_valid_append(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MUST have key.
        // MUST have value.
        if !self.extras.is_empty() || self.key.is_empty() || self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }

    fn check_valid_stat(&self) -> Result<(), FormatError> {
        // MUST NOT have extras.
        // MAY have key.
        // MUST NOT have value.
        if !self.extras.is_empty() || !self.value.is_empty() {
            return Err(FormatError::InvalidArguments);
        }
        Ok(())
    }
}

pub(crate) enum Action {
    Return,
    Cache,
    Ignore,
}

#[derive(Debug)]
pub(crate) struct Response {
    pub header: Header,
    pub extras: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

impl Response {
    pub fn success(req: &Request, extras: Bytes, key: Bytes, value: Bytes, cas: u64) -> Self {
        let body_length = extras.len() + key.len() + value.len();
        Self {
            header: Header {
                magic: Magic::Resp as u8,
                opcode: req.header.opcode,
                key_length: key.len() as u16,
                extras_length: extras.len() as u8,
                data_type: 0,
                status: Status::OK as u16,
                body_length: body_length as u32,
                opaque: req.header.opaque,
                cas,
            },
            extras,
            key,
            value,
        }
    }

    pub fn error(req: &Request, status: u16, msg: Bytes) -> Self {
        Self {
            header: Header {
                magic: Magic::Resp as u8,
                opcode: req.header.opcode,
                key_length: 0,
                extras_length: 0,
                data_type: 0,
                status: status as u16,
                body_length: msg.len() as u32,
                opaque: req.header.opaque,
                cas: 0,
            },
            extras: Bytes::new(),
            key: Bytes::new(),
            value: msg,
        }
    }

    pub fn with_key_value(req: &Request, key: &Bytes, value: Value) -> Self {
        let mut b_mut = BytesMut::new();
        b_mut.put_u32(value.extra);
        Self {
            header: Header {
                magic: Magic::Resp as u8,
                opcode: req.header.opcode,
                key_length: key.len() as u16,
                extras_length: 0x04,
                data_type: 0,
                status: 0,
                body_length: 0x04 + value.data.len() as u32 + key.len() as u32,
                opaque: req.header.opaque,
                cas: value.cas,
            },
            extras: Bytes::from(b_mut),
            key: key.clone(),
            value: value.data,
        }
    }

    pub fn with_cas(req: &Request, cas: u64) -> Self {
        Self {
            header: Header {
                magic: Magic::Resp as u8,
                opcode: req.header.opcode,
                key_length: 0,
                extras_length: 0,
                data_type: 0,
                status: 0,
                body_length: 0,
                opaque: req.header.opaque,
                cas,
            },
            extras: Bytes::new(),
            key: Bytes::new(),
            value: Bytes::new(),
        }
    }

    pub fn is_error(&self) -> bool {
        self.header.status != 0
    }

    pub fn get_action(&self) -> Action {
        match Cmd::try_from(self.header.opcode) {
            Ok(v) => match v {
                Cmd::GetQ | Cmd::GetKQ => {
                    if self.is_error() {
                        Action::Ignore
                    } else {
                        Action::Cache
                    }
                }
                _ => {
                    if v.is_quiet() && !self.is_error() {
                        Action::Ignore
                    } else {
                        Action::Return
                    }
                }
            },
            Err(_) => Action::Return,
        }
    }
}
