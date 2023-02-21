use std::num::Wrapping;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::atomic::AtomicU64};

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    pub data: Bytes,
    pub cas: u64,
    pub extra: u32,
    pub expired_at: Option<Instant>,
    pub stored_at: u64,
}

impl Value {
    fn is_expired(&self, flush: &AtomicU64) -> bool {
        let flush = flush.load(Ordering::Relaxed);
        if self.stored_at <= flush {
            return true;
        }
        if let Some(v) = &self.expired_at {
            *v <= Instant::now()
        } else {
            false
        }
    }
}

pub struct Store {
    s: RwLock<HashMap<Bytes, Value>>,
    flush: AtomicU64,
}

#[derive(Debug, PartialEq)]
pub enum StoreError {
    CasNotMatch,
    ItemExists,
    ItemNotExists,
    NotU64,
}

fn new_cas() -> u64 {
    rand::random()
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

impl Store {
    pub fn new() -> Self {
        Self {
            s: RwLock::new(HashMap::new()),
            flush: AtomicU64::new(0),
        }
    }

    pub fn g<'a>(
        s: &'a HashMap<Bytes, Value>,
        key: &Bytes,
        flush: &AtomicU64,
    ) -> Option<&'a Value> {
        s.get(key)
            .and_then(|v| if v.is_expired(flush) { None } else { Some(v) })
    }

    pub fn get(&self, key: &Bytes) -> Option<Value> {
        let s = self.s.read().unwrap();
        Self::g(&s, key, &self.flush).map(|v| v.clone())
    }

    fn check_cas(
        s: &HashMap<Bytes, Value>,
        key: &Bytes,
        cas: u64,
        flush: &AtomicU64,
    ) -> Result<(), StoreError> {
        if cas == 0 {
            Ok(())
        } else {
            let cur_ver = Self::g(s, key, flush).map(|v| v.cas).unwrap_or_default();
            if cas != cur_ver {
                Err(StoreError::CasNotMatch)
            } else {
                Ok(())
            }
        }
    }

    pub fn set(&self, key: &Bytes, mut value: Value) -> Result<u64, StoreError> {
        let mut s = self.s.write().unwrap();

        Self::check_cas(&s, key, value.cas, &self.flush)?;
        let cas = new_cas();
        value.cas = cas;
        s.insert(key.clone(), value);

        Ok(cas)
    }

    pub fn add(&self, key: &Bytes, mut value: Value) -> Result<u64, StoreError> {
        let mut s = self.s.write().unwrap();

        if value.cas != 0 {
            return Err(StoreError::CasNotMatch);
        }

        match s.get(key) {
            Some(v) if !v.is_expired(&self.flush) => return Err(StoreError::ItemExists),
            _ => {}
        }
        let cas = new_cas();
        value.cas = cas;
        s.insert(key.clone(), value);

        Ok(cas)
    }

    pub fn replace(&self, key: &Bytes, mut value: Value) -> Result<u64, StoreError> {
        let mut s = self.s.write().unwrap();

        match Self::g(&s, key, &self.flush) {
            None => return Err(StoreError::ItemNotExists),
            Some(v) => {
                if value.cas != 0 && v.cas != value.cas {
                    return Err(StoreError::CasNotMatch);
                }
            }
        }
        let cas = new_cas();
        value.cas = cas;
        s.insert(key.clone(), value);

        Ok(cas)
    }

    pub fn delete(&self, key: &Bytes, cas: u64) -> Result<(), StoreError> {
        let mut s = self.s.write().unwrap();

        Self::check_cas(&s, key, cas, &self.flush)?;

        match s.remove(key) {
            None => Err(StoreError::ItemNotExists),
            Some(_) => Ok(()),
        }
    }

    pub fn delta(
        &self,
        key: &Bytes,
        delta: u64,
        is_add: bool,
        init_value: u64,
        expired_at: Option<Instant>,
    ) -> Result<(u64, u64), StoreError> {
        let mut s = self.s.write().unwrap();

        let mut b = BytesMut::with_capacity(8);

        let cas = new_cas();
        let (val, upd) = match Self::g(&s, key, &self.flush) {
            None => {
                b.put_u64(init_value);
                Ok((
                    Value {
                        data: Bytes::from(b),
                        cas,
                        extra: 0,
                        expired_at,
                        stored_at: unix_timestamp(),
                    },
                    init_value,
                ))
            }
            Some(v) => {
                let mut cur_data = v.data.clone();
                if cur_data.len() != 8 {
                    return Err(StoreError::NotU64);
                }
                let mut data = Wrapping(cur_data.get_u64());
                let delta = Wrapping(delta);
                if is_add {
                    data = data + delta;
                } else {
                    if delta > data {
                        data = Wrapping(0);
                    } else {
                        data = data - delta;
                    }
                }
                b.put_u64(data.0);
                Ok((
                    Value {
                        data: Bytes::from(b),
                        cas,
                        extra: 0,
                        expired_at: v.expired_at.clone(),
                        stored_at: v.stored_at,
                    },
                    data.0,
                ))
            }
        }?;

        s.insert(key.clone(), val);
        Ok((upd, cas))
    }

    pub fn flush(&self, delay: u64) {
        let f = unix_timestamp() + delay * 1000 * 1000;
        self.flush.store(f, Ordering::Relaxed)
    }

    pub fn append(&self, key: &Bytes, value: &Bytes, is_append: bool) -> Result<u64, StoreError> {
        let mut s = self.s.write().unwrap();

        let cas = new_cas();
        let val_upd = match Self::g(&s, key, &self.flush) {
            Some(v) => {
                let mut byte_mut = BytesMut::with_capacity(v.data.len() + value.len());
                if is_append {
                    byte_mut.extend_from_slice(&v.data);
                    byte_mut.extend_from_slice(value);
                } else {
                    byte_mut.extend_from_slice(value);
                    byte_mut.extend_from_slice(&v.data);
                }

                Ok(Value {
                    data: Bytes::from(byte_mut),
                    cas,
                    extra: v.extra,
                    expired_at: v.expired_at,
                    stored_at: v.stored_at,
                })
            }
            None => Err(StoreError::ItemNotExists),
        }?;

        s.insert(key.clone(), val_upd);

        Ok(cas)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn test_set_get() {
        let s = Store::new();
        let key = Bytes::from("test_key");

        assert_eq!(None, s.get(&key));

        let value_data = Bytes::from("test_value");
        s.set(
            &key,
            Value {
                data: value_data.clone(),
                cas: 0,
                extra: 12345,
                expired_at: None,
                stored_at: unix_timestamp(),
            },
        )
        .unwrap();

        let ret = s.get(&key).unwrap();
        assert_eq!(value_data, ret.data);
        assert_ne!(0, ret.cas);
        assert_eq!(12345, ret.extra);
        assert_eq!(None, ret.expired_at);

        // get again
        let ret = s.get(&key).unwrap();
        assert_eq!(value_data, ret.data);
        assert_ne!(0, ret.cas);
        assert_eq!(12345, ret.extra);
        assert_eq!(None, ret.expired_at);

        s.set(
            &key,
            Value {
                data: value_data.clone(),
                cas: 0,
                extra: 12345,
                expired_at: None,
                stored_at: unix_timestamp(),
            },
        )
        .unwrap();

        // set with expire
        let expired_at = Instant::now() + Duration::from_secs(1);
        s.set(
            &key,
            Value {
                data: value_data.clone(),
                cas: 0,
                extra: 12345,
                expired_at: Some(expired_at),
                stored_at: unix_timestamp(),
            },
        )
        .unwrap();
        let ret = s.get(&key).unwrap();
        assert_eq!(Some(expired_at), ret.expired_at);

        // expired
        thread::sleep(Duration::from_secs(1));
        assert_eq!(None, s.get(&key));
    }

    #[test]
    fn test_set_cas() {
        let s = Store::new();

        let key = Bytes::from("test_key");
        let value_data = Bytes::from("test_value");
        let value = Value {
            data: value_data.clone(),
            cas: 0,
            extra: 12345,
            expired_at: None,
            stored_at: unix_timestamp(),
        };

        let cas = s.set(&key, value.clone()).unwrap();

        let mut value_with_cas = value.clone();
        value_with_cas.cas = cas - 1;
        let err = s.set(&key, value_with_cas.clone()).unwrap_err();
        assert_eq!(StoreError::CasNotMatch, err);

        value_with_cas.cas = cas;
        s.set(&key, value_with_cas.clone()).unwrap();
    }

    #[test]
    fn test_add() {
        let s = Store::new();

        let key = Bytes::from("test_key");
        let value_data = Bytes::from("test_value");
        let value = Value {
            data: value_data.clone(),
            cas: 0,
            extra: 12345,
            expired_at: None,
            stored_at: unix_timestamp(),
        };

        assert_ne!(0, s.add(&key, value.clone()).unwrap());
        assert_eq!(
            StoreError::ItemExists,
            s.add(&key, value.clone()).unwrap_err()
        );
    }

    #[test]
    fn test_replace() {
        let s = Store::new();

        let key = Bytes::from("test_key");
        let value_data = Bytes::from("test_value");
        let value = Value {
            data: value_data.clone(),
            cas: 0,
            extra: 12345,
            expired_at: None,
            stored_at: unix_timestamp(),
        };

        assert_eq!(
            StoreError::ItemNotExists,
            s.replace(&key, value.clone()).unwrap_err()
        );

        s.add(&key, value.clone()).unwrap();
        s.replace(&key, value.clone()).unwrap();
    }

    #[test]
    fn test_delete() {
        let s = Store::new();

        let key = Bytes::from("test_key");
        let value_data = Bytes::from("test_value");
        let value = Value {
            data: value_data.clone(),
            cas: 0,
            extra: 12345,
            expired_at: None,
            stored_at: unix_timestamp(),
        };

        s.set(&key, value.clone()).unwrap();
        s.get(&key).unwrap();

        s.delete(&key, 0).unwrap();
        assert!(s.get(&key).is_none());

        assert_eq!(StoreError::ItemNotExists, s.delete(&key, 0).unwrap_err());
    }

    #[test]
    fn test_incr_and_decr() {
        let s = Store::new();
        let key = Bytes::from("test_key");

        // use init value
        let (val, _cas) = s.delta(&key, 3, true, 0, None).unwrap();
        assert_eq!(0, val);

        // add value
        let (val, _cas) = s.delta(&key, 3, true, 0, None).unwrap();
        assert_eq!(3, val);

        // sub value
        let (val, _cas) = s.delta(&key, 1, false, 0, None).unwrap();
        assert_eq!(2, val);

        let value_data = Bytes::from("test_value");
        let value = Value {
            data: value_data.clone(),
            cas: 0,
            extra: 12345,
            expired_at: None,
            stored_at: unix_timestamp(),
        };
        // set nan value
        s.set(&key, value).unwrap();

        assert_eq!(
            StoreError::NotU64,
            s.delta(&key, 3, true, 0, None).unwrap_err()
        );
        assert_eq!(
            StoreError::NotU64,
            s.delta(&key, 3, false, 0, None).unwrap_err()
        );
    }
}
