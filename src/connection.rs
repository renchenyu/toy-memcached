use std::collections::VecDeque;
use std::io::Cursor;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{io, vec};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::frame::{self, response_status, Cmd, Request, Response, Status};
use crate::store::{Store, StoreError};

const VERSION: &str = "0.0.1";

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
    store: Arc<Store>,
    quiet_responses: VecDeque<Response>,
}

impl Connection {
    pub fn new(stream: TcpStream, store: Arc<Store>) -> Self {
        let stream = BufWriter::new(stream);
        let buf = BytesMut::with_capacity(4096);
        Self {
            stream,
            buf,
            store,
            quiet_responses: VecDeque::new(),
        }
    }

    pub async fn process(&mut self) -> io::Result<()> {
        'outer: loop {
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if !self.buf.is_empty() {
                    eprintln!("connect reset by peer");
                }
                break;
            }

            loop {
                match Request::check_parse(&self.buf[..]) {
                    Ok(_) => {
                        let mut cursor = Cursor::new(&self.buf[..]);
                        let request = Request::parse(&mut cursor);
                        self.buf.advance(cursor.position() as usize);

                        // println!("request {:?}\n", request);

                        let resps = match request.check_valid() {
                            Ok(_) => self.execute(&request),
                            Err(frame::FormatError::InvalidArguments) => vec![Response::error(
                                &request,
                                Status::InvalidArguments as u16,
                                Bytes::from(response_status::MSG_INVALID_ARGUMENTS),
                            )],
                            Err(frame::FormatError::InvalidOpcode) => vec![Response::error(
                                &request,
                                Status::UnknownCommand as u16,
                                Bytes::from(response_status::MSG_UNKNOWN_COMMAND),
                            )],
                        };

                        for resp in resps {
                            match resp.get_action() {
                                frame::Action::Return => {
                                    while let Some(quite_resp) = self.quiet_responses.pop_front() {
                                        self.write_response(&quite_resp).await?;
                                    }
                                    self.write_response(&resp).await?;
                                    self.stream.flush().await?;
                                }
                                frame::Action::Cache => {
                                    self.quiet_responses.push_back(resp);
                                }
                                frame::Action::Ignore => {
                                    // do nothing
                                }
                            };

                            if request.header.opcode == Cmd::Quit as u8
                                || request.header.opcode == Cmd::QuitQ as u8
                            {
                                break 'outer;
                            }
                        }
                    }
                    Err(frame::PacketError::InComplete) => {
                        // to read more data into buf
                        break;
                    }
                    Err(frame::PacketError::Invalid) => {
                        eprintln!("package invalid");
                        break 'outer;
                    }
                }
            }
        }

        Ok(())
    }

    async fn write_response(&mut self, response: &Response) -> io::Result<()> {
        // println!("write response {:?}\n", response);

        let header = &response.header;
        self.stream.write_u8(header.magic as u8).await?;
        self.stream.write_u8(header.opcode as u8).await?;
        self.stream.write_u16(header.key_length).await?;
        self.stream.write_u8(header.extras_length).await?;
        self.stream.write_u8(header.data_type).await?;
        self.stream.write_u16(header.status).await?;
        self.stream.write_u32(header.body_length).await?;
        self.stream.write_u32(header.opaque).await?;
        self.stream.write_u64(header.cas).await?;
        if !response.extras.is_empty() {
            self.stream.write_all(&response.extras[..]).await?;
        }
        if !response.key.is_empty() {
            self.stream.write_all(&response.key[..]).await?;
        }
        if !response.value.is_empty() {
            self.stream.write_all(&response.value[..]).await?;
        }
        Ok(())
    }

    fn execute(&self, req: &Request) -> Vec<Response> {
        let cmd = Cmd::try_from(req.header.opcode).unwrap();

        match cmd {
            Cmd::Get | Cmd::GetQ => vec![self.execute_get(req)],
            Cmd::GetK | Cmd::GetKQ => vec![self.execute_get_k(req)],
            Cmd::Set | Cmd::SetQ => vec![self.execute_set(req)],
            Cmd::Add | Cmd::AddQ => vec![self.execute_add(req)],
            Cmd::Replace | Cmd::ReplaceQ => vec![self.execute_replace(req)],
            Cmd::Delete | Cmd::DeleteQ => vec![self.execute_delete(req)],
            Cmd::Incr | Cmd::IncrQ => vec![self.execute_delta(req, true)],
            Cmd::Decr | Cmd::DecrQ => vec![self.execute_delta(req, false)],
            Cmd::Quit | Cmd::QuitQ => vec![self.execute_quit(req)],
            Cmd::Flush | Cmd::FlushQ => vec![self.execute_flush(req)],
            Cmd::NoOp => vec![self.execute_noop(req)],
            Cmd::Version => vec![self.execute_version(req)],
            Cmd::Append | Cmd::AppendQ => vec![self.execute_append(req, true)],
            Cmd::Prepend | Cmd::PrependQ => vec![self.execute_append(req, false)],
            Cmd::Stat => self.execute_stat(req),
        }
    }

    fn execute_get(&self, req: &Request) -> Response {
        match self.store.get(&req.key) {
            None => Response::error(
                req,
                Status::KeyNotFound as u16,
                Bytes::from(response_status::MSG_KEY_NOT_FOUND),
            ),
            Some(v) => Response::with_key_value(req, &Bytes::new(), v),
        }
    }

    fn execute_get_k(&self, req: &Request) -> Response {
        match self.store.get(&req.key) {
            None => Response::error(
                req,
                Status::KeyNotFound as u16,
                Bytes::from(response_status::MSG_KEY_NOT_FOUND),
            ),
            Some(v) => Response::with_key_value(req, &req.key, v),
        }
    }

    fn execute_set(&self, req: &Request) -> Response {
        let value = req.get_value();
        match self.store.set(&req.key, value) {
            Ok(cas) => Response::with_cas(req, cas),
            Err(StoreError::CasNotMatch) => Response::error(
                req,
                Status::KeyExists as u16,
                Bytes::from(response_status::MSG_KEY_EXISTS),
            ),
            Err(e) => panic!("unexpected err: {:?}", e),
        }
    }

    fn execute_add(&self, req: &Request) -> Response {
        let value = req.get_value();
        match self.store.add(&req.key, value) {
            Ok(cas) => Response::with_cas(req, cas),
            Err(StoreError::ItemExists) => Response::error(
                req,
                Status::KeyExists as u16,
                Bytes::from(response_status::MSG_KEY_EXISTS),
            ),
            Err(e) => panic!("unexpected err: {:?}", e),
        }
    }

    fn execute_replace(&self, req: &Request) -> Response {
        let value = req.get_value();
        match self.store.replace(&req.key, value) {
            Ok(cas) => Response::with_cas(req, cas),
            Err(StoreError::CasNotMatch) => Response::error(
                req,
                Status::KeyExists as u16,
                Bytes::from(response_status::MSG_KEY_EXISTS),
            ),
            Err(StoreError::ItemNotExists) => Response::error(
                req,
                Status::KeyNotFound as u16,
                Bytes::from(response_status::MSG_KEY_NOT_FOUND),
            ),
            Err(e) => panic!("unexpected err: {:?}", e),
        }
    }

    fn execute_delete(&self, req: &Request) -> Response {
        match self.store.delete(&req.key, req.header.cas) {
            Ok(_) => Response::success(req, Bytes::new(), Bytes::new(), Bytes::new(), 0),
            Err(StoreError::CasNotMatch) | Err(StoreError::ItemNotExists) => Response::error(
                req,
                Status::KeyNotFound as u16,
                Bytes::from(response_status::MSG_KEY_NOT_FOUND),
            ),
            Err(e) => panic!("unexpected err: {:?}", e),
        }
    }

    fn execute_delta(&self, req: &Request, is_add: bool) -> Response {
        let mut extras = req.extras.clone();

        let delta = extras.get_u64();
        let init = extras.get_u64();
        let expire = extras.get_u32();

        if expire == u32::MAX {
            return Response::error(
                req,
                Status::KeyNotFound as u16,
                Bytes::from(response_status::MSG_KEY_NOT_FOUND),
            );
        }

        let mut expired_at = None;
        if expire != 0 {
            expired_at = Some(Instant::now().add(Duration::from_secs(expire as u64)));
        }

        let mut buf = BytesMut::new();
        match self.store.delta(&req.key, delta, is_add, init, expired_at) {
            Ok((v, cas)) => {
                buf.put_u64(v);
                Response::success(req, Bytes::new(), Bytes::new(), Bytes::from(buf), cas)
            }
            Err(StoreError::NotU64) => {
                let msg;
                if is_add {
                    msg = response_status::MSG_INCR_ON_NON_NUMERIC_VALUE;
                } else {
                    msg = response_status::MSG_DECR_ON_NON_NUMERIC_VALUE;
                }
                Response::error(req, Status::OpOnNan as u16, Bytes::from(msg))
            }
            Err(e) => panic!("unexpected err: {:?}", e),
        }
    }

    fn execute_quit(&self, req: &Request) -> Response {
        Response::success(req, Bytes::new(), Bytes::new(), Bytes::new(), 0)
    }

    fn execute_flush(&self, req: &Request) -> Response {
        let mut delay = 0u64;
        if !req.extras.is_empty() {
            delay = req.extras.clone().get_u32() as u64;
        }
        self.store.flush(delay);
        Response::success(req, Bytes::new(), Bytes::new(), Bytes::new(), 0)
    }

    fn execute_noop(&self, req: &Request) -> Response {
        Response::success(req, Bytes::new(), Bytes::new(), Bytes::new(), 0)
    }

    fn execute_version(&self, req: &Request) -> Response {
        Response::success(req, Bytes::new(), Bytes::new(), Bytes::from(VERSION), 0)
    }

    fn execute_append(&self, req: &Request, is_append: bool) -> Response {
        match self.store.append(&req.key, &req.value.clone(), is_append) {
            Ok(cas) => Response::success(req, Bytes::new(), Bytes::new(), Bytes::new(), cas),
            Err(StoreError::ItemNotExists) => Response::error(
                req,
                Status::KeyNotFound as u16,
                Bytes::from(response_status::MSG_KEY_NOT_FOUND),
            ),
            Err(e) => panic!("unexpected err: {:?}", e),
        }
    }

    fn execute_stat(&self, req: &Request) -> Vec<Response> {
        let mut ret = vec![];

        ret.push(Response::success(
            req,
            Bytes::new(),
            Bytes::from("stat_key1"),
            Bytes::from("stat_val1"),
            0,
        ));
        ret.push(Response::success(
            req,
            Bytes::new(),
            Bytes::from("stat_key2"),
            Bytes::from("stat_val2"),
            0,
        ));
        ret.push(Response::success(
            req,
            Bytes::new(),
            Bytes::new(),
            Bytes::new(),
            0,
        ));

        ret
    }
}
