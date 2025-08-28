use std::io::{BufReader, BufRead, Write};
use std::os::unix::net::UnixStream;
use serde::{Serialize, Deserialize};
use std::sync::{Arc, Mutex};

#[derive(Debug, Serialize, Deserialize)]
struct QueryRequest {
    kind: String,
    key: String,
    id: String,
    ino: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub id: String,
    pub ino: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BootstrapRequest {
    kind: String,
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub bootstrap: String,
}

pub struct DedupClient {
    dedupsock: String,
    stream: Arc<Mutex<UnixStream>>,
}

impl DedupClient {
    pub fn new(dedupsock: &String) -> std::io::Result<Self> {
        let stream = UnixStream::connect(dedupsock)?;
        Ok(DedupClient {
            dedupsock: dedupsock.clone(),
            stream: Arc::new(Mutex::new(stream)),
        })
    }

    pub fn reconnect(&self) -> std::io::Result<()> {
        let new_stream = UnixStream::connect(&self.dedupsock)?;
        *self.stream.lock().unwrap() = new_stream;
        Ok(())
    }

    pub fn query(&self, key: String, id: String, ino: u64) -> std::io::Result<QueryResponse> {
        let req = QueryRequest { kind: "query".to_string(), key, id, ino };
        let req_json = serde_json::to_string(&req)?;
        let mut stream = self.stream.lock().unwrap();
        stream.write_all(req_json.as_bytes())?;
        stream.write_all(b"\n")?;
        stream.flush()?;

        let mut reader = BufReader::new(stream.try_clone()?);
        let mut line = String::new();
        reader.read_line(&mut line)?;  
        let resp: QueryResponse = serde_json::from_str(&line)?;
        debug!("resp is {}, {}\n", resp.id, resp.ino);
        Ok(resp)
    }

    pub fn getbs(&self, id: String) -> std::io::Result<BootstrapResponse> {
        let req = BootstrapRequest { kind: "getbs".to_string(), id };
        let req_json = serde_json::to_string(&req)?;
        let mut stream = self.stream.lock().unwrap();
        stream.write_all(req_json.as_bytes())?;
        stream.write_all(b"\n")?;
        stream.flush()?;

        let mut reader = BufReader::new(stream.try_clone()?);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let resp: BootstrapResponse = serde_json::from_str(&line)?;
        debug!("resp is {}\n", resp.bootstrap);
        Ok(resp)
    }
}
