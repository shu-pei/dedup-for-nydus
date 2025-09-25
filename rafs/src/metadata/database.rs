use std::io::{BufReader, BufRead, Write};
use std::os::unix::net::UnixStream;
use std::io::Result;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};

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
    // dedupsock: String,
    stream: Arc<Mutex<BufReader<UnixStream>>>,
}

impl DedupClient {
    pub fn new(dedupsock: &String) -> Result<Self> {
        let stream = UnixStream::connect(dedupsock)?;
        Ok(DedupClient {
            // dedupsock: dedupsock.clone(),
            stream: Arc::new(Mutex::new(BufReader::new(stream))),
        })
    }

    pub fn query(&self, key: &String, id: &String, ino: u64) -> Result<QueryResponse> {
        let req = QueryRequest {
            kind: "query".to_string(),
            key: key.clone(),
            id: id.clone(),
            ino
        };
        let req_json = serde_json::to_string(&req)?;
        let mut stream = self.stream.lock().unwrap();

        stream.get_mut().write_all(req_json.as_bytes())?;
        stream.get_mut().write_all(b"\n")?;
        stream.get_mut().flush()?;

        let mut line = String::new();
        stream.read_line(&mut line)?;  
        let resp: QueryResponse = serde_json::from_str(&line)?;
        Ok(resp)
    }

    pub fn getbs(&self, id: &String) -> Result<BootstrapResponse> {
        let req = BootstrapRequest {
            kind: "getbs".to_string(),
            id: id.clone()
        };
        let req_json = serde_json::to_string(&req)?;
        let mut stream = self.stream.lock().unwrap();
        stream.get_mut().write_all(req_json.as_bytes())?;
        stream.get_mut().write_all(b"\n")?;
        stream.get_mut().flush()?;

        let mut line = String::new();
        stream.read_line(&mut line)?;
        let resp: BootstrapResponse = serde_json::from_str(&line)?;
        Ok(resp)
    }
}
