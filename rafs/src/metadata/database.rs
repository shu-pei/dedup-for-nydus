use anyhow::Result;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct KeyValueClient {
    stream: Arc<Mutex<UnixStream>>,
}

impl KeyValueClient {
    pub fn new<P: AsRef<Path>>(socket_path: P) -> Result<Self> {
        let stream = UnixStream::connect(socket_path)?;
        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
        })
    }

    fn send_command(&self, cmd: &str) -> std::io::Result<String> {
        let mut stream = self.stream.lock().unwrap();
        stream.write_all(cmd.as_bytes())?;
        stream.write_all(b"\n")?;
        stream.flush()?;

        let mut reader = BufReader::new(&*stream);
        let mut response = String::new();
        reader.read_line(&mut response)?;
        Ok(response.trim().to_string())
    }

    // GET key -> Option<(u64,u64)>
    pub fn query(&self, key: &String, v1: u64, v2: u64) -> std::io::Result<(u64, u64)> {
        let cmd_get = format!("GET {}", key);
        let resp = self.send_command(&cmd_get)?;
        match resp.as_str() {
            "NOTFOUND" => {
                let cmd_put = format!("PUT {} {} {}", key, v1, v2);
                let resp_put = self.send_command(&cmd_put)?;
                if resp_put == "OK" {
                    Ok((v1, v2))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("server error: {}", resp_put),
                    ))
                }
            }
            s if s.starts_with("OK ") => {
                let parts: Vec<&str> = s[3..].split_whitespace().collect();
                if parts.len() == 2 {
                    let val1 = parts[0].parse::<u64>().map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, e)
                    })?;
                    let val2 = parts[1].parse::<u64>().map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, e)
                    })?;
                    Ok((val1, val2))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "invalid response format",
                    ))
                }
            }
            s => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("server error: {}", s),
            )),
        }
    }

    // GET_ID -> u64
    pub fn get_id(&self) -> std::io::Result<u64> {
        let resp = self.send_command("GET_ID")?;
        if resp.starts_with("OK ") {
            let id = resp[3..].parse::<u64>().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, e)
            })?;
            Ok(id)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("server error: {}", resp),
            ))
        }
    }
}
