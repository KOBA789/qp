mod btree;
mod buffer;
mod disk;
mod executor;
mod lock;
mod query;
mod slotted;

use std::env;
use std::thread;
use std::{
    io::Write,
    io::{BufRead, BufReader},
    net::TcpListener,
    net::TcpStream,
    sync::Arc,
};

use buffer::{BufferPool, BufferPoolManager};
use disk::DiskManager;
use executor::Executor;

fn main() -> Result<(), anyhow::Error> {
    let mut args = env::args_os();
    args.next();

    let qp_filename = args.next().expect("qp filename is required");
    let disk = DiskManager::open(qp_filename)?;
    let pool = BufferPool::new(5);
    let bufmgr = Arc::new(BufferPoolManager::new(disk, pool));
    let listener = TcpListener::bind("0.0.0.0:8124")?;

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let executor = Executor::new(bufmgr.clone());
        thread::spawn(move || Handler::new(executor).handle(stream));
    }

    Ok(())
}

struct Handler {
    executor: Executor,
}

impl Handler {
    fn new(executor: Executor) -> Self {
        Self { executor }
    }

    fn handle(&self, stream: TcpStream) -> Result<(), anyhow::Error> {
        let buf_read = BufReader::new(&stream);
        for line in buf_read.lines() {
            let line = line?;
            let response = self.handle_request(&line).unwrap_or_else(|err| {
                query::Response::Error(query::Error::Other {
                    message: err.to_string(),
                })
            });
            serde_json::to_writer(&stream, &response)?;
            (&stream).write_all(b"\n")?
        }
        Ok(())
    }

    fn handle_request(&self, line: &str) -> Result<query::Response, anyhow::Error> {
        let request: query::Request = serde_json::from_str(&line)?;
        Ok(self.executor.execute(request))
    }
}
