use std::{
    io::{self, BufRead, StdoutLock, Write},
    sync::mpsc,
    thread,
};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message<S> {
    pub src: String,
    pub dest: String,
    pub body: Body<S>,
}

impl<S: Serialize> Message<S> {
    pub fn write(&self, stdout: &mut StdoutLock) -> Result<()> {
        let msg = serde_json::to_string(self)?;
        stdout.write_all(msg.as_bytes())?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Body<S> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: S,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

pub trait Node<I>
where
    I: for<'a> Deserialize<'a> + Serialize,
{
    fn initialize(
        tx: mpsc::Sender<Message<I>>,
        rx: mpsc::Receiver<Message<I>>,
        node_id: String,
        other: Vec<String>,
    ) -> Self;

    fn run(&mut self) -> Result<()>;
}

pub fn run_dist_sys<N, I>() -> Result<()>
where
    N: Node<I>,
    I: for<'a> Deserialize<'a> + Serialize + Send + Sync + 'static,
{
    let init = {
        let mut stdin = io::stdin().lock();
        let mut buf = String::new();

        stdin.read_line(&mut buf)?;
        serde_json::from_str::<Message<InitPayload>>(&buf)?
    };

    let (node_id, other) = match init.body.payload {
        InitPayload::Init { node_id, node_ids } => (node_id, node_ids),
        _ => bail!("No initialization send"),
    };

    {
        let mut stdout = io::stdout().lock();

        Message {
            src: init.dest,
            dest: init.src,
            body: Body {
                msg_id: Some(usize::MAX),
                in_reply_to: init.body.msg_id,
                payload: InitPayload::InitOk,
            },
        }
        .write(&mut stdout)?;
    };

    let (node_tx, node_rx) = mpsc::channel::<Message<I>>();
    let (handler_tx, handler_rx) = mpsc::channel::<Message<I>>();

    thread::spawn(move || {
        let stdin = io::stdin().lock();

        for line in stdin.lines() {
            let next = serde_json::from_str::<Message<I>>(&line?)?;
            node_tx.send(next)?;
        }

        anyhow::Ok(())
    });

    thread::spawn(move || {
        let mut stdout = io::stdout().lock();

        while let Ok(msg) = handler_rx.recv() {
            msg.write(&mut stdout)?;
        }

        anyhow::Ok(())
    });

    N::initialize(handler_tx, node_rx, node_id, other).run()?;

    Ok(())
}
