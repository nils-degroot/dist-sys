use std::sync::mpsc;

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Debug)]
struct EchoNode {
    tx: mpsc::Sender<Message<EchoPayload>>,
    rx: mpsc::Receiver<Message<EchoPayload>>,
    node_id: String,
    msg_id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn initialize(
        tx: mpsc::Sender<Message<EchoPayload>>,
        rx: mpsc::Receiver<Message<EchoPayload>>,
        node_id: String,
        _other: Vec<String>,
    ) -> Self {
        Self {
            tx,
            rx,
            node_id,
            msg_id: 0,
        }
    }

    fn run(&mut self) -> Result<()> {
        while let Ok(msg) = self.rx.recv() {
            let echo = match msg.body.payload {
                EchoPayload::Echo { echo } => echo,
                EchoPayload::EchoOk { .. } => bail!("Invalid message for node"),
            };

            self.tx.send(Message {
                src: self.node_id.clone(),
                dest: msg.src,
                body: Body {
                    msg_id: {
                        let old = self.msg_id;
                        self.msg_id += 1;
                        Some(old)
                    },
                    in_reply_to: msg.body.msg_id,
                    payload: EchoPayload::EchoOk { echo },
                },
            })?;
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    dist_sys::run_dist_sys::<EchoNode, EchoPayload>()?;
    Ok(())
}
