use std::sync::mpsc;

use anyhow::Result;
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum GeneratePayload {
    Generate,
    GenerateOk { id: String },
}

#[derive(Debug)]
struct UniqueIdNode {
    tx: mpsc::Sender<Message<GeneratePayload>>,
    rx: mpsc::Receiver<Message<GeneratePayload>>,
    node_id: String,
    msg_id: usize,
}

impl Node<GeneratePayload> for UniqueIdNode {
    fn initialize(
        tx: mpsc::Sender<Message<GeneratePayload>>,
        rx: mpsc::Receiver<Message<GeneratePayload>>,
        node_id: String,
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
                    payload: GeneratePayload::GenerateOk {
                        id: format!("{}-{}", self.node_id, self.msg_id),
                    },
                },
            })?;
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    dist_sys::run_dist_sys::<UniqueIdNode, GeneratePayload>()?;
    Ok(())
}
