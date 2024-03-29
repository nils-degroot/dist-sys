use std::{
    collections::{HashSet, VecDeque},
    sync::mpsc,
};

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },
    #[serde[rename = "read_int"]]
    KvReadInt {
        key: String,
    },
    #[serde[rename = "read_int_ok"]]
    KvReadIntOk {
        value: usize,
    },
    #[serde[rename = "write"]]
    KvWrite {
        key: String,
        value: usize,
    },
    #[serde[rename = "write_ok"]]
    KvWriteOk,
    CompareAndSwap {
        key: String,
        from: usize,
        to: usize,
    },
    CompareAndSwapOk {
        key: String,
        from: usize,
        to: usize,
    },
}

#[derive(Debug)]
struct GCounterNode {
    tx: mpsc::Sender<Message<Payload>>,
    rx: mpsc::Receiver<Message<Payload>>,
    node_id: String,
    msg_id: usize,
    current_value: usize,
}

impl Node<Payload> for GCounterNode {
    fn initialize(
        tx: mpsc::Sender<Message<Payload>>,
        rx: mpsc::Receiver<Message<Payload>>,
        node_id: String,
    ) -> Self {
        Self {
            tx,
            rx,
            node_id,
            msg_id: 0,
            current_value: 0,
        }
    }

    fn run(&mut self) -> Result<()> {
        let mut queue = VecDeque::<Message<Payload>>::with_capacity(16);

        loop {
            if let Some(msg) = queue.pop_front() {
                match msg.body.payload {
                    Payload::Add { delta } => todo!(),
                    Payload::Read => {
                        let read_msg = self.generate_message(
                            Payload::KvReadInt {
                                key: "v".to_string(),
                            },
                            "seq-k".to_string(),
                            None,
                        );
                        self.tx.send(read_msg.clone())?;

                        let value = loop {
                            let next = self.rx.recv()?;

                            if next.body.in_reply_to == read_msg.body.msg_id {
                                match next.body.payload {
                                    Payload::KvReadIntOk { value } => {
                                        break value;
                                    }
                                    _ => queue.push_back(next),
                                }
                            } else {
                                queue.push_back(next)
                            }
                        };

                        let out_msg = self.generate_message(
                            Payload::ReadOk { value },
                            msg.src,
                            msg.body.msg_id,
                        );
                        self.tx.send(out_msg)?;
                    }
                    m => bail!("Invalid message for client: {m:?}"),
                }
            } else if let Ok(msg) = self.rx.recv() {
                queue.push_back(msg);
            } else {
                break;
            }
        }

        Ok(())
    }
}

impl GCounterNode {
    fn get_and_increment_id(&mut self) -> usize {
        let old = self.msg_id;
        self.msg_id += 1;
        old
    }

    fn generate_message(
        &mut self,
        payload: Payload,
        dest: String,
        in_reply_to: Option<usize>,
    ) -> Message<Payload> {
        Message {
            src: self.node_id.clone(),
            dest,
            body: Body {
                msg_id: Some(self.get_and_increment_id()),
                in_reply_to,
                payload,
            },
        }
    }
}

fn main() -> Result<()> {
    dist_sys::run_dist_sys::<GCounterNode, Payload>()?;
    Ok(())
}
