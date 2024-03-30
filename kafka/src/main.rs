use std::{
    collections::{HashMap, VecDeque},
    sync::mpsc,
};

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<[usize; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

#[derive(Debug)]
struct KafkaNode {
    tx: mpsc::Sender<Message<Payload>>,
    rx: mpsc::Receiver<Message<Payload>>,
    node_id: String,
    msg_id: usize,
    messages: HashMap<String, Vec<usize>>,
    committed_offsets: HashMap<String, usize>,
}

impl Node<Payload> for KafkaNode {
    fn initialize(
        tx: mpsc::Sender<Message<Payload>>,
        rx: mpsc::Receiver<Message<Payload>>,
        node_id: String,
        _other: Vec<String>,
    ) -> Self {
        Self {
            tx,
            rx,
            node_id,
            msg_id: 0,
            messages: HashMap::with_capacity(128),
            committed_offsets: HashMap::with_capacity(8),
        }
    }

    fn run(&mut self) -> Result<()> {
        let mut queue = VecDeque::<Message<Payload>>::with_capacity(16);

        loop {
            if let Some(next) = queue.pop_front() {
                match next.body.payload {
                    Payload::Send { key, msg } => {
                        let offset = match self.messages.get_mut(&key) {
                            Some(send) => {
                                let new_offset = send.len();
                                send.push(msg);

                                new_offset
                            }
                            None => {
                                let mut offsets = Vec::with_capacity(128);
                                offsets.push(msg);
                                self.messages.insert(key.clone(), offsets);
                                self.committed_offsets.insert(key, 0);

                                0
                            }
                        };

                        let ack_msg = self.generate_message(
                            Payload::SendOk { offset },
                            next.src,
                            next.body.msg_id,
                        );
                        self.tx.send(ack_msg)?;
                    }
                    Payload::Poll { offsets } => {
                        let msgs = offsets
                            .into_iter()
                            .map(|(key, offset)| {
                                let slice = self
                                    .messages
                                    .get(&key)
                                    .unwrap_or(&vec![])
                                    .iter()
                                    .enumerate()
                                    .skip(offset)
                                    .take(3)
                                    .map(|(i, v)| [i, *v])
                                    .collect::<Vec<_>>();

                                (key, slice)
                            })
                            .collect::<HashMap<_, _>>();

                        let ack_msg = self.generate_message(
                            Payload::PollOk { msgs },
                            next.src,
                            next.body.msg_id,
                        );
                        self.tx.send(ack_msg)?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        for (key, offset) in offsets {
                            self.committed_offsets.insert(key, offset);
                        }

                        let ack_msg = self.generate_message(
                            Payload::CommitOffsetsOk,
                            next.src,
                            next.body.msg_id,
                        );
                        self.tx.send(ack_msg)?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let offsets = keys
                            .into_iter()
                            .map(|key| {
                                let offset = *self.committed_offsets.get(&key).unwrap_or(&0);
                                (key, offset)
                            })
                            .collect::<HashMap<_, _>>();

                        let ack_msg = self.generate_message(
                            Payload::ListCommittedOffsetsOk { offsets },
                            next.src,
                            next.body.msg_id,
                        );
                        self.tx.send(ack_msg)?;
                    }
                    _ => bail!("Unexpected message reached for node: {next:?}"),
                }
            } else if let Ok(next) = self.rx.recv() {
                queue.push_back(next);
            } else {
                break;
            }
        }

        Ok(())
    }
}

impl KafkaNode {
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
    dist_sys::run_dist_sys::<KafkaNode, Payload>()?;
    Ok(())
}
