use std::{
    collections::{HashMap, HashSet},
    sync::mpsc,
};

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
}

#[derive(Debug)]
struct BroadcastNode {
    tx: mpsc::Sender<Message<Payload>>,
    rx: mpsc::Receiver<Message<Payload>>,
    node_id: String,
    msg_id: usize,
    values: HashSet<usize>,
    topology: HashMap<String, HashSet<String>>,
}

impl Node<Payload> for BroadcastNode {
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
            values: HashSet::with_capacity(512),
            topology: HashMap::with_capacity(16),
        }
    }

    fn run(&mut self) -> Result<()> {
        while let Ok(msg) = self.rx.recv() {
            let mut out = Message {
                src: self.node_id.clone(),
                dest: msg.src.clone(),
                body: Body {
                    msg_id: {
                        let old = self.msg_id;
                        self.msg_id += 1;
                        Some(old)
                    },
                    in_reply_to: msg.body.msg_id,
                    payload: Payload::TopologyOk,
                },
            };

            match msg.body.payload {
                Payload::Broadcast { message } => {
                    if !self.values.contains(&message) {
                        for child in self.topology.get(&self.node_id).unwrap_or(&HashSet::new()) {
                            self.tx.send(Message {
                                src: self.node_id.clone(),
                                dest: child.clone(),
                                body: Body {
                                    msg_id: {
                                        let old = self.msg_id;
                                        self.msg_id += 1;
                                        Some(old)
                                    },
                                    in_reply_to: None,
                                    payload: Payload::Broadcast { message },
                                },
                            })?;
                        }
                    }

                    self.values.insert(message);
                    out.body.payload = Payload::BroadcastOk;

                    self.tx.send(out)?;
                }
                Payload::Read => {
                    out.body.payload = Payload::ReadOk {
                        messages: self.values.clone(),
                    };

                    self.tx.send(out)?;
                }
                Payload::Topology { topology } => {
                    self.topology = topology;
                    out.body.payload = Payload::TopologyOk;

                    self.tx.send(out)?;
                }
                Payload::BroadcastOk => {}
                _ => bail!("Message invalid for node"),
            }
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    dist_sys::run_dist_sys::<BroadcastNode, Payload>()?;
    Ok(())
}
