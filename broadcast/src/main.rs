use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::mpsc,
    time::Instant,
};

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    neighbors: HashSet<String>,
}

impl Node<Payload> for BroadcastNode {
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
            values: HashSet::with_capacity(512),
            neighbors: HashSet::new(),
        }
    }

    fn run(&mut self) -> Result<()> {
        let mut queue = VecDeque::<Message<Payload>>::with_capacity(16);
        let mut unfinished = VecDeque::<(Instant, Message<Payload>)>::with_capacity(16);

        loop {
            while let Some((time, msg)) = unfinished.pop_front() {
                if time.elapsed().as_millis() < 300 {
                    unfinished.push_front((time, msg));
                    break;
                }

                self.tx.send(msg.clone())?;
                unfinished.push_back((Instant::now(), msg));
            }

            if let Some(msg) = queue.pop_front() {
                match msg.body.payload {
                    Payload::Broadcast { message } => {
                        self.send_response(&msg, Payload::BroadcastOk)?;

                        if self.values.contains(&message) {
                            continue;
                        }

                        self.values.insert(message);

                        for neighbor in self.neighbors.clone() {
                            if neighbor == msg.src {
                                continue;
                            }

                            let msg = self.generate_message(
                                Payload::Broadcast { message },
                                neighbor.to_string(),
                                None,
                            );

                            self.tx.send(msg.clone())?;
                            unfinished.push_back((Instant::now(), msg))
                        }
                    }
                    Payload::Read => {
                        self.send_response(
                            &msg,
                            Payload::ReadOk {
                                messages: self.values.clone(),
                            },
                        )?;
                    }
                    Payload::Topology { ref topology } => {
                        self.neighbors = topology[&self.node_id].clone();
                        self.send_response(&msg, Payload::TopologyOk)?;
                    }
                    Payload::BroadcastOk => {
                        let msg_id = msg.body.in_reply_to;
                        let index = unfinished
                            .iter()
                            .position(|(_, msg)| msg_id == msg.body.msg_id);

                        if let Some(index) = index {
                            unfinished.remove(index);
                        }
                    }
                    m => bail!("Message invalid for node: {m:?}"),
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

impl BroadcastNode {
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

    fn send_response(&mut self, msg: &Message<Payload>, payload: Payload) -> Result<()> {
        let ack_msg = self.generate_message(payload, msg.src.clone(), msg.body.msg_id);
        self.tx.send(ack_msg)?;

        Ok(())
    }
}

fn main() -> Result<()> {
    dist_sys::run_dist_sys::<BroadcastNode, Payload>()?;
    Ok(())
}
