use std::{
    collections::{HashMap, VecDeque},
    sync::mpsc,
    time::Duration,
};

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Replicate { value: usize },
}

#[derive(Debug)]
struct GCounterNode {
    tx: mpsc::Sender<Message<Payload>>,
    rx: mpsc::Receiver<Message<Payload>>,
    node_id: String,
    msg_id: usize,
    nodes: Vec<String>,
}

impl Node<Payload> for GCounterNode {
    fn initialize(
        tx: mpsc::Sender<Message<Payload>>,
        rx: mpsc::Receiver<Message<Payload>>,
        node_id: String,
        other: Vec<String>,
    ) -> Self {
        Self {
            tx,
            rx,
            node_id,
            msg_id: 0,
            nodes: other,
        }
    }

    fn run(&mut self) -> Result<()> {
        let mut backlog = VecDeque::<Message<Payload>>::with_capacity(16);

        let mut value = 0;
        let mut other_values = HashMap::with_capacity(self.nodes.len() - 1);
        let mut last_replication_value = 0;

        loop {
            if let Some(msg) = backlog.pop_front() {
                match msg.body.payload {
                    Payload::Add { delta } => {
                        value += delta;

                        let ack_msg =
                            self.generate_message(Payload::AddOk, msg.src, msg.body.msg_id);
                        self.tx.send(ack_msg.clone())?;
                    }
                    Payload::Read => {
                        let total_value = value + other_values.values().sum::<usize>();

                        let ack_msg = self.generate_message(
                            Payload::ReadOk { value: total_value },
                            msg.src,
                            msg.body.msg_id,
                        );
                        self.tx.send(ack_msg)?;
                    }
                    Payload::Replicate { value } => {
                        other_values.insert(msg.src, value);
                    }
                    m => bail!("Invalid message for client: {m:?}"),
                }

                continue;
            }

            if let Ok(msg) = self.rx.recv_timeout(Duration::from_millis(500)) {
                backlog.push_back(msg);
                continue;
            }

            if last_replication_value != value {
                for id in &self.nodes.clone() {
                    if id == &self.node_id {
                        continue;
                    }

                    let replicate_msg =
                        self.generate_message(Payload::Replicate { value }, id, None);
                    self.tx.send(replicate_msg)?;
                }

                last_replication_value = value;
            }
        }
    }
}

impl GCounterNode {
    fn get_and_increment_id(&mut self) -> usize {
        let old = self.msg_id;
        self.msg_id += 1;
        old
    }

    fn generate_message<S: ToString>(
        &mut self,
        payload: Payload,
        dest: S,
        in_reply_to: Option<usize>,
    ) -> Message<Payload> {
        Message {
            src: self.node_id.clone(),
            dest: dest.to_string(),
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
