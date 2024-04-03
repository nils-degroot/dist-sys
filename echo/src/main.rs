use std::sync::mpsc;

use anyhow::{bail, Result};
use dist_sys::{Body, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Debug)]
struct EchoNode {
    tx: mpsc::Sender<Message<Payload>>,
    rx: mpsc::Receiver<Message<Payload>>,
    node_id: String,
    msg_id: usize,
}

impl Node<Payload> for EchoNode {
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
        }
    }

    fn run(&mut self) -> Result<()> {
        while let Ok(msg) = self.rx.recv() {
            let echo = match msg.body.payload {
                Payload::Echo { ref echo } => echo,
                Payload::EchoOk { .. } => bail!("Invalid message for node"),
            };

            self.send_response(
                &msg,
                Payload::EchoOk {
                    echo: echo.to_string(),
                },
            )?;
        }

        Ok(())
    }
}

impl EchoNode {
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
    dist_sys::run_dist_sys::<EchoNode, Payload>()?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn simple_echo_test() {
        let (in_tx, in_rx) = mpsc::channel::<Message<Payload>>();
        let (out_tx, out_rx) = mpsc::channel::<Message<Payload>>();

        thread::spawn(move || {
            let mut node = EchoNode::initialize(out_tx, in_rx, "n1".to_string(), vec![]);
            node.run().unwrap();
        });

        in_tx
            .send(Message {
                src: "c1".to_string(),
                dest: "n1".to_string(),
                body: Body {
                    msg_id: Some(10),
                    in_reply_to: None,
                    payload: Payload::Echo {
                        echo: "Echo me 10".to_string(),
                    },
                },
            })
            .unwrap();

        let response = out_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("Failed to get a response in a reasonable time");

        assert_eq!(
            response,
            Message {
                src: "n1".to_string(),
                dest: "c1".to_string(),
                body: Body {
                    msg_id: Some(0),
                    in_reply_to: Some(10),
                    payload: Payload::EchoOk {
                        echo: "Echo me 10".to_string()
                    }
                }
            }
        );
    }
}
