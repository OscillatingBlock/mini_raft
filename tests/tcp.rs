use std::{collections::HashMap, sync::Arc, time::Duration};

use mini_raft::{
    codec::{Codec, Message, RequestVote},
    network::{dispatch_server_msg, NetworkManager},
    server::{Destination, Incoming, NodeId, Outgoing},
};
use tokio::{net::TcpListener, sync::mpsc};

/// Two real NetworkManagers over localhost TCP: handshake + framed delivery.
/// Regression coverage for codec length-prefix and handshake logic.
#[tokio::test]
async fn tcp_handshake_and_message_delivery() {
    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a1 = l1.local_addr().unwrap().to_string();
    let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a2 = l2.local_addr().unwrap().to_string();

    let id1 = NodeId::new(10);
    let id2 = NodeId::new(20);

    let (to_srv1_tx, mut to_srv1_rx) = mpsc::channel::<Incoming>(32);
    let (to_srv2_tx, mut to_srv2_rx) = mpsc::channel::<Incoming>(32);
    let (disp1_tx, disp1_rx) = mpsc::channel::<Outgoing>(32);
    let (disp2_tx, disp2_rx) = mpsc::channel::<Outgoing>(32);

    let nm1 = Arc::new(NetworkManager::new(to_srv1_tx, Codec::new(), id1));
    let nm2 = Arc::new(NetworkManager::new(to_srv2_tx, Codec::new(), id2));

    // Listeners.
    let nm1l = nm1.clone();
    tokio::spawn(async move { let _ = nm1l.listen(l1).await; });
    let nm2l = nm2.clone();
    tokio::spawn(async move { let _ = nm2l.listen(l2).await; });

    // Dispatchers (server -> network).
    let nm1d = nm1.clone();
    tokio::spawn(async move { dispatch_server_msg(disp1_rx, nm1d).await; });
    let nm2d = nm2.clone();
    tokio::spawn(async move { dispatch_server_msg(disp2_rx, nm2d).await; });

    // id1 < id2 so id1 dials id2 (dial_peers only dials higher ids).
    let mut peers = HashMap::new();
    peers.insert(id2, a2.clone());
    let _ = a1;
    nm1.dial_peers(peers, id1).await;

    // Give handshake time to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let ping = Message::RequestVoteType(RequestVote {
        term: 1,
        candidate_id: id1,
        last_log_index: 0,
        last_log_term: 0,
    });
    disp1_tx
        .send(Outgoing {
            dest: Destination::Node(id2),
            msg: ping,
        })
        .await
        .unwrap();

    let got = tokio::time::timeout(Duration::from_secs(3), to_srv2_rx.recv())
        .await
        .expect("timed out waiting for TCP-delivered message")
        .expect("channel closed");

    match got.msg {
        Message::RequestVoteType(req) => {
            assert_eq!(req.candidate_id, id1);
            assert_eq!(req.term, 1);
        }
        other => panic!("expected RequestVote over TCP, got {}", other.message_type()),
    }

    // Reverse direction works too (already-connected peer registry).
    disp2_tx
        .send(Outgoing {
            dest: Destination::Node(id1),
            msg: Message::RequestVoteType(RequestVote {
                term: 2,
                candidate_id: id2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await
        .unwrap();

    let got2 = tokio::time::timeout(Duration::from_secs(3), to_srv1_rx.recv())
        .await
        .expect("timed out on reverse path")
        .expect("channel closed");
    match got2.msg {
        Message::RequestVoteType(req) => assert_eq!(req.term, 2),
        other => panic!("expected reverse RequestVote, got {}", other.message_type()),
    }
}
