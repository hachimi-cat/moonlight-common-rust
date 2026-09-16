use std::{net::SocketAddr, sync::Arc, time::Duration};

use super::{
    ControlStream, ControlStreamConfig, ControlStreamEvent,
    packet::{ControlPacketConfig, EnetChannel},
    peer::{ControlHost, ControlHostConfig, ControlHostEvent, ControlPeerConfig, ControlPeerRole},
};
use crate::{
    ServerVersion,
    crypto::rustcrypto::RustCryptoBackend,
    stream::proto::{Instant, runtime::UdpStream},
};

fn connected_pair() -> (ControlStream, ControlHost, SocketAddr, SocketAddr, Instant) {
    let client_addr = "127.0.0.1:40000".parse().unwrap();
    let server_addr = "127.0.0.1:47999".parse().unwrap();
    let version = ServerVersion::new(7, 1, 431, -1);
    let mut client = ControlStream::new(
        Instant::from_nanos(0),
        ControlStreamConfig {
            server_version: version,
            addr: server_addr,
            sunshine_connect_data: None,
            encryption: None,
            apollo_permissions: None,
        },
        Arc::new(RustCryptoBackend),
    )
    .unwrap();
    let mut server = ControlHost::new(
        Instant::from_nanos(0),
        ControlHostConfig {
            peer_count: 1,
            peer_channel_count: EnetChannel::CHANNEL_COUNT,
        },
        Arc::new(RustCryptoBackend),
    )
    .unwrap();
    let mut connected = false;
    for tick in 0..100 {
        let now = Instant::from_nanos(tick * 10_000_000);
        client.handle_timeout(now).unwrap();
        server.handle_timeout(now).unwrap();
        while let Some((_, data)) = client.pending_send() {
            let data = data.to_vec();
            client.consume_send();
            server.handle_receive(now, client_addr, &data).unwrap();
        }
        while let Some(event) = server.poll_event() {
            if let ControlHostEvent::Connected { id, .. } = event {
                server
                    .configure_peer(
                        id,
                        ControlPeerConfig {
                            role: ControlPeerRole::Server,
                            encryption: None,
                            packets: ControlPacketConfig::new(version, false).unwrap(),
                        },
                    )
                    .unwrap();
            }
        }
        while let Some((_, data)) = server.pending_send() {
            let data = data.to_vec();
            server.consume_send();
            client.handle_receive(now, server_addr, &data).unwrap();
        }
        while let Some(event) = client.poll_event() {
            connected |= matches!(event, ControlStreamEvent::Connect);
        }
        if connected && server.configured_peers().count() == 1 && tick > 5 {
            return (client, server, client_addr, server_addr, now);
        }
    }
    panic!("control peers did not connect");
}

#[test]
fn remote_disconnect_is_delivered_before_not_connected_error() {
    let (mut client, mut server, _, server_addr, now) = connected_pair();
    let peer = server.configured_peers().next().unwrap();
    server.disconnect_now(peer, 0).unwrap();
    server
        .handle_timeout(now + Duration::from_millis(50))
        .unwrap();
    while let Some((_, data)) = server.pending_send() {
        let data = data.to_vec();
        server.consume_send();
        client
            .handle_receive(now + Duration::from_millis(50), server_addr, &data)
            .expect("disconnect must remain observable instead of aborting the browser pump");
    }
    assert!(matches!(
        client.poll_event(),
        Some(ControlStreamEvent::Disconnect)
    ));
}

#[test]
fn lost_control_path_delivers_disconnect_without_aborting_timer_pump() {
    let (mut client, _server, _, _, start) = connected_pair();
    for tick in 1..2400 {
        client
            .handle_timeout(start + Duration::from_millis(tick * 50))
            .expect("timeout must deliver Disconnect before returning NotConnected");
        while client.pending_send().is_some() {
            client.consume_send();
        }
        while let Some(event) = client.poll_event() {
            if matches!(event, ControlStreamEvent::Disconnect) {
                return;
            }
        }
    }
    panic!("blackholed control peer did not time out within 120 seconds");
}
