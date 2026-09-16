use super::payloader::{VideoPayloader, VideoPayloaderConfig, VideoPayloaderFecConfig};
use super::*;
use crate::{ServerVersion, crypto::disabled::DisabledCryptoBackend, stream::video::VideoFormat};
use std::sync::Arc;

fn fixture() -> (VideoStream, VideoPayloader) {
    let version = ServerVersion::new(7, 1, 431, -1);
    let stream = VideoStream::new(
        Instant::from_nanos(0),
        VideoStreamConfig {
            addr: "127.0.0.1:47998".parse().unwrap(),
            queue: VideoDepayloaderConfig {
                packet_size: 1024,
                format: VideoFormat::H264,
                server_version: version,
            },
            fps: 60,
            sunshine_ping: None,
            sunshine_encryption: None,
        },
        Arc::new(DisabledCryptoBackend),
    );
    let payloader = VideoPayloader::new(VideoPayloaderConfig {
        server_version: version,
        packet_size: 1024,
        fec: Some(VideoPayloaderFecConfig {
            fec_percentage: 20,
            min_required_fec_packets: 1,
        }),
    });
    (stream, payloader)
}

fn feed(
    stream: &mut VideoStream,
    payloader: &mut VideoPayloader,
    now: Instant,
    kind: FrameType,
    data: &[u8],
) {
    // Match real encoded frame sizes; the payloader's tiny-frame padding
    // is unrelated to these receive lifecycle regressions.
    let mut data = data.to_vec();
    data.resize(2000, 0x55);
    payloader.push_frame(0, None, kind, &data).unwrap();
    while let Some(packet) = payloader.poll_packet().unwrap() {
        stream.handle_receive(now, stream.addr, packet).unwrap();
    }
}

#[test]
fn stalled_video_timer_must_advance_after_requesting_recovery() {
    let (mut stream, _) = fixture();
    stream.ping_sender.set_finished();
    let now = Instant::from_nanos(2_000_000_000);
    stream.handle_timeout(now).unwrap();
    assert!(matches!(
        stream.poll_event(),
        Some(VideoStreamEvent::SignalIdr)
    ));
    assert!(
        stream.poll_timeout().unwrap() > now,
        "an already-expired deadline spins the UDP driver and starves the other media/control tasks"
    );
}

#[test]
fn video_setup_must_schedule_its_initial_ping() {
    let (stream, _) = fixture();
    assert_eq!(stream.poll_timeout(), Some(Instant::from_nanos(0)));
}

#[test]
fn completely_missing_frame_uses_the_later_frame_as_loss_evidence() {
    let (mut stream, mut payloader) = fixture();
    let idr = [0, 0, 0, 1, 0x65, 0, 1];
    feed(
        &mut stream,
        &mut payloader,
        Instant::from_nanos(0),
        FrameType::Idr,
        &idr,
    );
    assert!(matches!(
        stream.poll_event(),
        Some(VideoStreamEvent::OnFrame(_))
    ));
    payloader
        .push_frame(0, None, FrameType::PFrame, &[0, 0, 0, 1, 0x41, 0, 1])
        .unwrap();
    while payloader.poll_packet().unwrap().is_some() {} // lose all of frame 2
    let later = Instant::from_nanos(33_000_000);
    feed(
        &mut stream,
        &mut payloader,
        later,
        FrameType::PFrame,
        &[0, 0, 0, 1, 0x41, 0, 1],
    );
    assert!(
        stream.poll_timeout().unwrap() <= later + FULL_FRAME_RECEIVE_TIMEOUT,
        "one missing frame must not wait the two-second whole-stream stall timer"
    );
}

#[test]
fn completed_frames_do_not_accumulate_first_seen_history() {
    let (mut stream, mut payloader) = fixture();
    for i in 0..1000u64 {
        feed(
            &mut stream,
            &mut payloader,
            Instant::from_nanos((i * 16_666_667) as i64),
            FrameType::Idr,
            &[0, 0, 0, 1, 0x65, 0, 1],
        );
        assert!(matches!(
            stream.poll_event(),
            Some(VideoStreamEvent::OnFrame(_))
        ));
    }
    assert!(
        stream.frames_first_seen.len() <= 1,
        "completed frame history grows for the lifetime of the game"
    );
}
