use super::*;
use sans_io_time::Instant;

#[cfg(feature = "rustcrypto")]
#[test]
fn bad_encrypted_audio_packet_must_not_terminate_the_stream() {
    use super::payloader::{AudioPayloader, AudioPayloaderConfig};
    use crate::crypto::rustcrypto::RustCryptoBackend;
    let key = crate::stream::AesKey([0x42; 16]);
    let mut source = AudioPayloader::new(
        AudioPayloaderConfig {
            fec: false,
            frame_len: 3,
            encryption: Some((key, crate::stream::AesIv(123))),
        },
        RustCryptoBackend,
    );
    let mut stream = AudioStream::new(
        Instant::from_nanos(0),
        AudioStreamConfig {
            addr: "127.0.0.1:48000".parse().unwrap(),
            opus_config: OpusMultistreamConfig::STEREO,
            fec: false,
            sunshine_ping: None,
            sunshine_encryption: Some(SunshineEncryption {
                aes_key: key,
                aes_iv: crate::stream::AesIv(123),
            }),
        },
        Arc::new(RustCryptoBackend),
    );
    source.push_frame(0, &[1, 2, 3]).unwrap();
    let mut packet = source.poll_packet().unwrap().unwrap().to_vec();
    packet.pop(); // invalid CBC block length, never pass plaintext to the decoder
    assert!(
        stream
            .handle_receive(Instant::from_nanos(0), stream.addr, &packet)
            .is_ok()
    );
    assert!(stream.poll_event().is_none());
    source.push_frame(5, &[4, 5, 6]).unwrap();
    let packet = source.poll_packet().unwrap().unwrap();
    stream
        .handle_receive(Instant::from_nanos(5_000_000), stream.addr, packet)
        .unwrap();
    let Some(AudioStreamEvent::OnFrame(frame)) = stream.poll_event() else {
        panic!("valid audio must resume")
    };
    assert_eq!(frame.buffer.as_ref(), &[4, 5, 6]);
}

#[test]
fn quiet_audio_must_not_keep_an_expired_timer() {
    let mut stream = AudioStream::new_unencrypted(
        Instant::from_nanos(0),
        AudioStreamConfig {
            addr: "127.0.0.1:48000".parse().unwrap(),
            opus_config: OpusMultistreamConfig::STEREO,
            fec: false,
            sunshine_ping: None,
            sunshine_encryption: None,
        },
    );
    stream.ping_sender.set_finished();
    let now = Instant::from_nanos(101_000_000);
    stream.handle_timeout(now).unwrap();
    assert!(
        stream.poll_timeout().is_none_or(|deadline| deadline > now),
        "silence must not spin the shared media runtime"
    );
}

#[test]
fn audio_setup_must_schedule_its_initial_ping() {
    let stream = AudioStream::new_unencrypted(
        Instant::from_nanos(0),
        AudioStreamConfig {
            addr: "127.0.0.1:48000".parse().unwrap(),
            opus_config: OpusMultistreamConfig::STEREO,
            fec: false,
            sunshine_ping: None,
            sunshine_encryption: None,
        },
    );
    assert_eq!(stream.poll_timeout(), Some(Instant::from_nanos(0)));
}
