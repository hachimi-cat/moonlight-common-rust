use sans_io_time::Instant as SansInstant;
use std::{future::pending, io, pin::pin, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    time::{Instant, sleep, sleep_until},
    try_join,
};
use tracing::{Level, debug, info, instrument, warn};

use crate::stream::{
    HostFeatures, MoonlightStreamConfig, MoonlightStreamSettings,
    audio::OpusMultistreamConfig,
    control::EstimatedRttInfo,
    proto::{
        DynCryptoBackend, MoonlightStreamInput, MoonlightStreamProtoError, MoonlightStreamSetup,
        MoonlightStreamSetupOutput,
        audio::{AudioStream, AudioStreamError, AudioStreamEvent},
        control::{
            ControlStream, ControlStreamEvent, input_batcher::ClientInputEvent,
            packet::ControlPacket, peer::ControlError,
        },
        microphone::foundation::{FoundationMicStream, FoundationMicStreamError},
        video::{VideoStream, VideoStreamError, VideoStreamEvent},
    },
    tokio::driver::StreamDriver,
    video::{VideoCapabilities, VideoSetup},
};

mod driver;

#[derive(Debug, Error)]
pub enum MoonlightStreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("setup: {0}")]
    Setup(#[from] MoonlightStreamProtoError),
    #[error("audio: {0}")]
    Audio(#[from] AudioStreamError),
    #[error("video: {0}")]
    Video(#[from] VideoStreamError),
    #[error("control: {0}")]
    Control(#[from] ControlError),
    #[error("foundation mic: {0}")]
    FoundationMic(#[from] FoundationMicStreamError),
    #[error("connection timed out")]
    ConnectionTimeout,
    #[error("the stream was already closed")]
    Closed,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum MoonlightStreamEvent {
    Audio(AudioStreamEvent),
    Video(VideoStreamEvent),
    Control(ControlStreamEvent),
}

impl From<AudioStreamEvent> for MoonlightStreamEvent {
    fn from(value: AudioStreamEvent) -> Self {
        Self::Audio(value)
    }
}
impl From<VideoStreamEvent> for MoonlightStreamEvent {
    fn from(value: VideoStreamEvent) -> Self {
        Self::Video(value)
    }
}
impl From<ControlStreamEvent> for MoonlightStreamEvent {
    fn from(value: ControlStreamEvent) -> Self {
        Self::Control(value)
    }
}

pub struct MoonlightStream {
    host_features: HostFeatures,
    audio_setup: OpusMultistreamConfig,
    video_setup: VideoSetup,
    audio_stream: StreamDriver<AudioStream>,
    video_stream: StreamDriver<VideoStream>,
    control_stream: StreamDriver<ControlStream>,
    foundation_mic_stream: Option<StreamDriver<FoundationMicStream>>,
}

impl MoonlightStream {
    #[instrument(level = Level::DEBUG, skip_all, name = "stream")]
    pub async fn connect(
        config: MoonlightStreamConfig,
        settings: MoonlightStreamSettings,
        crypto_backend: DynCryptoBackend,
        video_capabilities: VideoCapabilities,
    ) -> Result<Self, MoonlightStreamError> {
        debug!(config = ?config, settings = ?settings, video_capabilities = ?video_capabilities, "stream connect");

        let base_time = Instant::now();

        let mut setup = MoonlightStreamSetup::new(
            SansInstant::from_std(base_time.into_std()),
            config,
            settings,
            crypto_backend,
            video_capabilities,
        )?;

        let mut buffer = vec![0; 4096];
        let mut tcp_stream = None;

        let host_features;

        let mut audio_setup = None;
        let mut video_setup = None;

        let mut audio_stream = None;
        let mut video_stream = None;
        let mut control_stream = None;
        let mut foundation_mic_stream = None;

        loop {
            let timeout = match setup.poll_output()? {
                MoonlightStreamSetupOutput::TcpConnect { addr } => {
                    tcp_stream = Some(TcpStream::connect(addr).await?);
                    continue;
                }
                MoonlightStreamSetupOutput::TcpWrite { data } => {
                    let tcp_stream = tcp_stream.as_mut().expect("tcp write");

                    tcp_stream.write_all(&data).await?;
                    continue;
                }
                MoonlightStreamSetupOutput::Timeout(timeout) => timeout,
                MoonlightStreamSetupOutput::StartAudioStream {
                    config,
                    audio_stream: new_audio_stream,
                } => {
                    audio_setup = Some(config);
                    audio_stream = Some(new_audio_stream);

                    continue;
                }
                MoonlightStreamSetupOutput::StartVideoStream {
                    setup,
                    video_stream: new_video_stream,
                } => {
                    video_setup = Some(setup);
                    video_stream = Some(new_video_stream);

                    continue;
                }
                MoonlightStreamSetupOutput::StartControlStream {
                    control_stream: new_control_stream,
                } => {
                    control_stream = Some(new_control_stream);

                    continue;
                }
                MoonlightStreamSetupOutput::FoundationStartMic {
                    mic_stream: new_foundation_mic_stream,
                } => {
                    foundation_mic_stream = Some(new_foundation_mic_stream);

                    continue;
                }
                MoonlightStreamSetupOutput::Connected { features } => {
                    host_features = features;
                    break;
                }
            };

            select! {
                _ = sleep_until(timeout.to_std(base_time.into_std()).into()) => {
                    setup.handle_input(MoonlightStreamInput::Timeout(SansInstant::from_std(base_time.into_std())))?;
                    continue;
                }
                result = tcp_stream.as_mut().expect("tcp stream should exist in this state").read(&mut buffer), if tcp_stream.is_some() => {
                    let len = result?;

                    let now = SansInstant::from_std(base_time.into_std());
                    if len == 0 {
                        setup.handle_input(MoonlightStreamInput::TcpDisconnected(now))?;
                    } else {
                        setup.handle_input(MoonlightStreamInput::TcpReceive {
                            now,
                            data: &buffer[0..len],
                        })?;
                    }
                }
            };
        }

        debug!("binding all streams");
        let (mut audio_stream, video_stream, mut control_stream, foundation_mic_stream) = try_join!(
            StreamDriver::new(audio_stream.expect("audio stream")),
            StreamDriver::new(video_stream.expect("video stream")),
            StreamDriver::new(control_stream.expect("control stream")),
            async {
                if let Some(foundation_mic_stream) = foundation_mic_stream {
                    StreamDriver::new(foundation_mic_stream).await.map(Some)
                } else {
                    Ok(None)
                }
            }
        )?;

        debug!("waiting for control stream connection");
        // Wait for enet connection
        let mut sleep = pin!(sleep(Duration::from_secs(20)));
        loop {
            select! {
                _ = &mut sleep => {
                    return Err(MoonlightStreamError::ConnectionTimeout);
                }
                result = control_stream.drive() => {
                    let event = result?;

                    match event {
                        ControlStreamEvent::Connect => {
                            info!("control stream connected");
                            break;
                        }
                        event => warn!(event = ?event, "got control stream event before being connected"),
                    }
                },
            }
        }

        // Apollo/Sunshine's recvThread uses one mutable sender endpoint for
        // outstanding audio AND video async receives. Concurrent initial
        // pings can bind one stream to the other stream's UDP port forever.
        // Complete audio discovery before polling the video driver's first
        // ping. Wait for actual media, not a guessed sleep; no host change is
        // required. This initial frame precedes browser media negotiation.
        initialize_audio_stream(&mut audio_stream).await?;

        Ok(Self {
            host_features,
            audio_setup: audio_setup.expect("audio setup"),
            audio_stream,
            video_setup: video_setup.expect("video setup"),
            video_stream,
            control_stream,
            foundation_mic_stream,
        })
    }

    /// If this instance can be discarded
    pub fn is_alive(&mut self) -> bool {
        // TODO: can this function be immutable?
        !self.control_stream.stream_mut().can_discard()
    }

    pub fn audio_setup(&self) -> OpusMultistreamConfig {
        self.audio_setup.clone()
    }
    pub fn video_setup(&self) -> VideoSetup {
        self.video_setup
    }

    pub fn estimated_rtt(&self) -> Result<EstimatedRttInfo, ControlError> {
        self.control_stream.stream().estimated_rtt()
    }

    pub fn send_input(&mut self, input: ClientInputEvent) -> Result<(), ControlError> {
        self.control_stream.stream_mut().batch_input(input)
    }
    pub fn send_raw(&mut self, packet: ControlPacket) -> Result<(), ControlError> {
        self.control_stream.stream_mut().send_raw(packet)
    }

    pub fn disconnect(&mut self) -> Result<(), ControlError> {
        self.control_stream.stream_mut().disconnect(0)
    }

    pub fn send_microphone_opus_data(&mut self, timestamp: Duration, frame: &[u8]) -> bool {
        if let Some(foundation_mic) = self.foundation_mic_stream.as_mut() {
            if let Err(err) = foundation_mic
                .stream_mut()
                .send_microphone_opus_data(timestamp, frame)
            {
                warn!(error = %err, "failed to send microphone data");
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    pub async fn drive(&mut self) -> Result<MoonlightStreamEvent, MoonlightStreamError> {
        select! {
            result = self.audio_stream.drive() => result.map(MoonlightStreamEvent::from),
            result = self.video_stream.drive() => result.map(MoonlightStreamEvent::from),
            result = self.control_stream.drive() => result.map(MoonlightStreamEvent::from),
            _ = async {
                if let Some(stream) = self.foundation_mic_stream.as_mut() {
                    stream.drive().await
                } else {
                    pending().await
                }
            } => unreachable!(),
        }
    }

    pub fn host_features(&self) -> HostFeatures {
        self.host_features.clone()
    }
}

async fn initialize_audio_stream(
    audio_stream: &mut StreamDriver<AudioStream>,
) -> Result<(), MoonlightStreamError> {
    info!("establishing audio endpoint before starting video discovery");
    tokio::time::timeout(Duration::from_secs(5), audio_stream.drive())
        .await
        .map_err(|_| MoonlightStreamError::ConnectionTimeout)??;
    Ok(())
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    use crate::stream::proto::audio::AudioStreamConfig;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn audio_discovery_must_wait_for_a_host_packet_before_video_can_start() {
        let host = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let audio = AudioStream::new_unencrypted(
            SansInstant::from_nanos(0),
            AudioStreamConfig {
                addr: host.local_addr().unwrap(),
                opus_config: OpusMultistreamConfig::STEREO,
                fec: false,
                sunshine_ping: None,
                sunshine_encryption: None,
            },
        );
        let mut audio = StreamDriver::new(audio).await.unwrap();
        let mut pending = Box::pin(initialize_audio_stream(&mut audio));
        let mut ping = [0; 64];
        let (result, received) = tokio::join!(
            tokio::time::timeout(Duration::from_millis(25), &mut pending),
            tokio::time::timeout(Duration::from_secs(1), host.recv_from(&mut ping)),
        );
        assert!(
            result.is_err(),
            "sending a ping alone must not release video discovery"
        );
        let (len, peer) = received.unwrap().unwrap();
        assert_eq!(&ping[..len], b"PING");
        let mut packet = vec![0x80, 97, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        packet.extend([1, 2, 3]);
        host.send_to(&packet, peer).await.unwrap();
        tokio::time::timeout(Duration::from_millis(200), pending)
            .await
            .unwrap()
            .unwrap();
        // Discovery consumes only its startup sample, not future audio.
        packet[3] = 1;
        host.send_to(&packet, peer).await.unwrap();
        let AudioStreamEvent::OnFrame(frame) =
            tokio::time::timeout(Duration::from_millis(200), audio.drive())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(frame.buffer.as_ref(), &[1, 2, 3]);
    }
}
