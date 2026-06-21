//! Packet types and framing — mirrors `teamspeak-js/src/transport/packet.ts`

#![allow(non_upper_case_globals)]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Voice = 0,
    VoiceWhisper = 1,
    Command = 2,
    CommandLow = 3,
    Ping = 4,
    Pong = 5,
    Ack = 6,
    AckLow = 7,
    Init1 = 8,
}

pub struct PacketFlags;

impl PacketFlags {
    pub const Fragmented: u8 = 0x10;
    pub const NewProtocol: u8 = 0x20;
    pub const Compressed: u8 = 0x40;
    pub const Unencrypted: u8 = 0x80;
}

#[derive(Debug, Clone)]
pub struct Packet {
    pub type_flagged: u8,
    pub id: u16,
    pub client_id: u16,
    pub generation_id: u32,
    pub data: Vec<u8>,
    pub received_at: std::time::Instant,
}

pub fn packet_type(p: &Packet) -> PacketType {
    match p.type_flagged & 0x0f {
        0 => PacketType::Voice,
        1 => PacketType::VoiceWhisper,
        2 => PacketType::Command,
        3 => PacketType::CommandLow,
        4 => PacketType::Ping,
        5 => PacketType::Pong,
        6 => PacketType::Ack,
        7 => PacketType::AckLow,
        8 => PacketType::Init1,
        _ => PacketType::Voice,
    }
}

pub fn packet_flags(p: &Packet) -> u8 {
    p.type_flagged & 0xf0
}

pub fn is_unencrypted(p: &Packet) -> bool {
    packet_flags(p) & PacketFlags::Unencrypted != 0
}

pub fn build_c2s_header(p: &Packet) -> Vec<u8> {
    let mut header = vec![0u8; 5];
    header[..2].copy_from_slice(&p.id.to_be_bytes());
    header[2..4].copy_from_slice(&p.client_id.to_be_bytes());
    header[4] = p.type_flagged;
    header
}

pub fn parse_s2c_header(raw: &[u8]) -> (u16, u8) {
    let id = u16::from_be_bytes([raw[0], raw[1]]);
    let type_flagged = raw[2];
    (id, type_flagged)
}

pub fn parse_c2s_header(raw: &[u8]) -> (u16, u16, u8) {
    let id = u16::from_be_bytes([raw[0], raw[1]]);
    let client_id = u16::from_be_bytes([raw[2], raw[3]]);
    let type_flagged = raw[4];
    (id, client_id, type_flagged)
}
