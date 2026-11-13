//! `setconnectioninfo` builder — mirrors
//! `teamspeak-js/src/client/connection-info.ts`.

use crate::command::build_command_ordered;
use crate::transport::{PacketLoss, RoundTrip, TrafficClass, TrafficSnapshot};

/// The classes in the order the server lists them.
const CLASSES: [TrafficClass; 3] = [
    TrafficClass::Speech,
    TrafficClass::Keepalive,
    TrafficClass::Control,
];

fn whole(n: f64) -> String {
    format!("{}", n.round() as u64)
}

/// The `setconnectioninfo` line that answers a `notifyconnectioninforequest`,
/// from what the connection has measured: the keepalive round trip (left out
/// until a pong has come back), estimated server-to-client packet loss fractions
/// (left out until S2C sequence samples arrive), packets and bytes sent and
/// received per class, and the bandwidth of the last second and of the last
/// minute per class, in bytes per second. File transfer figures are not tracked,
/// so they are not sent.
pub(crate) fn build_set_connection_info(
    traffic: &TrafficSnapshot,
    round_trip: Option<RoundTrip>,
    packet_loss: Option<PacketLoss>,
) -> String {
    let mut fields: Vec<(String, String)> = Vec::with_capacity(30);
    if let Some(rt) = round_trip {
        fields.push(("connection_ping".to_string(), format!("{:.4}", rt.mean_ms)));
        fields.push((
            "connection_ping_deviation".to_string(),
            format!("{:.4}", rt.deviation_ms),
        ));
    }
    if let Some(loss) = packet_loss {
        fields.push((
            "connection_server2client_packetloss_speech".to_string(),
            format!("{:.4}", loss.speech),
        ));
        fields.push((
            "connection_server2client_packetloss_keepalive".to_string(),
            format!("{:.4}", loss.keepalive),
        ));
        fields.push((
            "connection_server2client_packetloss_control".to_string(),
            format!("{:.4}", loss.control),
        ));
        fields.push((
            "connection_server2client_packetloss_total".to_string(),
            format!("{:.4}", loss.total),
        ));
    }

    for (direction, figures) in [("sent", &traffic.sent), ("received", &traffic.received)] {
        for c in CLASSES {
            fields.push((
                format!("connection_packets_{direction}_{}", c.as_str()),
                figures.get(c).packets.to_string(),
            ));
        }
        for c in CLASSES {
            fields.push((
                format!("connection_bytes_{direction}_{}", c.as_str()),
                figures.get(c).bytes.to_string(),
            ));
        }
    }

    for (direction, figures) in [("sent", &traffic.sent), ("received", &traffic.received)] {
        for c in CLASSES {
            fields.push((
                format!(
                    "connection_bandwidth_{direction}_last_second_{}",
                    c.as_str()
                ),
                figures.get(c).bytes_last_second.to_string(),
            ));
        }
        for c in CLASSES {
            fields.push((
                format!(
                    "connection_bandwidth_{direction}_last_minute_{}",
                    c.as_str()
                ),
                whole(figures.get(c).bytes_per_second_last_minute),
            ));
        }
    }

    let refs: Vec<(&str, &str)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    build_command_ordered("setconnectioninfo", &refs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::parse_command;
    use crate::transport::{PacketType, TrafficMeter};

    fn fixture_traffic() -> TrafficSnapshot {
        let mut meter = TrafficMeter::new();
        meter.record_sent(PacketType::Voice, 120, 1_000);
        meter.record_sent(PacketType::Command, 80, 1_000);
        meter.record_received(PacketType::Pong, 13, 1_500);
        meter.snapshot(3_000)
    }

    #[test]
    fn sends_counters_of_every_class_and_direction() {
        let traffic = fixture_traffic();
        let params = parse_command(&build_set_connection_info(&traffic, None, None))
            .unwrap()
            .params;
        assert_eq!(params["connection_packets_sent_speech"], "1");
        assert_eq!(params["connection_bytes_sent_speech"], "120");
        assert_eq!(params["connection_packets_sent_control"], "1");
        assert_eq!(params["connection_bytes_received_keepalive"], "13");
        assert_eq!(params["connection_packets_received_speech"], "0");
        assert_eq!(params.len(), 12 + 12);
    }

    #[test]
    fn leaves_ping_and_loss_out_until_measured_then_sends_four_decimals() {
        let traffic = fixture_traffic();
        let without_ping = parse_command(&build_set_connection_info(&traffic, None, None))
            .unwrap()
            .params;
        assert!(!without_ping.contains_key("connection_ping"));
        assert!(!without_ping.contains_key("connection_ping_deviation"));
        assert!(!without_ping.contains_key("connection_server2client_packetloss_total"));

        let params = parse_command(&build_set_connection_info(
            &traffic,
            Some(RoundTrip {
                mean_ms: 12.34567,
                deviation_ms: 0.5,
            }),
            Some(PacketLoss {
                speech: 0.01234,
                keepalive: 0.0,
                control: 0.005,
                total: 0.00876,
            }),
        ))
        .unwrap()
        .params;
        assert_eq!(params["connection_ping"], "12.3457");
        assert_eq!(params["connection_ping_deviation"], "0.5000");
        assert_eq!(
            params["connection_server2client_packetloss_speech"],
            "0.0123"
        );
        assert_eq!(
            params["connection_server2client_packetloss_keepalive"],
            "0.0000"
        );
        assert_eq!(
            params["connection_server2client_packetloss_control"],
            "0.0050"
        );
        assert_eq!(
            params["connection_server2client_packetloss_total"],
            "0.0088"
        );
    }

    #[test]
    fn never_sends_figures_it_does_not_track() {
        let traffic = fixture_traffic();
        let text = build_set_connection_info(
            &traffic,
            Some(RoundTrip {
                mean_ms: 1.0,
                deviation_ms: 1.0,
            }),
            Some(PacketLoss {
                speech: 0.0,
                keepalive: 0.0,
                control: 0.0,
                total: 0.0,
            }),
        );
        assert!(!text.contains("client2server_packetloss"));
        assert!(!text.contains("filetransfer"));
        assert!(!text.contains("connected_time"));
        assert!(!text.contains("connection_client_"));
    }

    #[test]
    fn sends_whole_numbers_for_counters_and_bandwidths() {
        let traffic = fixture_traffic();
        let params = parse_command(&build_set_connection_info(&traffic, None, None))
            .unwrap()
            .params;
        for (key, value) in &params {
            if key.starts_with("connection_") {
                assert!(
                    !value.is_empty() && value.chars().all(|c| c.is_ascii_digit()),
                    "{key}={value}"
                );
            }
        }
    }
}
