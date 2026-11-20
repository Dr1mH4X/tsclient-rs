use std::collections::HashMap;

use crate::command::build_command_ordered;

use super::errors::{MAX_COMMAND_LINE_BYTES, StreamError, StreamPayloadTooLargeError};
use super::types::{
    IceCandidate, PublishOptions, RawSignal, StreamAccessibility, StreamInfo, StreamSource,
    StreamTransport,
};

/// 校验并构造已转义的命令行；超过 [`MAX_COMMAND_LINE_BYTES`] 时在发送前拒绝。
pub(crate) fn encode_checked_command(
    command: &str,
    params: &[(&str, &str)],
) -> Result<String, StreamError> {
    let line = build_command_ordered(command, params);
    let size = line.len();
    if size > MAX_COMMAND_LINE_BYTES {
        return Err(StreamPayloadTooLargeError {
            command: command.to_string(),
            size,
            limit: MAX_COMMAND_LINE_BYTES,
        }
        .into());
    }
    Ok(line)
}

/// 构造 `setupstream` 命令（包含服务端要求的全部 7 个参数，固定 P2P `mode=1`）。
pub(crate) fn build_setup_command(options: &PublishOptions) -> Result<String, StreamError> {
    let Some(source) = options.source.known_wire_code() else {
        return Err(StreamError::InvalidOption(format!(
            "unknown stream source {:?}",
            options.source
        )));
    };
    let Some(accessibility) = options.accessibility.known_wire_code() else {
        return Err(StreamError::InvalidOption(format!(
            "unknown stream accessibility {:?}",
            options.accessibility
        )));
    };
    if options.bitrate > i32::MAX as u32 {
        return Err(StreamError::InvalidOption(
            "the bitrate must be an integer from 0 to 2147483647".to_string(),
        ));
    }

    let source_str = source.to_string();
    let bitrate_str = options.bitrate.to_string();
    let access_str = accessibility.to_string();
    let limit_str = options.viewer_limit.to_string();
    let audio_str = if options.audio { "1" } else { "0" };

    encode_checked_command(
        "setupstream",
        &[
            ("name", options.name.as_str()),
            ("type", source_str.as_str()),
            ("bitrate", bitrate_str.as_str()),
            ("accessibility", access_str.as_str()),
            ("mode", "1"),
            ("viewer_limit", limit_str.as_str()),
            ("audio", audio_str),
        ],
    )
}

/// 构造 `joinstreamrequest`（`remove=false` 加入，`remove=true` 离开或撤回）。
pub(crate) fn build_join_command(
    id: &str,
    streamer: i32,
    message: &str,
    remove: bool,
) -> Result<String, StreamError> {
    let clid_str = streamer.to_string();
    let remove_str = if remove { "1" } else { "0" };
    encode_checked_command(
        "joinstreamrequest",
        &[
            ("id", id),
            ("clid", clid_str.as_str()),
            ("msg", message),
            ("is_remove", remove_str),
        ],
    )
}

/// 构造 `respondjoinstreamrequest`（拒绝时必须传空 `offer`）。
pub(crate) fn build_respond_command(
    id: &str,
    viewer: i32,
    message: &str,
    offer: &str,
    accept: bool,
) -> Result<String, StreamError> {
    let clid_str = viewer.to_string();
    let decision_str = if accept { "1" } else { "0" };
    encode_checked_command(
        "respondjoinstreamrequest",
        &[
            ("id", id),
            ("clid", clid_str.as_str()),
            ("msg", message),
            ("offer", offer),
            ("decision", decision_str),
        ],
    )
}

/// 构造 `streamsignaling` 命令（透传 JSON 信令字符串）。
pub(crate) fn build_signal_command(id: &str, peer: i32, json: &str) -> Result<String, StreamError> {
    let clid_str = peer.to_string();
    encode_checked_command(
        "streamsignaling",
        &[("id", id), ("clid", clid_str.as_str()), ("json", json)],
    )
}

/// 构造 `stopstream` 命令。
pub(crate) fn build_stop_command(id: &str, reason: u32) -> Result<String, StreamError> {
    let reason_str = reason.to_string();
    encode_checked_command("stopstream", &[("id", id), ("reason", reason_str.as_str())])
}

/// 构造 `requeststreaminfo` 命令。
pub(crate) fn build_info_command(clid: i32) -> Result<String, StreamError> {
    let clid_str = clid.to_string();
    encode_checked_command("requeststreaminfo", &[("clid", clid_str.as_str())])
}

// ---- Signalling JSON --------------------------------------------------------

/// 编码自定义信令 JSON 文本。
pub(crate) fn encode_signal(cmd: &str, args: &serde_json::Value) -> String {
    serde_json::json!({ "cmd": cmd, "args": args }).to_string()
}

/// 编码 SDP answer 信令。
pub(crate) fn answer_json(sdp: &str) -> String {
    serde_json::json!({ "cmd": "answer", "args": { "answer": sdp } }).to_string()
}

/// 编码重协商 SDP offer 信令。
pub(crate) fn offer_json(sdp: &str) -> String {
    serde_json::json!({ "cmd": "offer", "args": { "offer": sdp } }).to_string()
}

/// 编码 ICE Candidate 信令。
pub(crate) fn candidate_json(candidate: &IceCandidate) -> String {
    serde_json::json!({
        "cmd": "iceCandidate",
        "args": {
            "sdp": candidate.candidate,
            "mid": candidate.sdp_mid,
            "mLine": candidate.sdp_m_line_index,
        }
    })
    .to_string()
}

/// 解析 `notifystreamsignaling` 的信令消息分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedSignal {
    Offer(String),
    Answer(String),
    Candidate(IceCandidate),
    Raw(RawSignal),
}

fn json_strict_u16(value: &serde_json::Value) -> Option<u16> {
    let serde_json::Value::Number(num) = value else {
        return None;
    };
    if num.as_f64().is_some_and(f64::is_sign_negative) {
        return None;
    }
    let val = num.as_u64()?;
    u16::try_from(val).ok()
}

/// 解析 `notifystreamsignaling` 的 `json` 字段；无法识别或非 JSON 时作为 [`RawSignal`] 透传。
pub fn parse_signal(json: &str) -> ParsedSignal {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json) else {
        return ParsedSignal::Raw(RawSignal {
            cmd: String::new(),
            args: None,
            json: json.to_string(),
        });
    };
    let Some(obj) = parsed.as_object() else {
        return ParsedSignal::Raw(RawSignal {
            cmd: String::new(),
            args: Some(parsed),
            json: json.to_string(),
        });
    };
    let Some(cmd) = obj.get("cmd").and_then(serde_json::Value::as_str) else {
        return ParsedSignal::Raw(RawSignal {
            cmd: String::new(),
            args: Some(parsed),
            json: json.to_string(),
        });
    };
    let args = obj.get("args");
    let make_raw = || {
        ParsedSignal::Raw(RawSignal {
            cmd: cmd.to_string(),
            args: args.cloned(),
            json: json.to_string(),
        })
    };

    match cmd {
        "answer" => args
            .and_then(serde_json::Value::as_object)
            .and_then(|map| map.get("answer"))
            .and_then(serde_json::Value::as_str)
            .map(|sdp| ParsedSignal::Answer(sdp.to_string()))
            .unwrap_or_else(make_raw),
        "offer" | "reconnectOffer" => match args {
            Some(serde_json::Value::String(sdp)) => ParsedSignal::Offer(sdp.clone()),
            Some(serde_json::Value::Object(map)) => map
                .get("offer")
                .or_else(|| map.get("sdp"))
                .and_then(serde_json::Value::as_str)
                .map(|sdp| ParsedSignal::Offer(sdp.to_string()))
                .unwrap_or_else(make_raw),
            _ => make_raw(),
        },
        "iceCandidate" => {
            let Some(map) = args.and_then(serde_json::Value::as_object) else {
                return make_raw();
            };
            let Some(sdp) = map.get("sdp").and_then(serde_json::Value::as_str) else {
                return make_raw();
            };
            let sdp_mid = map
                .get("mid")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            let sdp_m_line_index = map.get("mLine").and_then(json_strict_u16);
            ParsedSignal::Candidate(IceCandidate {
                candidate: sdp.to_string(),
                sdp_mid,
                sdp_m_line_index,
            })
        }
        _ => make_raw(),
    }
}

// ---- Notifications ----------------------------------------------------------

fn parse_wire_i64(raw: Option<&String>) -> Option<i64> {
    let s = raw?;
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || digits.len() > 15 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<i64>().ok()
}

fn parse_wire_u32(raw: Option<&String>, max_digits: Option<usize>) -> Option<u32> {
    let s = raw?;
    if s.is_empty()
        || max_digits.is_some_and(|max| s.len() > max)
        || !s.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    s.parse::<u32>().ok()
}

fn parse_uint16_clid(raw: Option<&String>) -> Option<i32> {
    let s = raw?;
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u16>().ok().map(i32::from)
}

/// 从 `notifystreamstarted` 或 `notifystreaminfo` 参数构建 [`StreamInfo`]，并与已有信息合并。
/// 对于不含 `id` 的空 `notifystreaminfo` 响应返回 `None`。
pub(crate) fn stream_info_from(
    params: &HashMap<String, String>,
    known: Option<&StreamInfo>,
) -> Option<StreamInfo> {
    let id = params.get("id")?;
    if id.is_empty() {
        return None;
    }

    let source_code = parse_wire_u32(params.get("type"), Some(15));
    let access_code = parse_wire_u32(
        params.get("accessibility").or_else(|| params.get("access")),
        Some(15),
    );
    let mode_code = parse_wire_u32(params.get("mode"), Some(15));
    let bitrate = parse_wire_i64(params.get("bitrate"));
    let viewer_limit = parse_wire_i64(params.get("viewer_limit"));
    let viewers = parse_wire_i64(params.get("viewer"));
    let clid = parse_uint16_clid(params.get("clid"));

    let client_id = match clid {
        Some(c) if c != 0 => c,
        _ => known.map_or(0, |info| info.client_id),
    };

    Some(StreamInfo {
        id: id.clone(),
        client_id,
        name: params
            .get("name")
            .cloned()
            .or_else(|| known.map(|info| info.name.clone()))
            .unwrap_or_default(),
        source: source_code
            .map(StreamSource::from_wire)
            .or_else(|| known.and_then(|info| info.source)),
        audio: match params.get("audio") {
            Some(v) => v == "1",
            None => known.is_some_and(|info| info.audio),
        },
        bitrate: bitrate.or_else(|| known.and_then(|info| info.bitrate)),
        accessibility: access_code
            .map(StreamAccessibility::from_wire)
            .or_else(|| known.and_then(|info| info.accessibility)),
        transport: match mode_code {
            Some(code) => StreamTransport::from_wire(code),
            None => known.and_then(|info| info.transport),
        },
        viewer_limit: viewer_limit.or_else(|| known.and_then(|info| info.viewer_limit)),
        viewers: viewers.or_else(|| known.and_then(|info| info.viewers)),
    })
}

/// 从通知参数读取指定字段的客户端 ID；缺失或非法时返回 `0`。
pub(crate) fn client_of(params: &HashMap<String, String>, key: &str) -> i32 {
    parse_uint16_clid(params.get(key)).unwrap_or(0)
}

/// 从 `notifystreamstopped` 读取停止原因码；缺失或越界时返回 `0`。
pub(crate) fn reason_of(params: &HashMap<String, String>) -> u32 {
    parse_wire_u32(params.get("reason"), None).unwrap_or(0)
}

/// 检查 SDP 文本中是否包含音频媒体段（任意行以 `m=audio ` 或 `m=audio\t` 开头）。
pub fn has_audio_section(sdp: &str) -> bool {
    sdp.split('\n')
        .any(|line| line.starts_with("m=audio ") || line.starts_with("m=audio\t"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn setup_command_includes_all_seven_parameters_in_p2p_mode() {
        let cmd = build_setup_command(&PublishOptions::new("Screen Demo")).expect("build setup");
        assert_eq!(
            cmd,
            "setupstream name=Screen\\sDemo type=2 bitrate=1500000 accessibility=1 mode=1 viewer_limit=0 audio=0"
        );
    }

    #[test]
    fn join_respond_signal_stop_and_info_commands_match_wire_format() {
        assert_eq!(
            build_join_command("stream-1", 42, "hello world", false).unwrap(),
            "joinstreamrequest id=stream-1 clid=42 msg=hello\\sworld is_remove=0"
        );
        assert_eq!(
            build_join_command("stream-1", 42, "", true).unwrap(),
            "joinstreamrequest id=stream-1 clid=42 msg= is_remove=1"
        );
        assert_eq!(
            build_respond_command("stream-1", 9, "no", "", false).unwrap(),
            "respondjoinstreamrequest id=stream-1 clid=9 msg=no offer= decision=0"
        );
        assert_eq!(
            build_signal_command("stream-1", 9, r#"{"cmd":"answer"}"#).unwrap(),
            r#"streamsignaling id=stream-1 clid=9 json={"cmd":"answer"}"#
        );
        assert_eq!(
            build_stop_command("stream-1", 7).unwrap(),
            "stopstream id=stream-1 reason=7"
        );
        assert_eq!(build_info_command(12).unwrap(), "requeststreaminfo clid=12");
    }

    #[test]
    fn command_line_limit_counts_escaped_utf8_bytes() {
        let ok_offer = "a".repeat(4_000);
        assert!(build_respond_command("id", 2, "", &ok_offer, true).is_ok());

        let too_many_spaces = " ".repeat(5_000);
        let err = build_respond_command("id", 2, "", &too_many_spaces, true).unwrap_err();
        match err {
            StreamError::PayloadTooLarge(e) => {
                assert_eq!(e.command, "respondjoinstreamrequest");
                assert_eq!(e.limit, MAX_COMMAND_LINE_BYTES);
                assert!(e.size > MAX_COMMAND_LINE_BYTES);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }

        let utf8 = "日".repeat(3_100);
        assert!(matches!(
            build_respond_command("id", 2, "", &utf8, true),
            Err(StreamError::PayloadTooLarge(_))
        ));
    }

    #[test]
    fn parse_signal_covers_offer_reconnect_answer_candidate_and_raw() {
        assert_eq!(
            parse_signal(r#"{"cmd":"answer","args":{"answer":"v=0\r\n"}}"#),
            ParsedSignal::Answer("v=0\r\n".to_string())
        );
        for raw_offer in [
            r#"{"cmd":"offer","args":"sdp-1"}"#,
            r#"{"cmd":"offer","args":{"offer":"sdp-1"}}"#,
            r#"{"cmd":"reconnectOffer","args":{"sdp":"sdp-1"}}"#,
        ] {
            assert_eq!(
                parse_signal(raw_offer),
                ParsedSignal::Offer("sdp-1".to_string())
            );
        }

        assert_eq!(
            parse_signal(
                r#"{"cmd":"iceCandidate","args":{"sdp":"candidate:1","mid":"0","mLine":0}}"#
            ),
            ParsedSignal::Candidate(IceCandidate {
                candidate: "candidate:1".to_string(),
                sdp_mid: Some("0".to_string()),
                sdp_m_line_index: Some(0),
            })
        );

        for invalid_mline in ["65536", "-1", "-0", "1.5", r#""1""#] {
            let line =
                format!(r#"{{"cmd":"iceCandidate","args":{{"sdp":"c","mLine":{invalid_mline}}}}}"#);
            match parse_signal(&line) {
                ParsedSignal::Candidate(c) => {
                    assert_eq!(c.sdp_m_line_index, None, "for {invalid_mline}")
                }
                other => panic!("expected Candidate, got {other:?}"),
            }
        }

        match parse_signal("not-json") {
            ParsedSignal::Raw(r) => {
                assert_eq!(r.cmd, "");
                assert!(r.args.is_none());
                assert_eq!(r.json, "not-json");
            }
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    #[test]
    fn stream_info_from_merges_and_validates_wire_numbers() {
        let started = stream_info_from(
            &params(&[
                ("id", "u-1"),
                ("clid", "5"),
                ("name", "Demo"),
                ("type", "3"),
                ("audio", "1"),
                ("bitrate", "1500000"),
                ("access", "3"),
                ("mode", "1"),
                ("viewer_limit", "4"),
            ]),
            None,
        )
        .expect("started info");
        assert_eq!(started.source, Some(StreamSource::Window));
        assert_eq!(started.accessibility, Some(StreamAccessibility::Private));
        assert_eq!(started.transport, Some(StreamTransport::P2p));
        assert!(started.audio);

        let updated = stream_info_from(&params(&[("id", "u-1"), ("viewer", "2")]), Some(&started))
            .expect("merged info");
        assert_eq!(updated.viewers, Some(2));
        assert_eq!(updated.name, "Demo");
        assert_eq!(updated.client_id, 5);
        assert_eq!(updated.transport, Some(StreamTransport::P2p));

        let unknown = stream_info_from(
            &params(&[
                ("id", "a"),
                ("type", "4294967295"),
                ("accessibility", "4294967295"),
                ("mode", "2"),
            ]),
            None,
        )
        .expect("unknown wire values");
        assert_eq!(unknown.source, Some(StreamSource::Unknown(4_294_967_295)));
        assert_eq!(
            unknown.accessibility,
            Some(StreamAccessibility::Unknown(4_294_967_295))
        );
        assert_eq!(unknown.transport, Some(StreamTransport::Unknown(2)));

        for bad in [
            "4294967296",
            "-1",
            "+1",
            "1.5",
            "",
            "1234567890123456",
            "0x1",
        ] {
            let info = stream_info_from(
                &params(&[
                    ("id", "a"),
                    ("type", bad),
                    ("accessibility", bad),
                    ("mode", bad),
                ]),
                None,
            )
            .unwrap();
            assert_eq!(info.source, None, "bad={bad}");
            assert_eq!(info.accessibility, None, "bad={bad}");
            assert_eq!(info.transport, None, "bad={bad}");
        }

        assert!(stream_info_from(&params(&[("return_code", "4")]), None).is_none());
        assert!(stream_info_from(&params(&[("id", "")]), None).is_none());
        assert_eq!(reason_of(&params(&[("reason", "0000000000000000007")])), 7);
        assert_eq!(reason_of(&params(&[("reason", "4294967296")])), 0);
    }

    #[test]
    fn has_audio_section_detects_m_audio_line() {
        assert!(has_audio_section(
            "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nm=video 9"
        ));
        assert!(has_audio_section("m=audio\t9"));
        assert!(!has_audio_section("v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96"));
        assert!(!has_audio_section("a=mid:m=audio 9"));
    }
}
