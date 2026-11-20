//! Init1 handshake protocol — mirrors `teamspeak-js/src/handshake/crypt-handshake.ts`

use rand::Rng;

use crate::Error;
use crate::command::build_command_ordered;
use crate::crypto::crypt::{AnsweredInit1Challenge, Crypt};

pub const INIT_VERSION: u32 = 1566914096;

const INIT_VERSION_LEN: usize = 4;
const INIT_TYPE_LEN: usize = 1;
const INIT_STEP_LEN: usize = 21;

/// Handle the TS3INIT1 handshake steps.
/// Returns the response bytes to send, or None if nothing should be sent.
pub fn process_init1(crypt: &mut Crypt, data: Option<&[u8]>) -> Result<Option<Vec<u8>>, Error> {
    if crypt.crypto_init_complete {
        return Ok(None);
    }

    let data = match data {
        Some(d) if d.is_empty() => return Ok(None),
        Some(d) if d[0] == 0x7f => {
            crypt.reset_init1_state();
            return Ok(Some(build_init1_start_packet()));
        }
        Some(d) => d,
        None => return Ok(Some(build_init1_start_packet())),
    };

    match data[0] {
        step @ (0..=2) => {
            if crypt.answered_init1_challenge.is_some()
                || crypt.init1_step.is_some_and(|prev| prev > step)
            {
                return Ok(None);
            }
            let pkt = match step {
                0 => build_init1_step1_packet(data)?,
                1 => build_init1_step2_packet(data)?,
                2 => build_init1_step3_packet(data)?,
                _ => unreachable!(),
            };
            if pkt.is_some() {
                crypt.init1_step = Some(step);
            }
            Ok(pkt)
        }
        3 => build_init1_step4_packet(crypt, data),
        _ => Ok(None),
    }
}

fn build_init1_start_packet() -> Vec<u8> {
    let mut buf = vec![0u8; INIT_VERSION_LEN + INIT_TYPE_LEN + 4 + 4 + 8];
    buf[..4].copy_from_slice(&INIT_VERSION.to_be_bytes());
    buf[4] = 0x00;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let now = if now > 0xffff_ffff {
        0xffff_ffff
    } else {
        now as u32
    };
    buf[5..9].copy_from_slice(&now.to_be_bytes());

    let rng: [u8; 4] = rand::rngs::OsRng.r#gen();
    buf[9..13].copy_from_slice(&rng);

    buf
}

fn build_init1_step1_packet(data: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    if data.len() != INIT_STEP_LEN {
        return Ok(None);
    }

    let ts_rand_offset = INIT_VERSION_LEN + INIT_TYPE_LEN + 4;
    let ts_rand = u32::from_le_bytes(data[ts_rand_offset..ts_rand_offset + 4].try_into().unwrap());

    let mut buf = vec![0u8; INIT_TYPE_LEN + 16 + 4];
    buf[0] = 0x01;
    buf[INIT_TYPE_LEN + 16..INIT_TYPE_LEN + 16 + 4].copy_from_slice(&ts_rand.to_be_bytes());
    Ok(Some(buf))
}

fn build_init1_step2_packet(data: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    if data.len() != INIT_STEP_LEN {
        return Ok(None);
    }

    let mut buf = vec![0u8; INIT_VERSION_LEN + INIT_TYPE_LEN + 16 + 4];
    buf[..4].copy_from_slice(&INIT_VERSION.to_be_bytes());
    buf[4] = 0x02;
    buf[5..].copy_from_slice(&data[1..21]);
    Ok(Some(buf))
}

fn build_init1_step3_packet(data: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    let expected_len = INIT_VERSION_LEN + INIT_TYPE_LEN + 16 + 4;
    if data.len() != expected_len {
        return Ok(None);
    }

    let mut buf = vec![0u8; INIT_TYPE_LEN + 64 + 64 + 4 + 100];
    buf[0] = 0x03;
    buf[INIT_TYPE_LEN + 64 - 1] = 1;
    buf[INIT_TYPE_LEN + 64 + 64 - 1] = 1;
    buf[INIT_TYPE_LEN + 64 + 64..INIT_TYPE_LEN + 64 + 64 + 4].copy_from_slice(&1u32.to_be_bytes());
    Ok(Some(buf))
}

fn build_init1_step4_packet(crypt: &mut Crypt, data: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    let expected_len = INIT_TYPE_LEN + 64 + 64 + 4 + 100;
    if data.len() != expected_len {
        return Ok(None);
    }

    if let Some(ref answered) = crypt.answered_init1_challenge {
        if answered.challenge.as_slice() == data {
            return Ok(Some(answered.response.clone()));
        }
        tracing::debug!("ignoring distinct init1 step 3 challenge after one was already answered");
        return Ok(None);
    }

    let level = u32::from_be_bytes(data[1 + 128..1 + 128 + 4].try_into().unwrap()) as i32;
    let y = crypt.solve_rsa_challenge(data, 1, level)?;

    let alpha_tmp: [u8; 10] = rand::rngs::OsRng.r#gen();
    crypt.alpha_tmp = alpha_tmp.to_vec();

    let alpha_b64 = base64_encode(&alpha_tmp);
    let omega_b64 = crypt.identity.public_key_base64().to_string();

    let cmd = build_command_ordered(
        "clientinitiv",
        &[
            ("alpha", &alpha_b64),
            ("omega", &omega_b64),
            ("ot", "1"),
            ("ip", ""),
        ],
    );
    let cmd_bytes = cmd.as_bytes();

    let mut buf = vec![0u8; INIT_VERSION_LEN + INIT_TYPE_LEN + 232 + 64 + cmd_bytes.len()];
    buf[..4].copy_from_slice(&INIT_VERSION.to_be_bytes());
    buf[4] = 0x04;
    buf[5..5 + 232].copy_from_slice(&data[1..233]);
    buf[5 + 232..5 + 232 + 64].copy_from_slice(&y[..64]);
    buf[5 + 232 + 64..].copy_from_slice(cmd_bytes);

    crypt.init1_step = Some(3);
    crypt.answered_init1_challenge = Some(AnsweredInit1Challenge {
        challenge: data.to_vec(),
        response: buf.clone(),
    });

    Ok(Some(buf))
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::generate_identity;

    fn make_step3_challenge(level: u32, non_zero_modulus: bool) -> Vec<u8> {
        let mut data = vec![0u8; INIT_TYPE_LEN + 64 + 64 + 4 + 100];
        data[0] = 0x03;
        data[1 + 63] = 0x03; // x = 3
        if non_zero_modulus {
            data[1 + 64 + 63] = 0x11; // n = 17
        }
        data[1 + 128..1 + 128 + 4].copy_from_slice(&level.to_be_bytes());
        data[1 + 132] = 0xaa;
        data
    }

    #[test]
    fn repeated_and_distinct_step3_challenges_preserve_alpha_tmp() {
        let mut crypt = Crypt::new(generate_identity(0));
        let challenge1 = make_step3_challenge(2, true);
        let first = process_init1(&mut crypt, Some(&challenge1))
            .unwrap()
            .expect("step 4 response");
        let saved_alpha = crypt.alpha_tmp.clone();
        assert_eq!(saved_alpha.len(), 10);

        let repeated = process_init1(&mut crypt, Some(&challenge1))
            .unwrap()
            .expect("cached step 4 response");
        assert_eq!(repeated, first);
        assert_eq!(crypt.alpha_tmp, saved_alpha);

        let challenge2 = make_step3_challenge(3, true);
        let distinct = process_init1(&mut crypt, Some(&challenge2)).unwrap();
        assert!(distinct.is_none());
        assert_eq!(crypt.alpha_tmp, saved_alpha);
    }

    #[test]
    fn rejects_older_steps_and_ignores_init1_after_crypto_init_complete() {
        let mut crypt = Crypt::new(generate_identity(0));

        let mut step2_pkt = vec![0u8; INIT_VERSION_LEN + INIT_TYPE_LEN + 16 + 4];
        step2_pkt[0] = 0x02;
        assert!(
            process_init1(&mut crypt, Some(&step2_pkt))
                .unwrap()
                .is_some()
        );

        let mut step1_pkt = vec![0u8; INIT_STEP_LEN];
        step1_pkt[0] = 0x01;
        assert!(
            process_init1(&mut crypt, Some(&step1_pkt))
                .unwrap()
                .is_none()
        );

        let mut step0_pkt = vec![0u8; INIT_STEP_LEN];
        step0_pkt[0] = 0x00;
        assert!(
            process_init1(&mut crypt, Some(&step0_pkt))
                .unwrap()
                .is_none()
        );

        let challenge = make_step3_challenge(1, true);
        assert!(
            process_init1(&mut crypt, Some(&challenge))
                .unwrap()
                .is_some()
        );
        assert!(
            process_init1(&mut crypt, Some(&step2_pkt))
                .unwrap()
                .is_none()
        );

        // 0x7f resets Init1 state while crypto_init_complete is false.
        assert!(process_init1(&mut crypt, Some(&[0x7f])).unwrap().is_some());
        assert!(
            process_init1(&mut crypt, Some(&step1_pkt))
                .unwrap()
                .is_some()
        );

        crypt.crypto_init_complete = true;
        assert!(process_init1(&mut crypt, None).unwrap().is_none());
        assert!(process_init1(&mut crypt, Some(&[0x7f])).unwrap().is_none());
        assert!(
            process_init1(&mut crypt, Some(&step2_pkt))
                .unwrap()
                .is_none()
        );
        assert!(
            process_init1(&mut crypt, Some(&challenge))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn handles_empty_slice_and_zero_modulus_without_panic() {
        let mut crypt = Crypt::new(generate_identity(0));

        assert!(process_init1(&mut crypt, Some(&[])).unwrap().is_none());

        let zero_mod = make_step3_challenge(1, false);
        assert!(process_init1(&mut crypt, Some(&zero_mod)).is_err());
        assert!(crypt.alpha_tmp.is_empty());
        assert!(crypt.answered_init1_challenge.is_none());
    }
}
