pub fn parse_u64(s: &str) -> u64 {
    if s.is_empty() {
        return 0;
    }
    s.parse::<u64>().unwrap_or(0)
}

pub fn parse_u16(s: &str) -> i32 {
    let v: i32 = s.parse().unwrap_or(0);
    if v < 0 || v > 65535 {
        return 0;
    }
    v
}

pub fn parse_i10(s: &str) -> i32 {
    s.parse().unwrap_or(0)
}

pub fn is_auto_nickname_match(expected: &str, actual: &str) -> bool {
    if actual == expected {
        return true;
    }
    if !actual.starts_with(expected) {
        return false;
    }
    // Use .get() for char-safe slicing — avoids panic if expected.len()
    // falls on a non-char boundary (defensive; starts_with guarantees validity).
    let suffix = actual.get(expected.len()..).unwrap_or("");
    suffix.chars().all(|c| c.is_ascii_digit()) && !suffix.is_empty()
}

pub fn split_command_rows(line: &str) -> Vec<String> {
    if !line.contains('|') {
        return vec![line.to_string()];
    }

    let first_pipe = line.find('|').unwrap_or(line.len());
    let first_segment = &line[..first_pipe];
    let (cmd_name, rest) = match first_segment.find(' ') {
        Some(space_idx) if !first_segment[..space_idx].contains('=') => {
            (Some(&first_segment[..space_idx]), &line[space_idx + 1..])
        }
        Some(_) => (None, line),
        None if first_segment.contains('=') => (None, line),
        None => return vec![line.to_string()],
    };

    let mut inherited: Vec<(String, String)> = Vec::new();
    let mut rows: Vec<String> = Vec::new();

    for part in rest.split('|') {
        let tokens: Vec<&str> = part.split(' ').filter(|t| !t.is_empty()).collect();
        if tokens.is_empty() {
            continue;
        }

        let present_keys: std::collections::HashSet<&str> = tokens
            .iter()
            .map(|token| match token.find('=') {
                Some(idx) => &token[..idx],
                None => *token,
            })
            .collect();

        let mut row_tokens: Vec<&str> = inherited
            .iter()
            .filter(|(k, _)| !present_keys.contains(k.as_str()))
            .map(|(_, token)| token.as_str())
            .collect();
        row_tokens.extend(tokens.iter().copied());

        let body = row_tokens.join(" ");
        match cmd_name {
            Some(name) => rows.push(format!("{name} {body}")),
            None => rows.push(body),
        }

        for token in tokens {
            match token.find('=') {
                Some(idx) => {
                    let key = &token[..idx];
                    if let Some(existing) = inherited.iter_mut().find(|(k, _)| k == key) {
                        existing.1 = token.to_string();
                    } else {
                        inherited.push((key.to_string(), token.to_string()));
                    }
                }
                None => {
                    inherited.retain(|(k, _)| k != token);
                }
            }
        }
    }

    if rows.is_empty() {
        vec![line.to_string()]
    } else {
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u64() {
        assert_eq!(parse_u64(""), 0);
        assert_eq!(parse_u64("42"), 42);
        assert_eq!(parse_u64("0"), 0);
    }

    #[test]
    fn test_parse_u16() {
        assert_eq!(parse_u16("0"), 0);
        assert_eq!(parse_u16("65535"), 65535);
        assert_eq!(parse_u16("65536"), 0);
        assert_eq!(parse_u16("-1"), 0);
    }

    #[test]
    fn test_parse_i10() {
        assert_eq!(parse_i10("42"), 42);
        assert_eq!(parse_i10("0"), 0);
        assert_eq!(parse_i10("-1"), -1);
        assert_eq!(parse_i10("abc"), 0);
    }

    #[test]
    fn test_is_auto_nickname_match() {
        assert!(is_auto_nickname_match("user", "user"));
        assert!(is_auto_nickname_match("user", "user1"));
        assert!(is_auto_nickname_match("user", "user42"));
        assert!(!is_auto_nickname_match("user", "usera"));
        assert!(!is_auto_nickname_match("user", "other"));
    }

    #[test]
    fn test_split_command_rows() {
        let result = split_command_rows("cmd1 arg1|arg2|arg3");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "cmd1 arg1");
        assert_eq!(result[1], "cmd1 arg2");
        assert_eq!(result[2], "cmd1 arg3");
    }

    #[test]
    fn test_split_command_rows_no_pipe() {
        let result = split_command_rows("cmd1 arg1 arg2");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "cmd1 arg1 arg2");
    }

    #[test]
    fn test_split_command_rows_no_args() {
        let result = split_command_rows("cmd1");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "cmd1");
    }

    #[test]
    fn test_split_command_rows_inherits_parameters_and_clears_bare_keys() {
        let result = split_command_rows(
            "notifycliententerview cfid=0 ctid=20 reasonid=2 clid=110 client_flag_avatar=abc client_nickname=A|clid=111 client_flag_avatar client_nickname=B|clid=112 client_nickname=C",
        );
        assert_eq!(
            result,
            vec![
                "notifycliententerview cfid=0 ctid=20 reasonid=2 clid=110 client_flag_avatar=abc client_nickname=A",
                "notifycliententerview cfid=0 ctid=20 reasonid=2 clid=111 client_flag_avatar client_nickname=B",
                "notifycliententerview cfid=0 ctid=20 reasonid=2 clid=112 client_nickname=C",
            ]
        );
    }

    #[test]
    fn test_split_command_rows_without_command_name_inherits_parameters() {
        let result = split_command_rows("clid=110 cid=20|clid=111|cid=30 clid=112");
        assert_eq!(
            result,
            vec!["clid=110 cid=20", "cid=20 clid=111", "cid=30 clid=112",]
        );
    }
}
