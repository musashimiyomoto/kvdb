//! kvdb-client — talks to a kvdb-server over HTTP with Basic auth.
//!
//! Interactive REPL:
//!   kvdb-client [BASE_URL] [--user U] [--password P]
//!
//! One-shot (runs a single command and exits):
//!   kvdb-client [BASE_URL] GET <key>
//!   kvdb-client [BASE_URL] SET <key> <value...>
//!   kvdb-client [BASE_URL] DEL <key>
//!   kvdb-client [BASE_URL] PING
//!
//! BASE_URL defaults to http://127.0.0.1:6380. Credentials come from
//! `--user`/`--password` flags or the KVDB_USER / KVDB_PASSWORD env vars.

use reqwest::{Client, StatusCode};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct Config {
    base_url: String,
    user: String,
    password: String,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let mut raw: Vec<String> = std::env::args().skip(1).collect();

    // Pull optional --user / --password flags from anywhere in the args.
    let user = take_flag(&mut raw, "--user").or_else(|| std::env::var("KVDB_USER").ok());
    let password =
        take_flag(&mut raw, "--password").or_else(|| std::env::var("KVDB_PASSWORD").ok());

    // An optional leading base URL (anything starting with http:// or https://).
    let base_url = if raw.first().map(|a| is_url(a)).unwrap_or(false) {
        raw.remove(0)
    } else {
        "http://127.0.0.1:6380".to_string()
    };

    let cfg = Config {
        base_url: base_url.trim_end_matches('/').to_string(),
        user: user.unwrap_or_default(),
        password: password.unwrap_or_default(),
    };

    let client = Client::new();

    if raw.is_empty() {
        repl(&client, &cfg).await
    } else {
        match run_command(&client, &cfg, &raw).await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(msg) => {
                eprintln!("error: {msg}");
                std::process::ExitCode::FAILURE
            }
        }
    }
}

/// Interactive read-eval-print loop.
async fn repl(client: &Client, cfg: &Config) -> std::process::ExitCode {
    println!(
        "Connected to kvdb at {}. Type HELP for commands, QUIT to exit.",
        cfg.base_url
    );
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    print_prompt().await;
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => break, // EOF (Ctrl-D)
            Err(e) => {
                eprintln!("input error: {e}");
                break;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            print_prompt().await;
            continue;
        }

        let parts: Vec<String> = line.split_whitespace().map(str::to_string).collect();
        match parts[0].to_uppercase().as_str() {
            "QUIT" | "EXIT" => break,
            "HELP" => print_help(),
            _ => {
                if let Err(msg) = run_command(client, cfg, &parts).await {
                    println!("error: {msg}");
                }
            }
        }
        print_prompt().await;
    }
    println!("bye");
    std::process::ExitCode::SUCCESS
}

/// Parses one command and performs the matching HTTP request.
async fn run_command(client: &Client, cfg: &Config, parts: &[String]) -> Result<(), String> {
    let cmd = parts.first().ok_or("empty command")?.to_uppercase();
    match cmd.as_str() {
        "PING" => {
            let (status, body) =
                request(client, cfg, reqwest::Method::GET, "/health", None).await?;
            report(status, &body, "PONG");
            Ok(())
        }
        "GET" => {
            let key = parts.get(1).ok_or("usage: GET <key>")?;
            let path = key_path(key);
            let (status, body) = request(client, cfg, reqwest::Method::GET, &path, None).await?;
            report(status, &body, &body);
            Ok(())
        }
        "SET" => {
            let key = parts.get(1).ok_or("usage: SET <key> <value>")?;
            if parts.len() < 3 {
                return Err("usage: SET <key> <value>".to_string());
            }
            let value = parts[2..].join(" ");
            let path = key_path(key);
            let (status, body) =
                request(client, cfg, reqwest::Method::PUT, &path, Some(value)).await?;
            report(status, &body, "OK");
            Ok(())
        }
        "DEL" | "DELETE" => {
            let key = parts.get(1).ok_or("usage: DEL <key>")?;
            let path = key_path(key);
            let (status, body) = request(client, cfg, reqwest::Method::DELETE, &path, None).await?;
            report(status, &body, "OK");
            Ok(())
        }
        other => Err(format!("unknown command: {other}")),
    }
}

/// Issues an authenticated HTTP request and returns `(status, body_text)`.
async fn request(
    client: &Client,
    cfg: &Config,
    method: reqwest::Method,
    path: &str,
    body: Option<String>,
) -> Result<(StatusCode, String), String> {
    let url = format!("{}{}", cfg.base_url, path);
    let mut req = client
        .request(method, &url)
        .basic_auth(&cfg.user, Some(&cfg.password));
    if let Some(b) = body {
        req = req.body(b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("request to {url} failed: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

/// Prints a friendly line for a response, given the text to show on success.
fn report(status: StatusCode, body: &str, on_ok: &str) {
    match status {
        StatusCode::OK => println!("{}", on_ok.trim_end()),
        StatusCode::NOT_FOUND => println!("(nil)"),
        StatusCode::UNAUTHORIZED => {
            println!("ERROR: unauthorized (check --user/--password or KVDB_USER/KVDB_PASSWORD)")
        }
        other => println!("ERROR: {} {}", other.as_u16(), body.trim_end()),
    }
}

/// Builds the `/v1/keys/<key>` path with the key percent-encoded.
fn key_path(key: &str) -> String {
    format!("/v1/keys/{}", percent_encode(key))
}

/// Percent-encodes a single URL path segment (RFC 3986 unreserved set is kept).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Removes `--flag value` from `args` if present, returning the value.
fn take_flag(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let pos = args.iter().position(|a| a == flag)?;
    if pos + 1 < args.len() {
        let value = args.remove(pos + 1);
        args.remove(pos);
        Some(value)
    } else {
        args.remove(pos);
        None
    }
}

fn is_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

fn print_help() {
    println!(
        "commands:\n  \
         GET <key>            fetch a value\n  \
         SET <key> <value>    store a value (value may contain spaces)\n  \
         DEL <key>            remove a key\n  \
         PING                 health check\n  \
         HELP                 show this help\n  \
         QUIT | EXIT          disconnect"
    );
}

async fn print_prompt() {
    let mut out = tokio::io::stdout();
    let _ = out.write_all(b"kvdb> ").await;
    let _ = out.flush().await;
}
