use tokio::{
    net::{TcpListener, TcpStream},
    io::{self, AsyncReadExt, AsyncWriteExt},
    time::{sleep, timeout},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Shared runtime state used to decide whether the backend is currently awake.
struct State {
    /// Number of players currently proxied through us.
    active: AtomicUsize,
    /// Unix seconds of the last time a player was connected (0 = never/asleep).
    last_active: AtomicU64,
    /// Seconds after the last player leaves before the container sleeps.
    sleep_timeout: u64,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();

    let backend_addr = std::env::var("BACKEND_ADDR").expect("no BACKEND_ADDR in env");
    let port = std::env::var("PORT").unwrap_or_else(|_| "25565".to_string());
    let motd = std::env::var("MOTD")
        .unwrap_or_else(|_| "Server is asleep \u{2014} join to wake it up!".to_string());
    // Optional: a static MOTD shown while the backend is online. If unset, the
    // proxy relays the backend's real MOTD (live player count/version) instead.
    let motd_online = std::env::var("MOTD_ONLINE").ok();
    // How long the container stays up after the last player leaves. The proxy
    // treats the backend as awake (and safe to probe) for this long after the
    // last activity, matching the container's own idle-sleep timer.
    let sleep_timeout = std::env::var("SLEEP_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(600);
    let bind_addr = format!("0.0.0.0:{}", port);

    println!("Starting proxy on {}", bind_addr);
    println!("Backend: {}", backend_addr);
    println!("Sleep timeout: {}s", sleep_timeout);
    match &motd_online {
        Some(_) => println!("Online MOTD: static (MOTD_ONLINE set)"),
        None => println!("Online MOTD: live passthrough from backend"),
    }

    let listener = TcpListener::bind(&bind_addr).await?;
    let state = Arc::new(State {
        active: AtomicUsize::new(0),
        last_active: AtomicU64::new(0),
        sleep_timeout,
    });
    let motd = Arc::new(motd);
    let motd_online = Arc::new(motd_online);

    {
        let state = state.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            loop {
                let count = state.active.load(Ordering::Acquire);
                if count > 0 {
                    println!("[keepalive] active={}, pinging backend...", count);

                    match TcpStream::connect(&backend_addr).await {
                        Ok(_) => println!("[keepalive] ping success"),
                        Err(e) => println!("[keepalive] ping failed: {}", e),
                    }
                }

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    loop {
        let (client, addr) = listener.accept().await?;
        println!("[conn] new client: {}", addr);

        let state = state.clone();
        let backend_addr = backend_addr.clone();
        let motd = motd.clone();
        let motd_online = motd_online.clone();

        tokio::spawn(async move {
            let res = handle_client(client, &backend_addr, &motd, motd_online.as_deref(), state).await;
            if let Err(e) = res {
                eprintln!("[conn] error from {}: {}", addr, e);
            }
        });
    }
}

async fn handle_client(
    mut client: TcpStream,
    backend_addr: &str,
    motd: &str,
    motd_online: Option<&str>,
    state: Arc<State>,
) -> io::Result<()> {
    // First packet on every connection is the Handshake. Read it whole so we
    // can both inspect `next_state` and replay it verbatim to the backend.
    let handshake = read_packet(&mut client).await?;
    let (protocol, next_state) = parse_handshake(&handshake)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed handshake"))?;

    if next_state == 1 {
        // Status / server-list ping. Never wakes a sleeping backend.
        // The backend is considered awake if a player is connected right now, or
        // if the last player left less than `sleep_timeout` ago (the container
        // hasn't hit its idle-sleep yet). Only then do we probe it.
        if backend_awake(&state) {
            // Fetch the backend's real status so the client sees the live player
            // count/version. If MOTD_ONLINE is set we swap in that description but
            // keep the real numbers; otherwise the backend's status is passed
            // through unchanged.
            println!("[status] backend online, fetching real status from backend");
            match serve_online_status(&mut client, backend_addr, &handshake, motd_online).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Probe failed: the container has actually slept. Mark it
                    // offline so we stop probing until the next real join.
                    println!("[status] backend probe failed ({}), serving sleeping MOTD", e);
                    if state.active.load(Ordering::Acquire) == 0 {
                        state.last_active.store(0, Ordering::Release);
                    }
                }
            }
        } else {
            println!("[status] server-list ping (protocol {}), backend asleep", protocol);
        }
        handle_status(&mut client, motd, protocol).await?;
        return Ok(());
    }

    // next_state == 2 (login) or 3 (transfer): a real join. Wake the backend.
    let current = state.active.fetch_add(1, Ordering::AcqRel) + 1;
    state.last_active.store(now_secs(), Ordering::Release);
    println!("[join] player joining (state {}), active connections: {}", next_state, current);

    let result = proxy_to_backend(client, backend_addr, &handshake).await;

    let current = state.active.fetch_sub(1, Ordering::AcqRel) - 1;
    // Start the idle countdown from the moment this player left.
    state.last_active.store(now_secs(), Ordering::Release);
    println!("[join] connection ended, active: {}", current);

    result
}

/// True if the backend is believed to be awake: either a player is connected
/// right now, or the last player left within the container's sleep timeout.
fn backend_awake(state: &State) -> bool {
    if state.active.load(Ordering::Acquire) > 0 {
        return true;
    }
    let last = state.last_active.load(Ordering::Acquire);
    last != 0 && now_secs().saturating_sub(last) < state.sleep_timeout
}

/// Serve a status ping using the backend's real status (live player count and
/// version). Only called while the backend is believed awake (see
/// `backend_awake`), so the probe should hit a running container rather than
/// wake a sleeping one. If `motd_online` is set, its text replaces the backend's
/// description while the real player count is preserved. Returns an error (so the
/// caller falls back to the offline MOTD) if the backend can't be reached.
async fn serve_online_status(
    client: &mut TcpStream,
    backend_addr: &str,
    handshake: &[u8],
    motd_online: Option<&str>,
) -> io::Result<()> {
    // Consume the client's Status Request (0x00, empty) before replying.
    let _req = read_packet(client).await?;

    let backend_json = fetch_backend_status(backend_addr, handshake).await?;

    // Optionally swap the description for MOTD_ONLINE, keeping real player count.
    let json = match motd_online {
        Some(text) => override_description(&backend_json, text),
        None => backend_json,
    };

    let mut payload = vec![0x00u8]; // Status Response packet id
    payload.extend_from_slice(&frame(json.as_bytes()));
    client.write_all(&frame(&payload)).await?;

    // Echo the Ping Request (0x01 + i64) so the latency bar shows.
    if let Ok(Ok(ping)) = timeout(Duration::from_secs(5), read_packet(client)).await {
        client.write_all(&frame(&ping)).await?;
    }

    Ok(())
}

/// Open a short-lived connection to the backend and perform a status handshake,
/// returning the raw status JSON it reports. Uses short timeouts so a stuck
/// backend doesn't hang the client's server list.
async fn fetch_backend_status(backend_addr: &str, handshake: &[u8]) -> io::Result<String> {
    let mut server = timeout(Duration::from_secs(3), TcpStream::connect(backend_addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status probe timed out"))??;

    // Replay the client's (state 1) handshake, then send our own Status Request.
    server.write_all(&frame(handshake)).await?;
    server.write_all(&frame(&[0x00])).await?;

    // Read the Status Response packet: [0x00, String(VarInt len + JSON)].
    let resp = timeout(Duration::from_secs(5), read_packet(&mut server))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status read timed out"))??;

    let (packet_id, rest) = take_varint(&resp)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad status response"))?;
    if packet_id != 0x00 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected status packet"));
    }
    let (json_len, rest) = take_varint(rest)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad status string"))?;
    let json_len = json_len as usize;
    if rest.len() < json_len {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated status string"));
    }
    Ok(String::from_utf8_lossy(&rest[..json_len]).into_owned())
}

/// Replace the `description` field of a backend status JSON with our own MOTD
/// text (supporting `&` color codes), preserving player count/version/favicon.
/// Falls back to the original JSON if it can't be parsed.
fn override_description(backend_json: &str, motd: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(backend_json) {
        Ok(mut value) => {
            value["description"] =
                serde_json::json!({ "text": translate_colors(motd) });
            value.to_string()
        }
        Err(_) => backend_json.to_string(),
    }
}

async fn proxy_to_backend(
    mut client: TcpStream,
    backend_addr: &str,
    handshake: &[u8],
) -> io::Result<()> {
    println!("[conn] connecting to backend...");
    let mut server = connect_with_retry(backend_addr).await?;
    println!("[conn] backend connected, replaying handshake and proxying");

    // The backend never saw the handshake we consumed, so send it first.
    server.write_all(&frame(handshake)).await?;

    match io::copy_bidirectional(&mut client, &mut server).await {
        Ok((c2s, s2c)) => {
            println!("[conn] closed (client->server {} bytes, server->client {} bytes)", c2s, s2c);
            Ok(())
        }
        Err(e) => {
            println!("[conn] proxy error: {}", e);
            Err(e)
        }
    }
}

/// Handle the status exchange locally so the backend stays off.
/// Flow: client sent Handshake, now sends Status Request (0x00, empty);
/// we reply with Status Response (0x00 + JSON), then echo the Ping (0x01).
async fn handle_status(client: &mut TcpStream, motd: &str, protocol: i32) -> io::Result<()> {
    // Status Request (empty body). Read and discard.
    let _req = read_packet(client).await?;

    // Echo the client's own protocol number so it never shows "incompatible".
    let json = format!(
        "{{\"version\":{{\"name\":\"sleeping\",\"protocol\":{protocol}}},\
          \"players\":{{\"max\":0,\"online\":0,\"sample\":[]}},\
          \"description\":{{\"text\":{motd}}}}}",
        protocol = protocol,
        motd = json_string(&translate_colors(motd)),
    );

    let mut payload = vec![0x00u8]; // Status Response packet id
    payload.extend_from_slice(&frame(json.as_bytes())); // String = VarInt len + UTF-8
    client.write_all(&frame(&payload)).await?;

    // Optional Ping Request (0x01 + i64). Echo it back so the ping bar/latency shows.
    if let Ok(Ok(ping)) = timeout(Duration::from_secs(5), read_packet(client)).await {
        client.write_all(&frame(&ping)).await?;
    }

    Ok(())
}

async fn connect_with_retry(addr: &str) -> io::Result<TcpStream> {
    let deadline = Duration::from_secs(60);

    println!("[retry] trying to connect to {}", addr);

    timeout(deadline, async {
        let mut attempt = 0;

        loop {
            attempt += 1;

            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    println!("[retry] connected after {} attempts", attempt);
                    return Ok(stream);
                }
                Err(e) => {
                    println!("[retry] attempt {} failed: {}", attempt, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await
    .map_err(|_| {
        println!("[retry] timeout reached, backend never woke up");
        io::Error::new(io::ErrorKind::TimedOut, "backend boot timeout")
    })?
}

// ---------- Minecraft protocol helpers ----------

/// Read one length-prefixed packet, returning its body (packet id + data).
async fn read_packet<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    let len = read_varint(r).await?;
    if len < 0 || len > 2 * 1024 * 1024 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad packet length"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Read a VarInt from an async stream.
async fn read_varint<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<i32> {
    let mut num: u32 = 0;
    let mut shift = 0;
    loop {
        let byte = r.read_u8().await?;
        num |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 32 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too big"));
        }
    }
    Ok(num as i32)
}

/// Parse a VarInt out of a byte slice, returning (value, rest).
fn take_varint(mut buf: &[u8]) -> Option<(i32, &[u8])> {
    let mut num: u32 = 0;
    let mut shift = 0;
    loop {
        let (&byte, rest) = buf.split_first()?;
        buf = rest;
        num |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 32 {
            return None;
        }
    }
    Some((num as i32, buf))
}

/// Parse the handshake body, returning (protocol_version, next_state).
fn parse_handshake(body: &[u8]) -> Option<(i32, i32)> {
    let (packet_id, rest) = take_varint(body)?;
    if packet_id != 0x00 {
        return None;
    }
    let (protocol, rest) = take_varint(rest)?;
    let (addr_len, rest) = take_varint(rest)?;
    let addr_len = addr_len as usize;
    if rest.len() < addr_len + 2 {
        return None;
    }
    let rest = &rest[addr_len + 2..]; // skip server_address + server_port (u16)
    let (next_state, _) = take_varint(rest)?;
    Some((protocol, next_state))
}

/// Encode a length prefix + payload into a full packet frame.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = encode_varint(payload.len() as u32);
    out.extend_from_slice(payload);
    out
}

fn encode_varint(mut value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    out
}

/// Translate Bukkit-style `&` color/format codes into the section sign (§)
/// codes the Minecraft protocol actually uses, mirroring Spigot's
/// translateAlternateColorCodes. `&c&l` -> `§c§l`. This also covers Bungee hex
/// codes of the form `&x&r&r&g&g&b&b`. An `&` not followed by a valid code is
/// left untouched, so `&&` and stray ampersands survive.
fn translate_colors(s: &str) -> String {
    const CODES: &str = "0123456789abcdefklmnorxABCDEFKLMNORX";
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '&' {
            if let Some(&next) = chars.peek() {
                if CODES.contains(next) {
                    out.push('\u{00A7}');
                    continue; // the code char itself is pushed next iteration
                }
            }
        }
        out.push(c);
    }
    out
}

/// Minimal JSON string encoder for the MOTD text.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
