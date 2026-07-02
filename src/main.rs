use tokio::{
    net::{TcpListener, TcpStream},
    io::{self, AsyncReadExt, AsyncWriteExt},
    time::{sleep, timeout},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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
    let bind_addr = format!("0.0.0.0:{}", port);

    println!("Starting proxy on {}", bind_addr);
    println!("Backend: {}", backend_addr);
    match &motd_online {
        Some(_) => println!("Online MOTD: static (MOTD_ONLINE set)"),
        None => println!("Online MOTD: live passthrough from backend"),
    }

    let listener = TcpListener::bind(&bind_addr).await?;
    let active = Arc::new(AtomicUsize::new(0));
    let motd = Arc::new(motd);
    let motd_online = Arc::new(motd_online);

    {
        let active = active.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            loop {
                let count = active.load(Ordering::Acquire);
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

        let active = active.clone();
        let backend_addr = backend_addr.clone();
        let motd = motd.clone();
        let motd_online = motd_online.clone();

        tokio::spawn(async move {
            let res = handle_client(client, &backend_addr, &motd, motd_online.as_deref(), active).await;
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
    active: Arc<AtomicUsize>,
) -> io::Result<()> {
    // First packet on every connection is the Handshake. Read it whole so we
    // can both inspect `next_state` and replay it verbatim to the backend.
    let handshake = read_packet(&mut client).await?;
    let (protocol, next_state) = parse_handshake(&handshake)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed handshake"))?;

    if next_state == 1 {
        // Status / server-list ping. Never wakes the backend.
        // If a player is connected through us the backend is provably awake, so
        // we can reflect its real state; otherwise it's asleep -> local MOTD.
        if active.load(Ordering::Acquire) > 0 {
            match motd_online {
                // Static online MOTD configured: answer locally, no probe.
                Some(text) => {
                    println!("[status] backend online, serving static MOTD_ONLINE");
                    handle_status(&mut client, text, protocol).await?;
                    return Ok(());
                }
                // Default: relay the backend's real status (live MOTD/count).
                None => {
                    println!("[status] backend online, relaying real MOTD from backend");
                    if relay_status(&mut client, backend_addr, &handshake).await.is_ok() {
                        return Ok(());
                    }
                    println!("[status] relay failed, falling back to sleeping MOTD");
                }
            }
        } else {
            println!("[status] server-list ping (protocol {}), backend asleep", protocol);
        }
        handle_status(&mut client, motd, protocol).await?;
        return Ok(());
    }

    // next_state == 2 (login) or 3 (transfer): a real join. Wake the backend.
    let current = active.fetch_add(1, Ordering::AcqRel) + 1;
    println!("[join] player joining (state {}), active connections: {}", next_state, current);

    let result = proxy_to_backend(client, backend_addr, &handshake).await;

    let current = active.fetch_sub(1, Ordering::AcqRel) - 1;
    println!("[join] connection ended, active: {}", current);

    result
}

/// Relay a status ping to the backend and pipe its real response back to the
/// client. Only called when the backend is known to be online (active > 0), so
/// the connect below does not risk waking a sleeping instance. Uses a short
/// timeout: if the backend doesn't answer quickly, the caller falls back to the
/// local sleeping MOTD rather than hanging the client's server list.
async fn relay_status(
    client: &mut TcpStream,
    backend_addr: &str,
    handshake: &[u8],
) -> io::Result<()> {
    let mut server = timeout(Duration::from_secs(3), TcpStream::connect(backend_addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status probe timed out"))??;

    // Replay the (state 1) handshake, then let the client's Status Request /
    // Ping flow through and the backend's Status Response / Pong flow back.
    server.write_all(&frame(handshake)).await?;
    timeout(Duration::from_secs(5), io::copy_bidirectional(client, &mut server))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status relay timed out"))??;
    Ok(())
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
        motd = json_string(motd),
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
