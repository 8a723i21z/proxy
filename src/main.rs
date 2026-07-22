use tokio::{
    net::{TcpListener, TcpStream},
    io::{self, AsyncReadExt, AsyncWriteExt},
    signal::unix::{signal, SignalKind},
    sync::{Notify, Semaphore},
    time::{sleep, timeout},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// How long a join waits for the backend to be reachable before it gives up and
/// asks the player to rejoin. Only bounds the asleep case; an already-up backend
/// connects instantly.
const QUICK_CONNECT_SECS: u64 = 5;

/// After closing a player's previous session, how long to wait before letting
/// their new login reach the backend, so the server registers the disconnect
/// first and never fires "logged in from another location".
const SESSION_HANDOFF_MS: u64 = 750;

/// How often we nudge a held client while the backend boots. The vanilla client
/// drops a login that receives nothing for ~30s, so stay well under that.
const HOLD_PING_SECS: u64 = 12;

/// How long to wait for the client's answer to a hold ping before concluding we
/// can't hold it safely.
const HOLD_ANSWER_SECS: u64 = 10;

/// Channel used for the hold's Login Plugin Requests. A namespace no mod or
/// client implements guarantees the answer is a plain "unknown channel".
const HOLD_CHANNEL: &str = "sleepproxy:hold";

/// First protocol version with Login Plugin Request (1.13). Older clients have
/// no packet we can send mid-login, so they get the startup message instead.
const PROTO_LOGIN_PLUGIN: i32 = 393;

/// Maximum size of any pre-play packet we read and buffer ourselves (handshake,
/// login start, status request). The largest legitimate one (login start with
/// signature data) is ~2KB; this cap stops attackers making us allocate big
/// buffers per connection. Play-phase traffic is raw-copied and unaffected.
const MAX_PREPLAY_PACKET: i32 = 64 * 1024;

/// Maximum size of a backend status response we read (may embed a base64
/// favicon, so it can legitimately reach tens of KB).
const MAX_STATUS_PACKET: i32 = 256 * 1024;

/// On SIGTERM (Railway redeploy/stop), how long to keep serving existing
/// sessions before exiting so players aren't cut mid-write.
const DRAIN_SECS: u64 = 25;

/// Shared runtime state used to decide whether the backend is currently awake.
struct State {
    /// Number of players currently proxied through us.
    active: AtomicUsize,
    /// Unix seconds of the last time a player was connected (0 = never/asleep).
    last_active: AtomicU64,
    /// Seconds after the last player leaves during which the online MOTD keeps
    /// showing (from cache/config, with 0 players); after this it goes offline.
    sleep_timeout: u64,
    /// Fallback max-players for any status we build ourselves (online MOTD and
    /// the sleeping MOTD) when the backend's real cap hasn't been cached yet.
    max_players: u64,
    /// How long to keep retrying the backend connection on a join before giving up.
    connect_timeout: u64,
    /// How long to hold a joining player on the connecting screen while the
    /// backend boots, before falling back to the startup message. 0 disables.
    join_hold: u64,
    /// True while a background task is booting the backend, to avoid piling up.
    waking: AtomicBool,
    /// Last status JSON fetched from the backend while a player was connected.
    /// Served (without probing) during the post-disconnect grace window so the
    /// online MOTD keeps showing without resetting the container's idle timer.
    cached_status: Mutex<Option<String>>,
    /// Live sessions keyed by username, each with a handle to cancel it. Lets a
    /// reconnect close the player's previous session before the new login lands.
    sessions: Mutex<HashMap<String, Arc<Notify>>>,
    /// Protocol version from the most recent client handshake, echoed in the
    /// keepalive's status pings so they look like a real client to the backend.
    last_protocol: AtomicI64,
    /// Optional favicon (a "data:image/png;base64,..." URI) for locally-built
    /// statuses. The backend's own favicon passes through when cached.
    favicon: Option<String>,
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
    // How long to keep retrying the backend connection while it boots, on a join.
    let connect_timeout = std::env::var("CONNECT_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(120);
    // How long a joining player is held on the connecting screen while the
    // backend boots, instead of being bounced with the startup message. Set to 0
    // to disconnect immediately (pre-hold behaviour).
    let join_hold = std::env::var("JOIN_HOLD_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(90);
    // Fallback max-players shown on the online and sleeping MOTDs before the
    // backend's real cap has been cached.
    let max_players = std::env::var("MAX_PLAYERS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(20);
    // Message shown to a player who joins while the backend is still booting.
    // Supports the same & color/format/hex codes and JSON as MOTD.
    let startup_message = std::env::var("STARTUP_MESSAGE").unwrap_or_else(|_| {
        "&e&lServer is starting up!\n&7Please rejoin in a few seconds.".to_string()
    });
    // Optional favicon for locally-built statuses (data:image/png;base64,...).
    let favicon = std::env::var("FAVICON").ok().filter(|s| !s.is_empty());
    // Cap on simultaneously open client connections (players + pings + scanners).
    // Protects fds/memory so a connection flood can't take the proxy down.
    let max_connections = std::env::var("MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(256);
    let bind_addr = format!("0.0.0.0:{}", port);

    println!("Starting proxy on {}", bind_addr);
    println!("Backend: {}", backend_addr);
    println!("Online MOTD window: {}s", sleep_timeout);
    println!("Connect timeout: {}s", connect_timeout);
    println!("Quick-connect window: {}s", QUICK_CONNECT_SECS);
    match join_hold {
        0 => println!("Join hold: off (cold joins get the startup message)"),
        secs => println!("Join hold: up to {}s on the connecting screen", secs),
    }
    match &motd_online {
        Some(_) => println!("Online MOTD: static (MOTD_ONLINE set)"),
        None => println!("Online MOTD: live passthrough from backend"),
    }

    let listener = TcpListener::bind(&bind_addr).await?;
    let state = Arc::new(State {
        active: AtomicUsize::new(0),
        last_active: AtomicU64::new(0),
        sleep_timeout,
        max_players,
        connect_timeout,
        join_hold,
        waking: AtomicBool::new(false),
        cached_status: Mutex::new(None),
        sessions: Mutex::new(HashMap::new()),
        last_protocol: AtomicI64::new(0),
        favicon,
    });
    let motd = Arc::new(motd);
    let motd_online = Arc::new(motd_online);
    let startup_message = Arc::new(startup_message);

    {
        let state = state.clone();
        let backend_addr = backend_addr.clone();

        // While players are connected, ping the backend with a REAL status
        // handshake every minute. A bare TCP connect is not enough: the
        // container host only counts Minecraft handshakes as activity, so a
        // quietly playing player (whose last handshake was their join) would
        // hit the idle timer and the container would stop mid-session,
        // resetting every connection.
        tokio::spawn(async move {
            loop {
                let count = state.active.load(Ordering::Acquire);
                if count > 0 {
                    let protocol = state.last_protocol.load(Ordering::Acquire) as i32;
                    let handshake = build_status_handshake(&backend_addr, protocol);
                    match fetch_backend_status(&backend_addr, &handshake).await {
                        Ok(json) => {
                            println!("[keepalive] active={}, status ping ok", count);
                            if let Ok(mut cache) = state.cached_status.lock() {
                                *cache = Some(json);
                            }
                        }
                        Err(e) => {
                            println!("[keepalive] active={}, status ping failed: {}", count, e)
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    let conn_limit = Arc::new(Semaphore::new(max_connections));
    let mut sigterm = signal(SignalKind::terminate())?;

    // Accept loop. Two availability rules:
    //  - accept() errors (EMFILE, ECONNABORTED, ...) are transient conditions,
    //    never a reason to exit: log, back off briefly, keep serving.
    //  - SIGTERM (redeploy/stop) stops accepting but drains active sessions
    //    for up to DRAIN_SECS so players aren't cut off mid-write.
    loop {
        let accepted = tokio::select! {
            _ = sigterm.recv() => break,
            accepted = listener.accept() => accepted,
        };

        let (client, addr) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("[conn] accept error: {} (continuing)", e);
                sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let Ok(permit) = conn_limit.clone().try_acquire_owned() else {
            // At capacity: shed the newcomer instead of degrading everyone.
            eprintln!("[conn] connection limit ({}) reached, dropping {}", max_connections, addr);
            continue;
        };

        println!("[conn] new client: {}", addr);
        // Match vanilla client/server behavior; without this, Nagle adds up to
        // ~40ms of buffering per hop on this latency-sensitive path.
        client.set_nodelay(true).ok();

        let state = state.clone();
        let backend_addr = backend_addr.clone();
        let motd = motd.clone();
        let motd_online = motd_online.clone();
        let startup_message = startup_message.clone();

        tokio::spawn(async move {
            let _permit = permit; // released when the connection task ends
            let res = handle_client(
                client,
                &backend_addr,
                &motd,
                motd_online.as_deref(),
                &startup_message,
                state,
            )
            .await;
            if let Err(e) = res {
                eprintln!("[conn] error from {}: {}", addr, e);
            }
        });
    }

    // SIGTERM received: drain. New connections are no longer accepted (the
    // replacement deployment owns those); existing sessions ride out the grace
    // period. The keepalive task keeps running so the backend stays awake for
    // the players still connected.
    println!("[shutdown] SIGTERM received, draining active sessions (max {}s)", DRAIN_SECS);
    let deadline = Instant::now() + Duration::from_secs(DRAIN_SECS);
    while state.active.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
        sleep(Duration::from_millis(500)).await;
    }
    println!(
        "[shutdown] exiting (active sessions remaining: {})",
        state.active.load(Ordering::Acquire)
    );
    Ok(())
}

async fn handle_client(
    mut client: TcpStream,
    backend_addr: &str,
    motd: &str,
    motd_online: Option<&str>,
    startup_message: &str,
    state: Arc<State>,
) -> io::Result<()> {
    // First packet on every connection is the Handshake. Read it whole so we
    // can both inspect `next_state` and replay it verbatim to the backend.
    // Bounded by a timeout so port scanners / health checks that connect and
    // send nothing don't park this task (and two fds) forever.
    let first = match timeout(Duration::from_secs(10), client.read_u8()).await {
        Ok(Ok(byte)) => byte,
        Ok(Err(_)) | Err(_) => return Ok(()), // silent/vanished connection; drop quietly
    };

    // Legacy server-list ping (pre-1.7 clients and some scanners, first byte
    // 0xFE). It isn't length-prefixed, so answer it in the legacy format and
    // close instead of misparsing it as a VarInt.
    if first == 0xFE {
        let online = state.active.load(Ordering::Acquire);
        let max = display_max(&state);
        let _ = send_legacy_status(&mut client, motd, online, max).await;
        return Ok(());
    }

    let handshake = match timeout(
        Duration::from_secs(10),
        read_packet_after(first, &mut client, MAX_PREPLAY_PACKET),
    )
    .await
    {
        Ok(packet) => packet?,
        Err(_) => return Ok(()),
    };
    let (protocol, next_state) = parse_handshake(&handshake)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed handshake"))?;
    state.last_protocol.store(protocol as i64, Ordering::Release);

    if next_state == 1 {
        // Status / server-list ping. Consume the client's next packet (same
        // timeout rationale as the handshake above).
        let request = match timeout(
            Duration::from_secs(10),
            read_packet_max(&mut client, MAX_PREPLAY_PACKET),
        )
        .await
        {
            Ok(req) => req?,
            Err(_) => return Ok(()),
        };

        // Some clients skip the Status Request and send the Ping Request
        // (0x01 + i64) directly to measure latency; echo it straight back.
        if request.first() == Some(&0x01) {
            client.write_all(&frame(&request)).await?;
            return Ok(());
        }

        // The online player count is always the proxy's own live connection
        // count (every player flows through us), so it's accurate without asking
        // the backend.
        let online = state.active.load(Ordering::Acquire);
        let since = secs_since_active(&state);

        let cached = || state.cached_status.lock().ok().and_then(|c| c.clone());

        let json = if online > 0 {
            // A player is connected. Serve from the cache when we have one (the
            // join-time seed and the 60s keepalive keep it fresh) so list pings
            // don't open a backend connection each; probe only when the cache is
            // still empty. Count always comes from the proxy.
            match cached() {
                Some(backend_json) => build_online_json(&backend_json, motd_online, online),
                None => {
                    println!("[status] player online, no cache yet; probing backend");
                    match fetch_backend_status(backend_addr, &handshake).await {
                        Ok(backend_json) => {
                            if let Ok(mut cache) = state.cached_status.lock() {
                                *cache = Some(backend_json.clone());
                            }
                            build_online_json(&backend_json, motd_online, online)
                        }
                        Err(e) => {
                            println!("[status] probe failed ({}), using config", e);
                            online_or_offline_json(None, motd_online, online, &state, protocol, motd)
                        }
                    }
                }
            }
        } else if since.is_some_and(|s| s < state.sleep_timeout) {
            // Idle but within the display window: show the online MOTD from cache
            // or MOTD_ONLINE with 0 players, and NEVER touch the backend so the
            // container can sleep on its own schedule.
            println!("[status] idle within window, serving online MOTD (no probe)");
            online_or_offline_json(cached(), motd_online, 0, &state, protocol, motd)
        } else {
            println!("[status] idle past window, serving offline MOTD");
            offline_status_json(motd, protocol, display_max(&state), state.favicon.as_deref())
        };

        send_status(&mut client, &json).await?;
        return Ok(());
    }

    // next_state == 2 (login) or 3 (transfer): a real join. The client's next
    // packet is Login Start; read it so we can learn the username (for
    // single-session enforcement) and replay it to the backend.
    let login_start = match timeout(
        Duration::from_secs(10),
        read_packet_max(&mut client, MAX_PREPLAY_PACKET),
    )
    .await
    {
        Ok(Ok(packet)) => packet,
        Ok(Err(e)) => {
            eprintln!("[join] failed to read login start: {}", e);
            return Ok(());
        }
        Err(_) => {
            eprintln!("[join] timed out reading login start");
            return Ok(());
        }
    };
    let username = parse_login_username(&login_start);

    let current = state.active.fetch_add(1, Ordering::AcqRel) + 1;
    state.last_active.store(now_secs(), Ordering::Release);
    println!(
        "[join] {} joining (state {}), active connections: {}",
        username.as_deref().unwrap_or("<unknown>"),
        next_state,
        current
    );

    let result = handle_join(
        client,
        backend_addr,
        &handshake,
        &login_start,
        username.as_deref(),
        protocol,
        &state,
        startup_message,
    )
    .await;

    let current = state.active.fetch_sub(1, Ordering::AcqRel) - 1;
    // Start the idle countdown from the moment this player left.
    state.last_active.store(now_secs(), Ordering::Release);
    println!("[join] connection ended, active: {}", current);

    result
}

/// Handle a join. If the backend is reachable quickly, proxy straight through.
/// Otherwise trigger its boot and hold the player on the connecting screen until
/// it comes up (see `hold_for_backend`), falling back to a polite "starting up"
/// disconnect if it never does, or if the client is too old to be held.
async fn handle_join(
    mut client: TcpStream,
    backend_addr: &str,
    handshake: &[u8],
    login_start: &[u8],
    username: Option<&str>,
    protocol: i32,
    state: &Arc<State>,
    startup_message: &str,
) -> io::Result<()> {
    if let Ok(Ok(server)) =
        timeout(Duration::from_secs(QUICK_CONNECT_SECS), TcpStream::connect(backend_addr)).await
    {
        println!("[join] backend reachable, proxying");
        return proxy_session(client, server, backend_addr, handshake, login_start, username, state)
            .await;
    }

    // Backend asleep or still booting: start the boot, then try to keep the
    // player parked on the connecting screen until it finishes.
    println!("[join] backend not ready, waking it");
    ensure_waking(backend_addr, state);

    if state.join_hold > 0 {
        if protocol >= PROTO_LOGIN_PLUGIN {
            match hold_for_backend(&mut client, backend_addr, state.join_hold).await {
                Ok(Some(server)) => {
                    return proxy_session(
                        client,
                        server,
                        backend_addr,
                        handshake,
                        login_start,
                        username,
                        state,
                    )
                    .await;
                }
                // Never came up (or we lost confidence in the hold): fall through
                // to the startup message so the player still gets an explanation.
                Ok(None) => println!("[hold] giving up, sending startup message"),
                // Client is gone; writing anything more is pointless.
                Err(e) => {
                    println!("[hold] client left during hold: {}", e);
                    return Ok(());
                }
            }
        } else {
            println!("[hold] client protocol {} is pre-1.13, can't be held", protocol);
        }
    }

    send_login_disconnect(&mut client, startup_message).await
}

/// Replay the consumed handshake/login start to a live backend and pipe the two
/// sockets together for the rest of the session.
async fn proxy_session(
    mut client: TcpStream,
    mut server: TcpStream,
    backend_addr: &str,
    handshake: &[u8],
    login_start: &[u8],
    username: Option<&str>,
    state: &Arc<State>,
) -> io::Result<()> {
    server.set_nodelay(true).ok();

    // Seed the status cache in the background if it's empty, so the online MOTD
    // shows the backend's real max/favicon right away instead of the MAX_PLAYERS
    // fallback until the first keepalive.
    if state.cached_status.lock().map(|c| c.is_none()).unwrap_or(false) {
        let addr = backend_addr.to_string();
        let st = state.clone();
        tokio::spawn(async move {
            let protocol = st.last_protocol.load(Ordering::Acquire) as i32;
            let hs = build_status_handshake(&addr, protocol);
            if let Ok(json) = fetch_backend_status(&addr, &hs).await {
                if let Ok(mut cache) = st.cached_status.lock() {
                    cache.get_or_insert(json);
                }
            }
        });
    }

    // Single-session enforcement: if this account already has a live
    // session, cancel it and give the backend a moment to register the
    // disconnect BEFORE this login lands — so the server never fires
    // "logged in from another location" when a player reconnects.
    let cancel = Arc::new(Notify::new());
    if let Some(name) = username {
        let previous = state
            .sessions
            .lock()
            .ok()
            .and_then(|mut sessions| sessions.insert(name.to_string(), cancel.clone()));
        if let Some(prev) = previous {
            println!("[join] {} reconnected; closing previous session first", name);
            prev.notify_one();
            sleep(Duration::from_millis(SESSION_HANDOFF_MS)).await;
        }
    }

    // The backend never saw the handshake or login start we consumed.
    server.write_all(&frame(handshake)).await?;
    server.write_all(&frame(login_start)).await?;

    // Pipe both directions, tearing down BOTH when either side ends OR
    // this session is superseded by a newer login for the same account.
    // (copy_bidirectional would keep the other half-open until both
    // sides close, leaving the backend session and our `active` count
    // hanging after a client drops.)
    let name = username.unwrap_or("<unknown>");
    let started = std::time::Instant::now();
    let (mut cr, mut cw) = client.split();
    let (mut sr, mut sw) = server.split();
    let client_to_server = async {
        let r = io::copy(&mut cr, &mut sw).await;
        let _ = sw.shutdown().await;
        r
    };
    let server_to_client = async {
        let r = io::copy(&mut sr, &mut cw).await;
        let _ = cw.shutdown().await;
        r
    };
    enum SessionEnd {
        Client(io::Result<u64>),
        Backend(io::Result<u64>),
        Superseded,
    }
    let end = tokio::select! {
        r = client_to_server => SessionEnd::Client(r),
        r = server_to_client => SessionEnd::Backend(r),
        _ = cancel.notified() => SessionEnd::Superseded,
    };

    // Log exactly which leg ended the session and how, so a disconnect
    // can be attributed to the client<->proxy path or the
    // proxy<->backend path from this one line.
    let secs = started.elapsed().as_secs();
    match &end {
        SessionEnd::Client(Ok(n)) => println!(
            "[conn] {}: client closed cleanly after {}s ({} bytes c->s); closing backend",
            name, secs, n
        ),
        SessionEnd::Client(Err(e)) => println!(
            "[conn] {}: CLIENT-LEG error after {}s: {} (drop came from client<->proxy path)",
            name, secs, e
        ),
        SessionEnd::Backend(Ok(n)) => println!(
            "[conn] {}: backend closed cleanly after {}s ({} bytes s->c); closing client",
            name, secs, n
        ),
        SessionEnd::Backend(Err(e)) => println!(
            "[conn] {}: BACKEND-LEG error after {}s: {} (drop came from proxy<->backend path)",
            name, secs, e
        ),
        SessionEnd::Superseded => println!(
            "[conn] {}: superseded by a newer session after {}s; closing both",
            name, secs
        ),
    }

    // Graceful teardown. Half-close our write side toward both peers
    // (FIN), then briefly drain leftover inbound bytes. Dropping a
    // socket with unread data in its buffer makes the kernel send RST,
    // and an RST discards data still in flight to the peer — e.g. the
    // server's final disconnect screen — showing up client-side as
    // "Connection reset by peer" instead of a clean disconnect.
    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
    let _ = timeout(Duration::from_millis(500), async {
        let mut cbuf = [0u8; 8192];
        let mut sbuf = [0u8; 8192];
        let drain_client = async {
            loop {
                match client.read(&mut cbuf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        };
        let drain_server = async {
            loop {
                match server.read(&mut sbuf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        };
        tokio::join!(drain_client, drain_server);
    })
    .await;

    // Deregister, but only if we're still the current session for this
    // name (a newer session may have replaced our map entry already).
    if let Some(name) = username {
        if let Ok(mut sessions) = state.sessions.lock() {
            if sessions.get(name).is_some_and(|c| Arc::ptr_eq(c, &cancel)) {
                sessions.remove(name);
            }
        }
    }
    Ok(())
}


/// Parse the username out of a Login Start packet (its first field is always a
/// String, across protocol versions). Returns None if it doesn't look like one.
fn parse_login_username(login_start: &[u8]) -> Option<String> {
    let (packet_id, rest) = take_varint(login_start)?;
    if packet_id != 0x00 {
        return None;
    }
    let (len, rest) = take_varint(rest)?;
    let len = len as usize;
    if len == 0 || len > 48 || rest.len() < len {
        return None;
    }
    Some(String::from_utf8_lossy(&rest[..len]).into_owned())
}

/// Spawn a background task that boots the backend, unless one is already running.
/// The connection is only used to wake the container and is dropped once up.
fn ensure_waking(backend_addr: &str, state: &Arc<State>) {
    if state.waking.swap(true, Ordering::AcqRel) {
        return; // a wake task is already in progress
    }
    let backend_addr = backend_addr.to_string();
    let state = state.clone();
    tokio::spawn(async move {
        println!("[wake] triggering backend boot...");
        match connect_with_retry(&backend_addr, state.connect_timeout).await {
            Ok(_) => println!("[wake] backend is up"),
            Err(e) => println!("[wake] backend failed to wake: {}", e),
        }
        state.waking.store(false, Ordering::Release);
    });
}

/// Hold a joining client on the connecting screen while the backend boots,
/// instead of bouncing it with "rejoin in a few seconds".
///
/// The vanilla client kills a login that receives nothing for ~30s, which is
/// shorter than a cold start, so simply waiting would time out. Any inbound
/// packet resets that timer, so we send a Login Plugin Request (1.13+) on a
/// private channel every `HOLD_PING_SECS` and swallow the client's "unknown
/// channel" answer. None of that advances the login state machine: when the
/// backend finally accepts, the caller replays the handshake and login start and
/// the real login runs end-to-end — encryption and Mojang auth included, exactly
/// as if the player had connected to an already-running server.
///
/// Returns the backend connection once it accepts one, `Ok(None)` if it never
/// did within `hold_secs` (caller should send the startup message), or `Err` if
/// the client hung up (caller should just close).
async fn hold_for_backend(
    client: &mut TcpStream,
    backend_addr: &str,
    hold_secs: u64,
) -> io::Result<Option<TcpStream>> {
    let started = Instant::now();
    let hold = Duration::from_secs(hold_secs);
    let mut last_ping = Instant::now();
    let mut message_id: u32 = 0;

    println!("[hold] holding client up to {}s while the backend boots", hold_secs);

    while started.elapsed() < hold {
        if let Ok(Ok(server)) =
            timeout(Duration::from_secs(2), TcpStream::connect(backend_addr)).await
        {
            println!(
                "[hold] backend accepted after {}s, resuming this player's login",
                started.elapsed().as_secs()
            );
            return Ok(Some(server));
        }

        if last_ping.elapsed() >= Duration::from_secs(HOLD_PING_SECS) {
            message_id = message_id.wrapping_add(1);
            // A failed write means the client is gone: stop holding for a ghost.
            send_login_plugin_request(client, message_id).await?;
            // Consume the answer before looping. The login stream has to be clean
            // the moment we hand it to the backend, or the server sees an
            // unexpected packet and kicks the player.
            if !read_query_answer(client).await? {
                println!("[hold] client didn't answer the hold ping");
                return Ok(None);
            }
            last_ping = Instant::now();
        }

        sleep(Duration::from_secs(1)).await;
    }

    println!("[hold] backend still down after {}s", hold_secs);
    Ok(None)
}

/// Send a clientbound Login Plugin Request (0x04) on a channel nothing
/// implements. Vanilla answers "unknown channel" and stays put in the login
/// phase, which is exactly what the hold needs: inbound traffic that resets the
/// client's read timeout without changing any state.
async fn send_login_plugin_request(client: &mut TcpStream, message_id: u32) -> io::Result<()> {
    let mut payload = vec![0x04u8]; // Login Plugin Request packet id
    payload.extend_from_slice(&encode_varint(message_id));
    payload.extend_from_slice(&frame(HOLD_CHANNEL.as_bytes())); // Channel: Identifier
    // Data is the rest of the packet, and we send none.
    client.write_all(&frame(&payload)).await
}

/// Wait for the client's answer to a hold ping and discard it. `Ok(true)` means
/// it answered and the stream is back at a packet boundary; `Ok(false)` means it
/// didn't in time, so the hold can't safely continue (the read may have stopped
/// mid-packet, making the stream unsafe to splice onto the backend — the caller
/// only writes a disconnect after this); `Err` means the client is gone.
async fn read_query_answer(client: &mut TcpStream) -> io::Result<bool> {
    match timeout(
        Duration::from_secs(HOLD_ANSWER_SECS),
        read_packet_max(client, MAX_PREPLAY_PACKET),
    )
    .await
    {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) => Err(e),
        Err(_) => Ok(false),
    }
}

/// Send a clientbound Login Disconnect (0x00) with a styled reason, so the
/// player sees a friendly message instead of a raw timeout.
async fn send_login_disconnect(client: &mut TcpStream, message: &str) -> io::Result<()> {
    let json = motd_component(message).to_string();
    let mut payload = vec![0x00u8]; // Login Disconnect packet id
    payload.extend_from_slice(&frame(json.as_bytes())); // Reason: String (chat JSON)
    client.write_all(&frame(&payload)).await?;
    Ok(())
}

/// Seconds since a player was last connected, or None if none ever were
/// (or the backend was marked asleep).
fn secs_since_active(state: &State) -> Option<u64> {
    let last = state.last_active.load(Ordering::Acquire);
    if last == 0 {
        None
    } else {
        Some(now_secs().saturating_sub(last))
    }
}

/// Send a Status Response (0x00 + JSON) to the client, then echo its Ping
/// Request (0x01) so the latency bar shows.
async fn send_status(client: &mut TcpStream, json: &str) -> io::Result<()> {
    let mut payload = vec![0x00u8]; // Status Response packet id
    payload.extend_from_slice(&frame(json.as_bytes())); // String = VarInt len + UTF-8
    client.write_all(&frame(&payload)).await?;

    if let Ok(Ok(ping)) = timeout(
        Duration::from_secs(5),
        read_packet_max(client, MAX_PREPLAY_PACKET),
    )
    .await
    {
        client.write_all(&frame(&ping)).await?;
    }
    Ok(())
}

/// Answer a legacy (pre-1.7, 0xFE) server-list ping: a 0xFF packet holding a
/// UTF-16BE string of null-separated fields. Protocol 127 marks us as a modern
/// server so legacy clients see the MOTD but "outdated server" for joining.
async fn send_legacy_status(
    client: &mut TcpStream,
    motd: &str,
    online: usize,
    max: u64,
) -> io::Result<()> {
    let text = plain_motd_text(motd);
    let payload = format!("\u{00A7}1\0127\0\0{}\0{}\0{}", text, online, max);
    let utf16: Vec<u16> = payload.encode_utf16().collect();
    let mut out = vec![0xFFu8];
    out.extend_from_slice(&(utf16.len() as u16).to_be_bytes());
    for unit in utf16 {
        out.extend_from_slice(&unit.to_be_bytes());
    }
    client.write_all(&out).await
}

/// Flatten a MOTD (any supported format) to plain text for the legacy ping.
fn plain_motd_text(motd: &str) -> String {
    fn collect(value: &serde_json::Value, out: &mut String) {
        match value {
            serde_json::Value::String(s) => out.push_str(s),
            serde_json::Value::Object(obj) => {
                if let Some(serde_json::Value::String(s)) = obj.get("text") {
                    out.push_str(s);
                }
                if let Some(serde_json::Value::Array(extra)) = obj.get("extra") {
                    for part in extra {
                        collect(part, out);
                    }
                }
            }
            serde_json::Value::Array(parts) => {
                for part in parts {
                    collect(part, out);
                }
            }
            _ => {}
        }
    }
    let mut out = String::new();
    collect(&motd_component(motd), &mut out);
    // Null bytes are field separators in the legacy format; newlines don't render.
    out.replace(['\0', '\n'], " ")
}

/// The local "sleeping" status JSON, echoing the client's protocol so it never
/// shows as incompatible. `max` is advertised as the slot cap so a sleeping
/// server reads as "0 / 20" in the list rather than a suspicious "0 / 0".
fn offline_status_json(motd: &str, protocol: i32, max: u64, favicon: Option<&str>) -> String {
    let mut status = serde_json::json!({
        "version": { "name": "sleeping", "protocol": protocol },
        "players": { "max": max, "online": 0, "sample": [] },
        "description": motd_component(motd),
    });
    if let Some(icon) = favicon {
        status["favicon"] = serde_json::Value::from(icon);
    }
    status.to_string()
}

/// Max-players to advertise on any status we build ourselves: the backend's own
/// cap from the last cached status when we have one, else the MAX_PLAYERS
/// fallback. Never 0, so the slot count stays plausible while the backend sleeps.
fn display_max(state: &State) -> u64 {
    state
        .cached_status
        .lock()
        .ok()
        .and_then(|cache| cache.clone())
        .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
        .and_then(|value| value["players"]["max"].as_u64())
        .filter(|max| *max > 0)
        .unwrap_or(state.max_players)
}

/// Serve an online-looking status without probing the backend: prefer the cached
/// backend status, else synthesize one from MOTD_ONLINE, else fall back to the
/// offline MOTD. `online` is the (proxy-tracked) player count to display.
fn online_or_offline_json(
    cached: Option<String>,
    motd_online: Option<&str>,
    online: usize,
    state: &State,
    protocol: i32,
    offline_motd: &str,
) -> String {
    if let Some(backend_json) = cached {
        build_online_json(&backend_json, motd_online, online)
    } else if let Some(text) = motd_online {
        synth_online_json(text, display_max(state), online, protocol, state.favicon.as_deref())
    } else {
        offline_status_json(offline_motd, protocol, display_max(state), state.favicon.as_deref())
    }
}

/// Synthesize an online status JSON purely from config (no backend data), for
/// when the online MOTD is needed but nothing has been cached yet.
fn synth_online_json(
    description: &str,
    max: u64,
    online: usize,
    protocol: i32,
    favicon: Option<&str>,
) -> String {
    let mut status = serde_json::json!({
        "version": { "name": "online", "protocol": protocol },
        "players": { "max": max, "online": online, "sample": [] },
        "description": motd_component(description),
    });
    if let Some(icon) = favicon {
        status["favicon"] = serde_json::Value::from(icon);
    }
    status.to_string()
}

/// Build an online status JSON from the backend's status. Optionally swaps the
/// description for MOTD_ONLINE, and sets the online player count to `online`
/// (the proxy's own live count). Falls back to the raw backend JSON if it can't
/// be parsed.
fn build_online_json(backend_json: &str, motd_online: Option<&str>, online: usize) -> String {
    match serde_json::from_str::<serde_json::Value>(backend_json) {
        Ok(mut value) => {
            if let Some(text) = motd_online {
                value["description"] = motd_component(text);
            }
            if let Some(players) = value.get_mut("players") {
                players["online"] = serde_json::Value::from(online);
            }
            value.to_string()
        }
        Err(_) => backend_json.to_string(),
    }
}

/// Build a state-1 (status) handshake body addressed to `addr` ("host:port"),
/// exactly as a vanilla client pinging the backend directly would send it.
fn build_status_handshake(addr: &str, protocol: i32) -> Vec<u8> {
    let (host, port) = match addr.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<u16>().unwrap_or(25565)),
        None => (addr, 25565),
    };
    let mut body = vec![0x00u8]; // Handshake packet id
    body.extend_from_slice(&encode_varint(protocol as u32));
    body.extend_from_slice(&encode_varint(host.len() as u32));
    body.extend_from_slice(host.as_bytes());
    body.extend_from_slice(&port.to_be_bytes());
    body.push(0x01); // next_state = 1 (status)
    body
}

/// Open a short-lived connection to the backend and perform a status handshake,
/// returning the raw status JSON it reports. Uses short timeouts so a stuck
/// backend doesn't hang the client's server list.
async fn fetch_backend_status(backend_addr: &str, handshake: &[u8]) -> io::Result<String> {
    let mut server = timeout(Duration::from_secs(3), TcpStream::connect(backend_addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status probe timed out"))??;
    server.set_nodelay(true).ok();

    // Replay the client's (state 1) handshake, then send our own Status Request.
    server.write_all(&frame(handshake)).await?;
    server.write_all(&frame(&[0x00])).await?;

    // Read the Status Response packet: [0x00, String(VarInt len + JSON)].
    let resp = timeout(Duration::from_secs(5), read_packet_max(&mut server, MAX_STATUS_PACKET))
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

async fn connect_with_retry(addr: &str, deadline_secs: u64) -> io::Result<TcpStream> {
    let deadline = Duration::from_secs(deadline_secs);

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
/// `max` bounds both the declared length and the allocation, so a hostile
/// peer can't make us allocate large buffers on demand.
async fn read_packet_max<R: AsyncReadExt + Unpin>(r: &mut R, max: i32) -> io::Result<Vec<u8>> {
    let len = read_varint(r).await?;
    read_packet_body(r, len, max).await
}

/// Like `read_packet_max`, but the first byte of the length VarInt was already
/// consumed by the caller (used for legacy-ping detection on the first byte).
async fn read_packet_after<R: AsyncReadExt + Unpin>(
    first: u8,
    r: &mut R,
    max: i32,
) -> io::Result<Vec<u8>> {
    let len = read_varint_cont(first, r).await?;
    read_packet_body(r, len, max).await
}

async fn read_packet_body<R: AsyncReadExt + Unpin>(
    r: &mut R,
    len: i32,
    max: i32,
) -> io::Result<Vec<u8>> {
    if len < 0 || len > max {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad packet length"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Read a VarInt from an async stream.
async fn read_varint<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<i32> {
    let byte = r.read_u8().await?;
    read_varint_cont(byte, r).await
}

/// Continue reading a VarInt whose first byte was already consumed.
async fn read_varint_cont<R: AsyncReadExt + Unpin>(first: u8, r: &mut R) -> io::Result<i32> {
    let mut num: u32 = (first & 0x7F) as u32;
    let mut byte = first;
    let mut shift = 0;
    while byte & 0x80 != 0 {
        shift += 7;
        if shift >= 32 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too big"));
        }
        byte = r.read_u8().await?;
        num |= ((byte & 0x7F) as u32) << shift;
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
    // Hostnames are capped at 255 by the protocol; rejecting out-of-range
    // values here also rules out negative-length arithmetic entirely.
    if !(0..=255).contains(&addr_len) {
        return None;
    }
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

/// Build a Minecraft text component (as JSON) from a MOTD string.
///
/// Accepts three input styles so you can paste whichever your gradient tool
/// produces:
///   1. A raw JSON chat component (starts with `{` or `[`) -> used verbatim.
///   2. `&#RRGGBB` hex codes, e.g. `&#54DAF4C&#57D6E4l...`
///   3. Bungee `&x&R&R&G&G&B&B` hex codes, plus named `&0`-`&f`, formats
///      `&k`-`&o`, and reset `&r`.
/// A color code (named or hex) resets active formatting, matching vanilla.
fn motd_component(motd: &str) -> serde_json::Value {
    let trimmed = motd.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(motd) {
            return value;
        }
    }
    parse_legacy_motd(motd)
}

/// Parse `&`/`§` color, hex, and format codes into a `{text,extra:[...]}`
/// component with one run per style change.
fn parse_legacy_motd(motd: &str) -> serde_json::Value {
    // Let a literal "\n" in env vars mean a line break (env can't hold a real one).
    let motd = motd.replace("\\n", "\n");
    let chars: Vec<char> = motd.chars().collect();
    let mut runs: Vec<serde_json::Value> = Vec::new();
    let mut buf = String::new();
    let mut style = Style::default();

    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];

        // <gradient:#RRGGBB:#RRGGBB[:#RRGGBB...]>text</gradient>
        // Emits one run per character with interpolated hex colors. The text
        // inside is taken literally (no & codes); formatting flags active at
        // the tag (e.g. &l before it) are inherited by every character.
        if c == '<' {
            if let Some((stops, text, consumed)) = parse_gradient_tag(&chars[i..]) {
                flush_run(&mut runs, &mut buf, &style);
                let visible: Vec<char> = text.chars().collect();
                let last = visible.len().saturating_sub(1).max(1) as f32;
                for (idx, ch) in visible.iter().enumerate() {
                    let mut run_style = Style {
                        color: Some(gradient_color(&stops, idx as f32 / last)),
                        ..Style::default()
                    };
                    run_style.bold = style.bold;
                    run_style.italic = style.italic;
                    run_style.underlined = style.underlined;
                    run_style.strikethrough = style.strikethrough;
                    run_style.obfuscated = style.obfuscated;
                    let mut s = String::new();
                    s.push(*ch);
                    let mut one = s;
                    flush_run(&mut runs, &mut one, &run_style);
                }
                i += consumed;
                continue;
            }
        }

        if (c == '&' || c == '\u{00A7}') && i + 1 < chars.len() {
            let next = chars[i + 1];

            // &#RRGGBB
            if next == '#'
                && i + 8 <= chars.len()
                && chars[i + 2..i + 8].iter().all(|c| c.is_ascii_hexdigit())
            {
                flush_run(&mut runs, &mut buf, &style);
                let hex: String = chars[i + 2..i + 8].iter().collect();
                style.set_color(format!("#{}", hex.to_lowercase()));
                i += 8;
                continue;
            }

            // &x&R&R&G&G&B&B (Bungee hex)
            if next == 'x' || next == 'X' {
                if let Some((hex, end)) = read_bungee_hex(&chars, i) {
                    flush_run(&mut runs, &mut buf, &style);
                    style.set_color(format!("#{}", hex.to_lowercase()));
                    i = end;
                    continue;
                }
            }

            // Named color, format, or reset.
            let code = next.to_ascii_lowercase();
            if let Some(name) = color_name(code) {
                flush_run(&mut runs, &mut buf, &style);
                style.set_color(name.to_string());
                i += 2;
                continue;
            }
            if let Some(applied) = style.apply_format(code) {
                if applied {
                    flush_run(&mut runs, &mut buf, &style);
                }
                i += 2;
                continue;
            }
        }
        buf.push(c);
        i += 1;
    }
    flush_run(&mut runs, &mut buf, &style);
    serde_json::json!({ "text": "", "extra": runs })
}

/// Try to parse a `<gradient:#RRGGBB[:#RRGGBB...]>text</gradient>` tag at the
/// start of `chars`. Returns (color stops, inner text, chars consumed), or None
/// if this isn't a well-formed gradient tag (caller then treats `<` literally).
fn parse_gradient_tag(chars: &[char]) -> Option<(Vec<(u8, u8, u8)>, String, usize)> {
    const OPEN: &str = "<gradient:";
    const CLOSE: &str = "</gradient>";
    let open: Vec<char> = OPEN.chars().collect();
    if chars.len() < open.len() || chars[..open.len()] != open[..] {
        return None;
    }

    // Header: colon-separated #RRGGBB stops up to '>'.
    let header_end = chars.iter().position(|&c| c == '>')?;
    let header: String = chars[open.len()..header_end].iter().collect();
    let mut stops = Vec::new();
    for part in header.split(':') {
        let hex = part.strip_prefix('#')?;
        if hex.len() != 6 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        stops.push((
            u8::from_str_radix(&hex[0..2], 16).ok()?,
            u8::from_str_radix(&hex[2..4], 16).ok()?,
            u8::from_str_radix(&hex[4..6], 16).ok()?,
        ));
    }
    if stops.is_empty() {
        return None;
    }

    // Body: everything up to the closing tag.
    let close: Vec<char> = CLOSE.chars().collect();
    let body_start = header_end + 1;
    let rel_close = chars[body_start..]
        .windows(close.len())
        .position(|w| *w == close[..])?;
    let text: String = chars[body_start..body_start + rel_close].iter().collect();
    let consumed = body_start + rel_close + close.len();
    Some((stops, text, consumed))
}

/// Interpolate along multi-stop gradient colors at position t in [0, 1],
/// returning a "#rrggbb" string.
fn gradient_color(stops: &[(u8, u8, u8)], t: f32) -> String {
    let t = t.clamp(0.0, 1.0);
    if stops.len() == 1 {
        let (r, g, b) = stops[0];
        return format!("#{:02x}{:02x}{:02x}", r, g, b);
    }
    let scaled = t * (stops.len() - 1) as f32;
    let idx = (scaled.floor() as usize).min(stops.len() - 2);
    let frac = scaled - idx as f32;
    let (r1, g1, b1) = stops[idx];
    let (r2, g2, b2) = stops[idx + 1];
    let lerp = |a: u8, b: u8| (a as f32 + (b as f32 - a as f32) * frac).round() as u8;
    format!("#{:02x}{:02x}{:02x}", lerp(r1, r2), lerp(g1, g2), lerp(b1, b2))
}

/// Read six `&<hex>` pairs after a `&x`, returning (hex string, index past them).
fn read_bungee_hex(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut hex = String::with_capacity(6);
    let mut j = start + 2;
    for _ in 0..6 {
        if j + 1 < chars.len()
            && (chars[j] == '&' || chars[j] == '\u{00A7}')
            && chars[j + 1].is_ascii_hexdigit()
        {
            hex.push(chars[j + 1]);
            j += 2;
        } else {
            return None;
        }
    }
    Some((hex, j))
}

#[derive(Default)]
struct Style {
    color: Option<String>,
    bold: bool,
    italic: bool,
    underlined: bool,
    strikethrough: bool,
    obfuscated: bool,
}

impl Style {
    /// Set a color, resetting active formatting like vanilla legacy codes do.
    fn set_color(&mut self, color: String) {
        *self = Style { color: Some(color), ..Style::default() };
    }

    /// Apply a format/reset code. Returns Some(true) if it changed style,
    /// Some(false) if it was a no-op reset, None if `code` isn't a format code.
    fn apply_format(&mut self, code: char) -> Option<bool> {
        match code {
            'k' => self.obfuscated = true,
            'l' => self.bold = true,
            'm' => self.strikethrough = true,
            'n' => self.underlined = true,
            'o' => self.italic = true,
            'r' => *self = Style::default(),
            _ => return None,
        }
        Some(true)
    }
}

/// Append the buffered text as a styled run, then clear the buffer.
fn flush_run(runs: &mut Vec<serde_json::Value>, buf: &mut String, style: &Style) {
    if buf.is_empty() {
        return;
    }
    let mut obj = serde_json::Map::new();
    obj.insert("text".into(), serde_json::Value::String(std::mem::take(buf)));
    if let Some(color) = &style.color {
        obj.insert("color".into(), serde_json::Value::String(color.clone()));
    }
    if style.bold {
        obj.insert("bold".into(), true.into());
    }
    if style.italic {
        obj.insert("italic".into(), true.into());
    }
    if style.underlined {
        obj.insert("underlined".into(), true.into());
    }
    if style.strikethrough {
        obj.insert("strikethrough".into(), true.into());
    }
    if style.obfuscated {
        obj.insert("obfuscated".into(), true.into());
    }
    runs.push(serde_json::Value::Object(obj));
}

/// Map a legacy color code (`0`-`f`) to its Minecraft color name.
fn color_name(code: char) -> Option<&'static str> {
    Some(match code {
        '0' => "black",
        '1' => "dark_blue",
        '2' => "dark_green",
        '3' => "dark_aqua",
        '4' => "dark_red",
        '5' => "dark_purple",
        '6' => "gold",
        '7' => "gray",
        '8' => "dark_gray",
        '9' => "blue",
        'a' => "green",
        'b' => "aqua",
        'c' => "red",
        'd' => "light_purple",
        'e' => "yellow",
        'f' => "white",
        _ => return None,
    })
}
