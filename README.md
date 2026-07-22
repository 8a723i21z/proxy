# Deploy and Host Serverless Minecraft with Proxy on Railway

Serverless Minecraft with Proxy runs a Paper Minecraft server that sleeps when empty and wakes when a player joins. A lightweight Rust proxy holds the public port, answers server-list pings without waking the server, and boots it on the first real login, so you only pay for compute while people are actually playing.

## About Hosting Serverless Minecraft with Proxy

A Minecraft server normally runs around the clock, billing compute even when nobody is online. Scaling it to zero sounds simple but breaks in practice, because Minecraft clients ping every server on their multiplayer list every few seconds, and a plain TCP proxy treats those pings as real traffic. This template solves that: the proxy answers pings itself and wakes the server only on a genuine login. Hosting also needs a volume for the world, which is included, and Serverless toggled on manually for the Minecraft service, since Railway does not allow that setting to be baked into a template.

## Common Use Cases

- A small private survival server for friends who play a few evenings a week
- Seasonal or event servers that sit idle for long stretches between sessions
- Modpack and plugin testing servers that only need to run while you are working on them
- Community servers with predictable off-hours, such as overnight or weekdays
- Hosting a world for a group in another timezone without paying for the gap

## Dependencies for Serverless Minecraft with Proxy Hosting

- [itzg/docker-minecraft-server](https://github.com/itzg/docker-minecraft-server), running Paper by default
- A custom Rust wake proxy built on Tokio, included in this repository
- Railway Serverless (app sleeping) enabled on the Minecraft service
- A Railway volume attached to the Minecraft service for world storage
- Railway TCP proxy on the proxy service for public networking

### Deployment Dependencies

- [itzg/docker-minecraft-server documentation](https://docker-minecraft-server.readthedocs.io/)
- [Paper](https://papermc.io/)
- [Minecraft EULA](https://aka.ms/MinecraftEULA), which you must accept via the `EULA` variable
- [Railway Serverless documentation](https://docs.railway.com/reference/app-sleeping)

### Implementation Details

**Enable Serverless on the `minecraft` service after deploying.** This is what saves the money, and it has to be done by hand: go to the service's settings > **Deploy** > **Serverless** and toggle **Enable Serverless**. Leave it **off** for the `proxy` service, which must stay running to hold the public port, answer pings, and wake the backend.

Two services are deployed:

| Service | What it is | Sleeps? |
| --- | --- | --- |
| `minecraft` | itzg/docker-minecraft-server, private networking only, with a volume for world storage | Yes, this is the expensive one |
| `proxy` | This repo. Owns the public TCP port | No, but it only uses about 3 MB of RAM |

**Resources.** The proxy idles at around 3 MB of RAM, so set it to the lowest CPU and memory allocation available. Give the `minecraft` service 1 vCPU and as much RAM as you want the server to have. Keep that allocation comfortably above the JVM heap set by the `MEMORY` variable, since the JVM needs a few hundred MB on top of the heap.

**Idle.** The proxy answers server-list pings itself with your offline MOTD, echoing the client's protocol version so it never shows as incompatible. The Minecraft container is never contacted, so it stays asleep.

**First join.** The proxy gives the backend 5 seconds. If it isn't up, it starts the boot in the background and *holds the player on the connecting screen* until the server is ready, then puts them through on the same connection — no rejoin. Minecraft's client drops a login that goes quiet for about 30 seconds, which is less than a cold start, so the proxy sends the client a login plugin request every 12 seconds to keep that timer from firing. None of those packets advance the login, so encryption and Mojang authentication still happen end-to-end between the player and the real server once it answers.

The hold lasts up to `JOIN_HOLD_SECS` (default 90). If the server still isn't up by then, or the client predates 1.13 and has no packet the proxy can send mid-login, the player gets the styled `STARTUP_MESSAGE` and can rejoin. Set `JOIN_HOLD_SECS=0` to always disconnect immediately instead.

The client shows its normal connecting screen during the hold; the vanilla protocol has no way to put custom text there before login completes.

**Playing.** Joins proxy straight through, with encryption, mods, and plugins passing untouched. The proxy sends a real status handshake every 60 seconds so the server can't sleep mid-session, which also refreshes a cached copy of the backend's MOTD, player cap, and icon.

**Back to sleep.** After the last player leaves, the proxy shows the online MOTD for `SLEEP_TIMEOUT_SECS` from that cache, never probing, so the container's idle timer runs down undisturbed.

The proxy is a fork of a simpler one, with a lot of improvements added on top. The original forwards raw TCP without reading it, so it can't tell a server-list ping from a real join. This fork speaks the Minecraft protocol.

| | Original | This fork |
| --- | --- | --- |
| Server-list pings | Wake the backend | Answered locally, backend untouched |
| Ping while asleep | Client hangs, then fails | Instant MOTD with icon and player count |
| Join during cold start | Generic connection timeout | Player is held, then let in without rejoining |
| Keepalive while playing | Bare TCP connect | Real Minecraft status handshake |
| Player count | Counts pings and scanners too | Actual players only |
| Session teardown | Half-open sessions linger | Both legs closed, counts stay accurate |
| Transient accept error | Process exits | Logged, keeps serving |

It also enforces one live session per username so reconnects never trigger "You logged in from another location", supports rich MOTDs (`&` color codes, hex, and `<gradient:#54DAF4:#6DB654>text</gradient>` tags), answers legacy pre-1.7 pings, and drains active sessions for 25 seconds on redeploy so players aren't cut off mid-write.

Proxy variables:

| Variable | Default | What it does |
| --- | --- | --- |
| `BACKEND_ADDR` | *(required)* | Minecraft service's private address, e.g. `minecraft.railway.internal:25565` |
| `PORT` | `25565` | Port the proxy listens on |
| `MOTD` | | Shown while the server is asleep |
| `MOTD_ONLINE` | *(unset)* | Shown while it's up. Leave unset to relay the server's real MOTD |
| `MAX_PLAYERS` | `20` | Slot cap shown (online *and* asleep) until the server's real value is cached |
| `FAVICON` | *(unset)* | 64x64 PNG as a `data:image/png;base64,...` URI |
| `SLEEP_TIMEOUT_SECS` | `600` | How long after the last player the online MOTD keeps showing. Cosmetic only |
| `CONNECT_TIMEOUT_SECS` | `120` | How long the background wake keeps retrying |
| `JOIN_HOLD_SECS` | `90` | How long a cold join waits on the connecting screen. `0` to disconnect instead |
| `MAX_CONNECTIONS` | `256` | Cap on simultaneous client connections |
| `STARTUP_MESSAGE` | | Disconnect message, shown only if the hold runs out |

Minecraft service variables are documented in [minecraft.env.example](minecraft.env.example).

Things to know:

- **The first join of a session is a rejoin.** Waking takes around 20 seconds, longer than the client will wait, so the first player gets a "starting up" message and connects on their second attempt. Everyone after that joins normally.
- **The backend sees the proxy's IP**, not the player's. IP bans, geo plugins, and connection logs all show the proxy. There's no PROXY-protocol support.
- **Your world is on a volume.** It survives sleeping, waking, and redeploys. Don't detach it.
- **`SLEEP_TIMEOUT_SECS` doesn't control sleeping.** It only controls which MOTD is displayed. The container's own idle timer decides when it stops.

## Why Deploy Serverless Minecraft with Proxy on Railway?

Railway is a singular platform to deploy your infrastructure stack. Railway will host your infrastructure so you don't have to deal with configuration, while allowing you to vertically and horizontally scale it.

By deploying Serverless Minecraft with Proxy on Railway, you are one step closer to supporting a complete full-stack application with minimal burden. Host your servers, databases, AI agents, and more on Railway.
