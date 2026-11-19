use tsclient_rs::open_protocol::{
    BotCommand, BotCommandList, ClientKind, OpenProtocolConfig, OpenProtocolEvent,
    OpenProtocolHandle, PeerIdentity,
};
use tsclient_rs::{Client, ClientOptions, generateIdentity};

#[tokio::main]
async fn main() -> Result<(), tsclient_rs::Error> {
    let identity = generateIdentity(8);
    let mut client = Client::new(
        identity,
        "127.0.0.1:9987".to_string(),
        "OpenProtocolBot".to_string(),
        ClientOptions::default(),
    );

    let config = OpenProtocolConfig::new(PeerIdentity::new(
        ClientKind::Bot,
        "ExampleBot",
        env!("CARGO_PKG_VERSION"),
    ))
    .with_commands(
        BotCommandList::new(vec![
            BotCommand::new("replay", "Replay recent voice segments")
                .with_args("[seconds] [@nickname]"),
            BotCommand::new("skip", "Skip current track"),
        ])
        .with_prefix("!"),
    );

    let open_protocol = OpenProtocolHandle::attach(&client, config);
    let mut events = open_protocol.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            if let OpenProtocolEvent::PeerIdentityUpdated {
                peer_uid,
                clid,
                identity,
            } = event
            {
                println!(
                    "Discovered peer {} (clid={}): {} {}",
                    peer_uid, clid, identity.app_name, identity.app_version
                );
            }
        }
    });

    client.connect().await?;
    client.wait_connected(None).await?;
    println!("Connected, CLID: {}", client.client_id());

    client.disconnect().await?;
    Ok(())
}
