//! nebulad --config FILE

use nebula::server::{App, Config};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args: Vec<String> = std::env::args().collect();
    let path = match args.iter().position(|a| a == "--config") {
        Some(i) if i + 1 < args.len() => args[i + 1].clone(),
        _ => {
            eprintln!("usage: nebulad --config FILE");
            std::process::exit(2);
        }
    };
    let cfg: Config = serde_json::from_slice(&std::fs::read(&path).expect("read config")).expect("parse config");
    let listen = cfg.listen.clone();
    let app = App::open(cfg).expect("open state");
    let listener = tokio::net::TcpListener::bind(&listen).await.expect("bind");
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    app.serve(listener).await.expect("serve");
}
