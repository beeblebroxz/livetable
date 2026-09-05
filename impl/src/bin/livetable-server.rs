/// LiveTable WebSocket Server
///
/// Standalone server that provides WebSocket access to LiveTable tables
/// with real-time updates for frontend clients.
use livetable::server::run_server;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Get host and port from environment or use defaults
    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("PORT must be a number");

    // Start the server
    if std::env::args().any(|arg| arg == "--lab") {
        // Reset/load controls are explicitly local-only and opt-in.
        if !["127.0.0.1", "::1", "localhost"].contains(&host.as_str()) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "--lab requires a loopback HOST"));
        }
        let mut engine = livetable::engine::TableEngine::new();
        engine.enable_lab().map_err(std::io::Error::other)?;
        let listener = std::net::TcpListener::bind((host.as_str(), port))?;
        println!("LiveTable Lab • ws://{host}:{port}/ws • 1,000 synthetic orders");
        livetable::server::server_from_listener_with_engine(listener, engine)?.await
    } else {
        run_server(&host, port).await
    }
}
