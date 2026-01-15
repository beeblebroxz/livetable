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
    run_server(&host, port).await
}
