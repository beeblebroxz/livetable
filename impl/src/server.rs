/// HTTP server with WebSocket support for real-time table updates
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

use crate::websocket::{AppState, TableWebSocket};

/// WebSocket endpoint handler
async fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let resp = ws::start(TableWebSocket::new(state), &req, stream)?;
    Ok(resp)
}

/// Health check endpoint
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "message": "LiveTable WebSocket server is running"
    }))
}

/// Start the HTTP server with WebSocket support
pub async fn run_server(host: &str, port: u16) -> std::io::Result<()> {
    let state = web::Data::new(AppState::new());

    println!("🚀 LiveTable WebSocket Server");
    println!("====================================");
    println!("📡 WebSocket: ws://{}:{}/ws", host, port);
    println!("🏥 Health check: http://{}:{}/health", host, port);
    println!("====================================");
    println!();

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            // Enable logger
            .wrap(middleware::Logger::default())
            // CORS for development
            .wrap(
                actix_cors::Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header()
                    .max_age(3600),
            )
            // WebSocket endpoint
            .route("/ws", web::get().to(ws_index))
            // Health check
            .route("/health", web::get().to(health_check))
    })
    .bind((host, port))?
    .run()
    .await
}
