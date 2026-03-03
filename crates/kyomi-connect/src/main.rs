mod callback_server;
mod config;
pub(crate) mod config_file;
mod executor;
mod health;
mod service;
pub mod wizard;
mod ws_client;

use std::io::IsTerminal;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kyomi-connect", about = "Kyomi Connect — secure database proxy agent")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the Connect agent (skip auto-setup, fail if not configured)
    Run,
    /// Reconfigure the setup wizard (token, database credentials)
    Setup {
        /// Token from Kyomi dashboard (skips interactive prompt)
        #[arg(long)]
        token: Option<String>,
        /// Token file path
        #[arg(long)]
        token_file: Option<String>,
        /// Database host (skips interactive prompt)
        #[arg(long)]
        db_host: Option<String>,
        /// Database port
        #[arg(long)]
        db_port: Option<u16>,
        /// Database name
        #[arg(long)]
        db_name: Option<String>,
        /// Database user
        #[arg(long)]
        db_user: Option<String>,
        /// Database password file
        #[arg(long)]
        db_password_file: Option<String>,
        /// SSL mode (disable, prefer, require, verify-ca, verify-full)
        #[arg(long)]
        db_ssl_mode: Option<String>,
    },
    /// Show connection status and datasource info
    Status,
    /// Manage systemd service
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
}

#[derive(Subcommand)]
enum ServiceAction {
    /// Install systemd service unit
    Install,
    /// Uninstall systemd service unit
    Uninstall,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        // `kyomi-connect setup [flags]` — always run wizard, then start agent
        Some(Commands::Setup {
            token,
            token_file,
            db_host,
            db_port,
            db_name,
            db_user,
            db_password_file,
            db_ssl_mode,
        }) => {
            if let Err(e) = wizard::run_setup(
                token,
                token_file,
                db_host,
                db_port,
                db_name,
                db_user,
                db_password_file,
                db_ssl_mode,
            )
            .await
            {
                eprintln!("  Setup failed: {e}");
                std::process::exit(1);
            }
            run_agent().await;
        }

        // `kyomi-connect run` — run agent directly, fail if not configured
        Some(Commands::Run) => {
            run_agent().await;
        }

        // `kyomi-connect status`
        Some(Commands::Status) => {
            let cf = match config_file::ConfigFile::load() {
                Some(cf) => cf,
                None => {
                    eprintln!();
                    eprintln!("  No configuration found.");
                    eprintln!("  Run 'kyomi-connect' to set up.");
                    eprintln!();
                    std::process::exit(1);
                }
            };

            eprintln!();
            eprintln!("  Kyomi Connect Status");
            eprintln!("  ────────────────────");
            eprintln!();

            // Token check
            eprint!("  Token        ");
            let peek = match wizard::peek_token_safe(&cf.token) {
                Some(p) => {
                    eprintln!("\x1b[32m\u{2713}\x1b[0m");
                    Some(p)
                }
                None => {
                    eprintln!("\x1b[31m\u{2717}\x1b[0m  invalid JWT format");
                    None
                }
            };

            // Database check
            eprint!("  Database     ");
            let db_label = peek.as_ref()
                .map(|p| db_type_label(&p.db))
                .unwrap_or("unknown");
            eprintln!("{db_label} \x1b[2m({}:{}/{})\x1b[0m", cf.db_host, cf.db_port, cf.db_name);

            // Kyomi API check
            if let Some(peek) = peek {
                eprint!("  Kyomi        ");
                match wizard::fetch_connect_info_safe(&peek.iss, &cf.token).await {
                    Some(info) => {
                        eprintln!(
                            "\x1b[32m\u{2713}\x1b[0m  {} \x1b[2m({})\x1b[0m",
                            info.datasource_name, info.workspace_name
                        );
                    }
                    None => {
                        eprintln!("\x1b[31m\u{2717}\x1b[0m  unreachable");
                    }
                }
            }

            eprintln!();
        }

        // `kyomi-connect service install|uninstall`
        Some(Commands::Service { action }) => match action {
            ServiceAction::Install => {
                if let Err(e) = service::install() {
                    eprintln!("  {e}");
                    std::process::exit(1);
                }
            }
            ServiceAction::Uninstall => {
                if let Err(e) = service::uninstall() {
                    eprintln!("  {e}");
                    std::process::exit(1);
                }
            }
        },

        // No subcommand: auto-detect — has config → run, no config → setup then run
        None => {
            let has_config = config_file::ConfigFile::load().is_some()
                || std::env::var("KYOMI_TOKEN").is_ok();

            if has_config {
                run_agent().await;
            } else if std::io::stdin().is_terminal() {
                // No config, interactive terminal → run setup first
                if let Err(e) = wizard::run_setup(
                    None, None, None, None, None, None, None, None,
                )
                .await
                {
                    eprintln!("  Setup failed: {e}");
                    std::process::exit(1);
                }
                run_agent().await;
            } else {
                // No config, non-interactive → can't setup, tell them how
                eprintln!();
                eprintln!("  No configuration found.");
                eprintln!("  Run 'kyomi-connect' interactively to set up,");
                eprintln!("  or set KYOMI_TOKEN and DB_* environment variables.");
                eprintln!();
                std::process::exit(1);
            }
        }
    }
}

async fn run_agent() {
    eprintln!();
    eprintln!("  Kyomi Connect");
    eprintln!("  ─────────────");
    eprintln!();

    // 1. Verify token
    eprint!("  Token Valid          ");
    let config = match config::ConnectConfig::from_env().await {
        Ok(c) => {
            eprintln!("\x1b[32m\u{2713}\x1b[0m");
            c
        }
        Err(e) => {
            eprintln!("\x1b[31m\u{2717}\x1b[0m  {e}");
            eprintln!();
            eprintln!("  Run 'kyomi-connect setup' to reconfigure.");
            std::process::exit(1);
        }
    };

    // 2. Test database connection
    eprint!("  Database Connection  ");
    let executor = match executor::CommandExecutor::from_config(&config).await {
        Ok(e) => {
            eprintln!(
                "\x1b[32m\u{2713}\x1b[0m  {} \x1b[2m({}:{})\x1b[0m",
                db_type_label(&config.db_type), config.db_host, config.db_port,
            );
            Arc::new(e)
        }
        Err(e) => {
            let msg = e.to_string();
            let display_msg = msg
                .rsplit_once(": ")
                .map(|(_, root)| root)
                .unwrap_or(&msg);
            eprintln!("\x1b[31m\u{2717}\x1b[0m  {display_msg}");
            eprintln!();
            eprintln!("  Run 'kyomi-connect setup' to reconfigure.");
            std::process::exit(1);
        }
    };

    // 3. Connect to Kyomi backend
    eprint!("  Kyomi Connection     ");
    let peek = wizard::peek_token_safe(&config.token);
    let info = match &peek {
        Some(p) => wizard::fetch_connect_info_safe(&p.iss, &config.token).await,
        None => None,
    };

    let ws_client = ws_client::WsClient::new(config.ws_url.clone(), config.token.clone());
    match ws_client.connect_once().await {
        Ok(()) => {
            let detail = match &info {
                Some(i) => format!("{} \x1b[2m({})\x1b[0m", i.datasource_name, i.workspace_name),
                None => "connected".to_string(),
            };
            eprintln!("\x1b[32m\u{2713}\x1b[0m  {detail}");
        }
        Err(e) => {
            let msg = e.to_string();
            let display_msg = msg
                .rsplit_once(": ")
                .map(|(_, root)| root)
                .unwrap_or(&msg);
            eprintln!("\x1b[31m\u{2717}\x1b[0m  {display_msg}");
            eprintln!();
            eprintln!("  Will keep retrying in the background...");
        }
    }

    eprintln!();
    eprintln!("  Ready — listening for queries.");
    eprintln!();

    // 4. Start health check server (after startup output is done)
    let ws_connected = Arc::new(AtomicBool::new(false));
    let db_healthy = Arc::new(AtomicBool::new(true));
    tokio::spawn(health::start_health_server(
        config.health_port,
        ws_connected.clone(),
        db_healthy.clone(),
    ));

    // 5. Run forever (reconnects automatically on disconnection)
    ws_client
        .run_forever(ws_connected, move |request| {
            let executor = executor.clone();
            async move { executor.execute(request).await }
        })
        .await;
}

/// Human-readable label for a datasource type slug.
fn db_type_label(db_type: &str) -> &str {
    match db_type {
        "postgres" => "PostgreSQL",
        "mysql" => "MySQL",
        "clickhouse" => "ClickHouse",
        "sqlserver" => "SQL Server",
        "redshift" => "Redshift",
        "snowflake" => "Snowflake",
        "databricks" => "Databricks",
        "synapse" => "Azure Synapse",
        other => other,
    }
}
