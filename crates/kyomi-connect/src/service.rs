use std::path::PathBuf;

const SERVICE_NAME: &str = "kyomi-connect";
const UNIT_PATH: &str = "/etc/systemd/system/kyomi-connect.service";

pub fn install() -> anyhow::Result<()> {
    if !is_root() {
        anyhow::bail!("Service install requires root. Run with: sudo kyomi-connect service install");
    }

    let binary_path = std::env::current_exe()?.canonicalize()?;
    let config_dir = PathBuf::from("/etc/kyomi-connect");
    let env_file = config_dir.join("env");

    // Copy user config to system location if not already present
    if !config_dir.exists() {
        if let Some(user_config_dir) = dirs::config_dir() {
            let user_config = user_config_dir.join("kyomi-connect");
            if user_config.exists() {
                std::fs::create_dir_all(&config_dir)?;
                for entry in std::fs::read_dir(&user_config)? {
                    let entry = entry?;
                    let dest = config_dir.join(entry.file_name());
                    std::fs::copy(entry.path(), &dest)?;
                }
                println!(
                    "  Copied config from {} to {}",
                    user_config.display(),
                    config_dir.display()
                );
            }
        }
    }

    // Generate env file from config.toml if it exists
    let config_toml = config_dir.join("config.toml");
    if config_toml.exists() {
        let config: crate::config_file::ConfigFile =
            toml::from_str(&std::fs::read_to_string(&config_toml)?)?;

        let mut env_content = format!(
            "KYOMI_TOKEN={}\nDB_HOST={}\nDB_PORT={}\nDB_NAME={}\nDB_USER={}\n",
            config.token, config.db_host, config.db_port, config.db_name, config.db_user,
        );
        if let Some(ref password_file) = config.db_password_file {
            env_content.push_str(&format!("DB_PASSWORD_FILE={password_file}\n"));
        }
        if let Some(ref ssl_mode) = config.db_ssl_mode {
            env_content.push_str(&format!("DB_SSL_MODE={ssl_mode}\n"));
        }
        if let Some(ref ssl_ca) = config.db_ssl_ca {
            env_content.push_str(&format!("DB_SSL_CA={ssl_ca}\n"));
        }
        if let Some(health_port) = config.health_port {
            env_content.push_str(&format!("HEALTH_PORT={health_port}\n"));
        }
        env_content.push_str("RUST_LOG=info\n");

        std::fs::write(&env_file, &env_content)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&env_file, std::fs::Permissions::from_mode(0o600))?;
        }
        println!("  Generated env file: {}", env_file.display());
    }

    let unit = format!(
        r#"[Unit]
Description=Kyomi Connect Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile={env_file}
ExecStart={binary} run
Restart=always
RestartSec=5
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
"#,
        env_file = env_file.display(),
        binary = binary_path.display(),
    );

    std::fs::write(UNIT_PATH, &unit)?;
    println!("  Created: {UNIT_PATH}");

    let status = std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status()?;
    if !status.success() {
        anyhow::bail!("systemctl daemon-reload failed");
    }

    println!();
    println!("  Service installed. To start:");
    println!("    sudo systemctl enable --now {SERVICE_NAME}");
    println!();
    println!("  To check status:");
    println!("    sudo systemctl status {SERVICE_NAME}");

    Ok(())
}

pub fn uninstall() -> anyhow::Result<()> {
    if !is_root() {
        anyhow::bail!(
            "Service uninstall requires root. Run with: sudo kyomi-connect service uninstall"
        );
    }

    let _ = std::process::Command::new("systemctl")
        .args(["stop", SERVICE_NAME])
        .status();
    let _ = std::process::Command::new("systemctl")
        .args(["disable", SERVICE_NAME])
        .status();

    if std::path::Path::new(UNIT_PATH).exists() {
        std::fs::remove_file(UNIT_PATH)?;
        println!("  Removed: {UNIT_PATH}");
    }

    let _ = std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status();

    println!("  Service uninstalled.");
    Ok(())
}

fn is_root() -> bool {
    #[cfg(unix)]
    {
        // Safety: geteuid() is a standard POSIX call with no unsafe invariants
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}
