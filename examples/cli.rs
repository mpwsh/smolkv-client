use clap::{Args, Parser, Subcommand, ValueEnum};
use config::{Config, ConfigError, File};
use dirs::config_dir;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use smolkv_client::{Error, QueryBuilder, SmolKv, SortOrder};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tokio_stream::StreamExt;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EndpointConfig {
    url: String,
    secret: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Settings {
    default_endpoint: Option<String>,
    endpoints: HashMap<String, EndpointConfig>,
}

impl Settings {
    fn load() -> Result<Self, ConfigError> {
        let config_path = get_config_path();

        // Create default config if it doesn't exist
        if !config_path.exists() {
            let default_settings = Settings::default();
            let parent_dir = config_path.parent().unwrap();
            fs::create_dir_all(parent_dir).ok();
            let toml = toml::to_string(&default_settings).unwrap();
            fs::write(&config_path, toml).ok();
        }

        let s = Config::builder()
            .add_source(File::from(config_path))
            .build()?;

        s.try_deserialize()
    }

    fn save(&self) -> Result<(), std::io::Error> {
        let config_path = get_config_path();
        let parent_dir = config_path.parent().unwrap();
        fs::create_dir_all(parent_dir)?;

        let toml =
            toml::to_string(self).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        fs::write(config_path, toml)
    }

    fn get_endpoint(&self) -> Result<(String, EndpointConfig), Error> {
        match &self.default_endpoint {
            Some(name) => match self.endpoints.get(name) {
                Some(config) => Ok((name.clone(), config.clone())),
                None => Err(Error::BadRequest(format!(
                    "Default endpoint '{}' not found in config",
                    name
                ))),
            },
            None => Err(Error::BadRequest(
                "No default endpoint set. Use 'endpoint use <name>' to set a default endpoint."
                    .into(),
            )),
        }
    }
}

fn get_config_path() -> PathBuf {
    let mut path = config_dir().unwrap_or_else(|| PathBuf::from("."));
    path.push("smolkv");
    path.push("config.toml");
    path
}

// Parse path format for key operations: <collection>/<key>
fn parse_key_path(path: &str) -> Result<(String, String), Error> {
    let parts: Vec<&str> = path.split('/').collect();

    match parts.len() {
        1 => Err(Error::BadRequest(
            "Invalid path format. Use <collection>/<key>".into(),
        )),
        2 => Ok((parts[0].to_string(), parts[1].to_string())),
        _ => {
            // If there are more parts, consider the first part as collection and the rest as a key with slashes
            let collection = parts[0].to_string();
            let key = parts[1..].join("/");
            Ok((collection, key))
        }
    }
}

#[derive(Parser)]
#[command(author, version, about = "SmolKV CLI client", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage server endpoints in configuration file
    Endpoint(EndpointArgs),

    /// Collection management commands
    Collection(CollectionCommands),

    /// Put a value into a collection
    Put {
        /// Path in format collection/key
        path: String,

        /// JSON value as string
        value: String,
    },

    /// Get a value from a collection
    Get {
        /// Path in format collection/key
        path: String,
    },

    /// Delete a value from a collection
    Del {
        /// Path in format collection/key
        path: String,
    },

    /// Import an array of values from a json file directly into a collection
    Import {
        /// Collection name
        collection: String,

        /// JSON property to use as the key (supports dot notation like "owner.login")
        #[arg(
            long,
            help = "JSON property to use as the key for each object. Supports dot notation for nested properties (e.g., 'owner.login', 'metadata.id')"
        )]
        key: Option<String>,

        /// Path to JSON file (must contain an array of objects)
        #[arg(
            long,
            help = "Path to the JSON file to import. File must contain an array of objects."
        )]
        file: String,
    },
}

#[derive(Args)]
struct EndpointArgs {
    #[command(subcommand)]
    command: EndpointCommands,
}

#[derive(Subcommand)]
enum EndpointCommands {
    /// Set a new endpoint in configuration file
    #[command(alias = "s")]
    Set {
        /// Endpoint name
        name: String,

        /// Endpoint URL
        url: String,

        /// Secret key (optional)
        #[arg(long)]
        secret: Option<String>,
    },

    /// List endpoints in configuration file
    #[command(alias = "ls")]
    List,

    /// Remove an endpoint from configuration file
    #[command(alias = "rm")]
    Remove {
        /// Endpoint name
        name: String,
    },

    /// Set the default endpoint
    Use {
        /// Endpoint name to use as default
        name: String,
    },
}

#[derive(Args)]
struct CollectionCommands {
    #[command(subcommand)]
    command: CollectionSubcommands,
}

#[derive(Subcommand)]
enum CollectionSubcommands {
    /// Create a new collection
    Create {
        /// Collection name
        name: String,
    },

    /// Drop (delete) a collection
    Drop {
        /// Collection name
        name: String,
    },

    /// List all items in a collection with optional query
    List {
        /// Collection name
        name: String,

        /// JSONPath query expression
        #[arg(long)]
        query: Option<String>,

        /// Maximum number of results
        #[arg(long)]
        limit: Option<usize>,

        /// Sort order
        #[arg(long, value_enum, default_value_t = CliSortOrder::Asc)]
        order: CliSortOrder,

        /// Include keys in results
        #[arg(long, default_value_t = false)]
        keys: bool,

        /// Start key for range queries
        #[arg(long)]
        from: Option<String>,

        /// End key for range queries
        #[arg(long)]
        to: Option<String>,
    },

    /// Watch for changes in a collection
    Watch {
        /// Collection name
        name: String,
    },

    /// Backup commands
    Backup(BackupCommands),

    /// Restore commands
    Restore(RestoreCommands),
}

#[derive(Args)]
struct BackupCommands {
    #[command(subcommand)]
    command: BackupSubcommands,
}

#[derive(Subcommand)]
enum BackupSubcommands {
    /// Create a new backup
    Create {
        /// Collection name
        name: String,
    },

    /// Get backup status
    Status {
        /// Collection name
        name: String,

        /// Backup ID
        #[arg(long)]
        id: String,
    },

    /// List all backups for a collection
    List {
        /// Collection name
        name: String,

        /// JSONPath query expression
        #[arg(long)]
        query: Option<String>,

        /// Maximum number of results
        #[arg(long)]
        limit: Option<usize>,

        /// Sort order
        #[arg(long, value_enum, default_value_t = CliSortOrder::Asc)]
        order: CliSortOrder,

        /// Include keys in results
        #[arg(long, default_value_t = false)]
        keys: bool,

        /// Start key for range queries
        #[arg(long)]
        from: Option<String>,

        /// End key for range queries
        #[arg(long)]
        to: Option<String>,
    },

    /// Upload a backup file
    Upload {
        /// Collection name
        name: String,

        /// Path to backup file
        #[arg(long)]
        file: String,
    },

    /// Download a backup file
    Download {
        /// Collection name
        name: String,

        /// Backup ID
        #[arg(long)]
        id: String,

        /// Output file path (default: <collection>-<backup_id>.sst)
        #[arg(long)]
        output: Option<String>,
    },
}

#[derive(Args)]
struct RestoreCommands {
    #[command(subcommand)]
    command: RestoreSubcommands,
}

#[derive(Subcommand)]
enum RestoreSubcommands {
    /// Restore a collection from a backup
    Create {
        /// Collection name
        name: String,

        /// Backup ID
        #[arg(long)]
        id: String,
    },

    /// Get restore status
    Status {
        /// Collection name
        name: String,

        /// Restore ID
        #[arg(long)]
        id: String,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum CliSortOrder {
    Asc,
    Desc,
}

impl From<CliSortOrder> for SortOrder {
    fn from(order: CliSortOrder) -> Self {
        match order {
            CliSortOrder::Asc => SortOrder::Asc,
            CliSortOrder::Desc => SortOrder::Desc,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    let mut settings =
        Settings::load().map_err(|e| Error::BadRequest(format!("Failed to load config: {}", e)))?;

    // Handle endpoint commands separately as they don't need a KV client
    if let Commands::Endpoint(args) = &cli.command {
        match &args.command {
            EndpointCommands::Use { name } => {
                if !settings.endpoints.contains_key(name) {
                    return Err(Error::BadRequest(format!(
                        "Endpoint '{}' not found in config",
                        name
                    )));
                }
                settings.default_endpoint = Some(name.clone());
                settings
                    .save()
                    .map_err(|e| Error::BadRequest(format!("Failed to save config: {}", e)))?;
                println!("Using endpoint '{}'", name);
                return Ok(());
            }
            EndpointCommands::Set { name, url, secret } => {
                settings.endpoints.insert(
                    name.clone(),
                    EndpointConfig {
                        url: url.clone(),
                        secret: secret.clone(),
                    },
                );
                settings
                    .save()
                    .map_err(|e| Error::BadRequest(format!("Failed to save config: {}", e)))?;
                println!("Endpoint '{}' set successfully", name);
                return Ok(());
            }
            EndpointCommands::List => {
                println!("SmolKV Configuration:");

                if let Some(default) = &settings.default_endpoint {
                    println!("Default endpoint: {}", default);
                } else {
                    println!("Default endpoint: <not set>");
                }

                println!("\nConfigured endpoints:");
                if settings.endpoints.is_empty() {
                    println!("  <none>");
                } else {
                    for (name, config) in &settings.endpoints {
                        println!("  {}: {}", name, config.url);
                    }
                }
                return Ok(());
            }
            EndpointCommands::Remove { name } => {
                if settings.endpoints.remove(name).is_some() {
                    // If removing the default endpoint, unset it
                    if let Some(default) = &settings.default_endpoint {
                        if default == name {
                            settings.default_endpoint = None;
                        }
                    }

                    settings
                        .save()
                        .map_err(|e| Error::BadRequest(format!("Failed to save config: {}", e)))?;
                    println!("Endpoint '{}' removed successfully", name);
                } else {
                    println!("Endpoint '{}' not found in config", name);
                }
                return Ok(());
            }
        }
    }

    // Get the default endpoint for all other commands
    let (_, endpoint_config) = settings.get_endpoint()?;

    // Create the KV client once
    let kv = match &endpoint_config.secret {
        Some(secret) => SmolKv::new(endpoint_config.url, Some(secret.clone())),
        None => SmolKv::new(endpoint_config.url, None::<String>),
    };

    // Process the command with a single KV client
    let res = match &cli.command {
        Commands::Endpoint(_) => unreachable!(), // Already handled

        Commands::Collection(cmd) => {
            match &cmd.command {
                CollectionSubcommands::Create { name } => kv.create_collection(name).await?,
                CollectionSubcommands::Drop { name } => kv.drop_collection(name).await?,
                CollectionSubcommands::List {
                    name,
                    query,
                    limit,
                    order,
                    keys,
                    from,
                    to,
                } => {
                    let mut builder = QueryBuilder::new()
                        .keys(*keys)
                        .from(from.clone())
                        .to(to.clone())
                        .limit(*limit)
                        .order(order.clone());

                    // Only add the query if it's present and non-empty
                    if let Some(q) = query {
                        if !q.is_empty() {
                            builder = builder.query(q);
                        }
                    }

                    Value::Array(kv.query_collection(name, builder).await?)
                }
                CollectionSubcommands::Watch { name } => {
                    let resp = kv.subscribe(name).await?;
                    let mut stream = resp.bytes_stream();

                    while let Some(chunk) = stream.next().await {
                        match chunk {
                            Ok(bytes) => {
                                let text = String::from_utf8_lossy(&bytes);
                                if let Some(data) = text.strip_prefix("data: ") {
                                    if let Ok(event) = serde_json::from_str::<Value>(data) {
                                        println!("{}", serde_json::to_string_pretty(&event)?);
                                    }
                                }
                            }
                            Err(e) => println!("Stream error: {}", e),
                        }
                    }
                    json!({"message": "streaming connection closed"})
                }
                CollectionSubcommands::Backup(backup_cmd) => {
                    match &backup_cmd.command {
                        BackupSubcommands::Create { name } => kv.start_backup(name).await?,
                        BackupSubcommands::Status { name, id } => {
                            kv.backup_status(name, id).await?
                        }
                        BackupSubcommands::List {
                            name,
                            query,
                            limit,
                            order,
                            keys,
                            from,
                            to,
                        } => {
                            let mut builder = QueryBuilder::new()
                                .keys(*keys)
                                .from(from.clone())
                                .to(to.clone())
                                .limit(*limit)
                                .order(order.clone());

                            // Only add the query if it's present and non-empty
                            if let Some(q) = query {
                                if !q.is_empty() {
                                    builder = builder.query(q);
                                }
                            }

                            Value::Array(kv.query_collection(name, builder).await?)
                        }
                        BackupSubcommands::Upload { name, file } => {
                            let file_bytes = tokio::fs::read(file).await.map_err(|e| {
                                Error::BadRequest(format!("Failed to read file: {}", e))
                            })?;

                            kv.upload_backup(name, file_bytes).await?
                        }
                        BackupSubcommands::Download { name, id, output } => {
                            let output_path = output
                                .clone()
                                .unwrap_or_else(|| format!("{}-{}.sst", name, id));

                            println!("Downloading backup to {}...", output_path);
                            let bytes = kv.download_backup(name, id).await?;

                            tokio::fs::write(&output_path, bytes).await.map_err(|e| {
                                Error::BadRequest(format!("Failed to write file: {}", e))
                            })?;

                            json!({"message": format!("Backup downloaded successfully to {}", output_path)})
                        }
                    }
                }
                CollectionSubcommands::Restore(restore_cmd) => match &restore_cmd.command {
                    RestoreSubcommands::Create { name, id } => kv.start_restore(name, id).await?,
                    RestoreSubcommands::Status { name, id } => kv.restore_status(name, id).await?,
                },
            }
        }

        Commands::Put { path, value } => {
            let (collection, key) = parse_key_path(path)?;

            let parsed_value: Value = serde_json::from_str(value)
                .map_err(|e| Error::BadRequest(format!("Invalid JSON value: {}", e)))?;

            kv.put(&collection, &key, &parsed_value).await?
        }

        Commands::Get { path } => {
            let (collection, key) = parse_key_path(path)?;
            kv.get(&collection, &key).await?
        }

        Commands::Del { path } => {
            let (collection, key) = parse_key_path(path)?;
            let deleted = kv.delete(&collection, &key).await?;
            json!({"path": path, "deleted": deleted})
        }
        Commands::Import {
            collection,
            key,
            file,
        } => {
            let file_bytes = tokio::fs::read(&file)
                .await
                .map_err(|e| Error::BadRequest(format!("Failed to read file: {}", e)))?;

            kv.import_values(collection, key.clone(), file_bytes)
                .await?
        }
    };

    // Print the result
    println!("{}", serde_json::to_string_pretty(&res)?);

    Ok(())
}
